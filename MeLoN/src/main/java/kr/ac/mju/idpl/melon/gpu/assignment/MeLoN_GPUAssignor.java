package kr.ac.mju.idpl.melon.gpu.assignment;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import kr.ac.mju.idpl.melon.MeLoN_Constants;
import kr.ac.mju.idpl.melon.MeLoN_ContainerRequest;
import kr.ac.mju.idpl.melon.MeLoN_Task;
import kr.ac.mju.idpl.melon.MeLoN_Constants.AppExecutionType;
import kr.ac.mju.idpl.melon.gpu.assignment.*;
import kr.ac.mju.idpl.melon.gpu.assignment.strategy.GPUAssignmentStrategy;

public class MeLoN_GPUAssignor {
	private static final Logger LOG = LoggerFactory.getLogger(MeLoN_GPUAssignor.class);
	private Map<String, MeLoN_GPUDeviceInfo> gpuDevicesInfo;
	private List<MeLoN_GPURequest> gpuRequests;
	private String[] nodes;
	private GPUAssignmentStrategy strategy;
	private AppExecutionType appExecutionType;
	
	private boolean allAssigned;
	
	public MeLoN_GPUAssignor(String[] nodes, GPUAssignmentStrategy strategy, List<MeLoN_ContainerRequest> requests, AppExecutionType appExecutionType){
		this.gpuDevicesInfo = new HashMap<>();
		this.gpuRequests = new Vector<>();
		this.nodes = nodes;
		this.strategy = strategy;
		this.appExecutionType = appExecutionType;
		strategy.initGpuRequests(gpuRequests, requests);
	}
	
	private int parseMibStrToMbInt(String memoryUsageStr) {
		memoryUsageStr = memoryUsageStr.toLowerCase();
		int mib = memoryUsageStr.indexOf("mib");
		if (-1 != mib) {
			return Integer.parseInt(memoryUsageStr.substring(0, mib).trim()) * 104858 / 100000;
		}
		return 0;
	}
	
	public void updateGPUDeviceInfo() throws IOException, InterruptedException, SAXException,
			ParserConfigurationException, NumberFormatException, XPathExpressionException {
		for (String host : nodes) {
			ProcessBuilder monitoringProcessBuilder = new ProcessBuilder("sh", "-c",
					"sshpass -p hadoop ssh -T -oStrictHostKeyChecking=no hadoop@" + host + " nvidia-smi -q -x");
			Process monitoringProcess = monitoringProcessBuilder.start();
			monitoringProcess.waitFor();
			BufferedReader br = new BufferedReader(new InputStreamReader(monitoringProcess.getInputStream()));

			String result = "";
			String line;
			for (int i = 0; (line = br.readLine()) != null; i++) {
				// skip xml document spec
				if (i > 1) {
					result = result + line.trim();
				}
			}
			InputSource is = new InputSource(new StringReader(result));
			Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(is);
			XPath xPath = XPathFactory.newInstance().newXPath();
			String expression = "/nvidia_smi_log/attached_gpus";
			int gpuNum = Integer.parseInt(xPath.compile(expression).evaluate(doc));
			for (int i = 1; i <= gpuNum; i++) {
				expression = "/nvidia_smi_log/gpu[" + i + "]/minor_number";
				int deviceNum = Integer.parseInt(xPath.compile(expression).evaluate(doc));

				expression = "/nvidia_smi_log/gpu[" + i + "]/fb_memory_usage/total";
				String totalMemoryUsageStr = xPath.compile(expression).evaluate(doc);
				int totalMemoryUsage = parseMibStrToMbInt(totalMemoryUsageStr);

				expression = "/nvidia_smi_log/gpu[" + i + "]/fb_memory_usage/used";
				String usedMemoryUsageStr = xPath.compile(expression).evaluate(doc);
				int usedMemoryUsage = parseMibStrToMbInt(usedMemoryUsageStr);

				expression = "/nvidia_smi_log/gpu[" + i + "]/fb_memory_usage/free";
				String freeMemoryUsageStr = xPath.compile(expression).evaluate(doc);
				int freeMemoryUsage = parseMibStrToMbInt(freeMemoryUsageStr);

				expression = "/nvidia_smi_log/gpu[" + i + "]/processes/process_info";
				NodeList nl = (NodeList) xPath.compile(expression).evaluate(doc, XPathConstants.NODESET);
				int cptPsCnt = 0;
				for (int pn = 1; pn <= nl.getLength(); pn++) {
					expression = "/nvidia_smi_log/gpu[" + i + "]/processes/process_info[" + pn + "]/type";
					if (xPath.compile(expression).evaluate(doc).contains("C")) {
						cptPsCnt++;
					}
				}
				expression = "/nvidia_smi_log/gpu[" + i + "]/utilization/gpu_util";
				int gpuUtil = Integer.parseInt(xPath.compile(expression).evaluate(doc).replaceAll("%", "").trim());

				String deviceId = host + ":" + deviceNum;
				if (!gpuDevicesInfo.containsKey(deviceId)) {
					gpuDevicesInfo.put(deviceId,
							new MeLoN_GPUDeviceInfo(host, deviceNum, totalMemoryUsage, usedMemoryUsage, cptPsCnt, gpuUtil));
				} else {
					gpuDevicesInfo.get(deviceId).updateGPUInfo(usedMemoryUsage, cptPsCnt, gpuUtil);
				}
			}
		}
	}
	public void printGPUsInfo() {
		LOG.info("=================================");
		for (String deviceId : gpuDevicesInfo.keySet()) {
			LOG.info("DeviceID={}, util={}(%), cpc={}(ea), total={}(MB), used={}(MB), free={}(MB)",
					gpuDevicesInfo.get(deviceId).getDeviceId(), gpuDevicesInfo.get(deviceId).getGpuUtil(),
					gpuDevicesInfo.get(deviceId).getComputeProcessCount(), gpuDevicesInfo.get(deviceId).getTotal(),
					gpuDevicesInfo.get(deviceId).getUsed(), gpuDevicesInfo.get(deviceId).getFree());
		}
		LOG.info("=================================");
	}
	
	public void gpuDeviceAssignment() {
		boolean done = false;
		while (!done) {
			LOG.info("GPU assignment type is {}", strategy.getStrategyName());
			if (strategy.isNecessaryGPUAssignment()) {
				try {
					updateGPUDeviceInfo();
				} catch (NumberFormatException | XPathExpressionException | SAXException
						| ParserConfigurationException | IOException | InterruptedException e) {
					e.printStackTrace();
				}
				printGPUsInfo();
			}
			allAssigned = strategy.gpuAssignment(gpuDevicesInfo, gpuRequests);
			
			if(allAssigned) {
				done = true;
			} else if (!allAssigned && appExecutionType == AppExecutionType.DISTRIBUTED) {
				resetGpuRequests();
			} else if (appExecutionType == AppExecutionType.BATCH) {
				done = true;
			}
		}
		
	}
	
	public void resetGpuRequests() {
		for (MeLoN_GPURequest gpuReq : gpuRequests) {
			if(gpuReq.isAssigned() && gpuReq.getRequiredGPUMemory() > 0) {
				gpuReq.resetRequest();
			}
		}
	}
	
	public List<MeLoN_GPURequest> getGPURequests(){
		return this.gpuRequests;
	}
	
	public boolean isAllAssigned() {
		return allAssigned;
	}
	
	public synchronized Map<String, String> getGPUDeviceEnv(Container container, MeLoN_Task task) {
		LOG.info("Container {} getGPUDeviceEnv. task is {}", container.getId(), task.getJobName());
		Map<String, String> env = new ConcurrentHashMap<>();
		for (MeLoN_GPURequest gpuReq : gpuRequests) {
			if (gpuReq.getJobName().equals(task.getJobName()) && gpuReq.getDevice() != null
					&& gpuReq.getDevice().getDeviceHost().equals(container.getNodeId().getHost())
					&& gpuReq.isRequested()) {
				gpuReq.setStatusAllocated();
				gpuReq.setContainerId(container.getId());
				env.put(MeLoN_Constants.CUDA_VISIBLE_DEVICES, String.valueOf(gpuReq.getDevice().getDeviceNum()));
				env.put(MeLoN_Constants.FRACTION, gpuReq.getFraction());
				LOG.info("Extra envs set. Task = " + task.getJobName() + ":" + task.getTaskIndex()
						+ " Device = " + gpuReq.getDevice().getDeviceId() + ", Using "
						+ gpuReq.getRequiredGPUMemory() + "/" + gpuReq.getDevice().getTotal() + "MB, Fraction = "
						+ gpuReq.getFraction() + " ContainerId = " + container.getId());
				break;
			}else if(gpuReq.getJobName().equals(task.getJobName()) && gpuReq.getDevice() == null) {
				gpuReq.setContainerId(container.getId());
			}
		}
		return env;
	}
	
	public void onTaskCompleted(ContainerId containerId) {
		for (MeLoN_GPURequest gpuReq : gpuRequests) {
			if (gpuReq.isThisContainer(containerId)) {
				LOG.info("{} is finished.", containerId);
				gpuReq.finished();
			}

		}
	}
}
