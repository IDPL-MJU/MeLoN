package kr.ac.mju.idpl.melon;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import kr.ac.mju.idpl.melon.MeLoN_Constants.GPUAllocMode;

public class MeLoN_GPUAllocator {
	private static final Logger LOG = LoggerFactory.getLogger(MeLoN_GPUAllocator.class);
	private String[] nodes;
	private Map<String, GPUDeviceInfo> nodeGPUInfoMap = new HashMap<>();
	private GPUAllocMode gpuAllocMode;
	private List<GPURequest> gpuDeviceAllocInfo = new Vector<>();
	
	private boolean allAllocated;
	
	MeLoN_GPUAllocator(String[] nodes, GPUAllocMode gpuAllocMode){
		this.nodes = nodes;
		this.gpuAllocMode = gpuAllocMode;
	}
	private int parseMibStrToMbInt(String memoryUsageStr) {
		memoryUsageStr = memoryUsageStr.toLowerCase();
		int mib = memoryUsageStr.indexOf("mib");
		if (-1 != mib) {
			return Integer.parseInt(memoryUsageStr.substring(0, mib).trim()) * 104858 / 100000;
		}
		return 0;
	}
	
	public void setGPURequests(List<MeLoN_ContainerRequest> requests) {
		for (MeLoN_ContainerRequest request : requests) {
			LOG.info("***gpuDeviceAllocInfo put jobName = {}, GPUMemory = {}", request.getJobName(),
					String.valueOf(request.getGpuMemory()));
//			if(request.getGpuMemory() > 0) {
				gpuDeviceAllocInfo.add(new GPURequest(request.getJobName(), request.getGpuMemory()));
//			}
		}
	}
	
	public void updateGPUDeviceInfo() throws IOException, InterruptedException, SAXException,
			ParserConfigurationException, NumberFormatException, XPathExpressionException {
		for (String host : nodes) {
			// LOG.info("=================================");
			// LOG.info("Host = {}", host);
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
				if (!nodeGPUInfoMap.containsKey(deviceId)) {
					nodeGPUInfoMap.put(deviceId,
							new GPUDeviceInfo(host, deviceNum, totalMemoryUsage, usedMemoryUsage, cptPsCnt, gpuUtil));
				} else {
					nodeGPUInfoMap.get(deviceId).updateGPUInfo(usedMemoryUsage, cptPsCnt, gpuUtil);
				}
			}
		}
	}
	public void logGPUInfo() {
		LOG.info("=================================");
		for (String deviceId : nodeGPUInfoMap.keySet()) {
			LOG.info("***DeviceID={}, util={}(%), cpc={}(ea), total={}(MB), used={}(MB), free={}(MB)",
					nodeGPUInfoMap.get(deviceId).getDeviceId(), nodeGPUInfoMap.get(deviceId).getGpuUtil(),
					nodeGPUInfoMap.get(deviceId).getComputeProcessCount(), nodeGPUInfoMap.get(deviceId).getTotal(),
					nodeGPUInfoMap.get(deviceId).getUsed(), nodeGPUInfoMap.get(deviceId).getFree());
		}
		LOG.info("=================================");
	}
	
	public boolean gpuDeviceAllocating() {
		allAllocated = true;
		LOG.info("=================================");
		LOG.info("***GPU allocation mode is {}", gpuAllocMode);
		LOG.info("=================================");
		for (GPURequest gpuReq : gpuDeviceAllocInfo) {
			if(gpuReq.isNotReady()) {
				String allocDevice = null;
				int allocDeviceMemoryTotal = 0;
				int allocDeviceMemoryFree = 0;
				int allocDeviceGPUUtil = 100;
				int allocDeviceCPC = 100;
				if (gpuAllocMode == GPUAllocMode.MELON) {
					for (String deviceId : nodeGPUInfoMap.keySet()) {
						if(nodeGPUInfoMap.get(deviceId).getGpuUtil() < (allocDeviceGPUUtil + 5)
								&& nodeGPUInfoMap.get(deviceId).getFree() > gpuReq.getRequiredGPUMemory() * 1.1) {
							if(nodeGPUInfoMap.get(deviceId).getGpuUtil() < (allocDeviceGPUUtil - 5)
									|| nodeGPUInfoMap.get(deviceId).getComputeProcessCount() < allocDeviceCPC) {
								allocDevice = nodeGPUInfoMap.get(deviceId).getDeviceId();
								allocDeviceMemoryTotal = nodeGPUInfoMap.get(deviceId).getTotal(); 
								allocDeviceMemoryFree = nodeGPUInfoMap.get(deviceId).getFree();
								allocDeviceCPC = nodeGPUInfoMap.get(deviceId).getComputeProcessCount();
								allocDeviceGPUUtil = nodeGPUInfoMap.get(deviceId).getGpuUtil();
							}
						}
					}
				}else if (gpuAllocMode == GPUAllocMode.ADVANCED_WORST) {
					for (String deviceId : nodeGPUInfoMap.keySet()) {
						if (nodeGPUInfoMap.get(deviceId).getFree() > allocDeviceMemoryFree
								&& nodeGPUInfoMap.get(deviceId).getFree() > gpuReq.getRequiredGPUMemory() * 1.1) {
							if(nodeGPUInfoMap.get(deviceId).getGpuUtil() < (allocDeviceGPUUtil + 5)
									&& nodeGPUInfoMap.get(deviceId).getComputeProcessCount() < allocDeviceCPC) {
								allocDevice = nodeGPUInfoMap.get(deviceId).getDeviceId();
								allocDeviceMemoryTotal = nodeGPUInfoMap.get(deviceId).getTotal();
								allocDeviceMemoryFree = nodeGPUInfoMap.get(deviceId).getFree();
								allocDeviceCPC = nodeGPUInfoMap.get(deviceId).getComputeProcessCount();
								allocDeviceGPUUtil = nodeGPUInfoMap.get(deviceId).getGpuUtil();
							}
						}
					}
					
				} else if (gpuAllocMode == GPUAllocMode.WORST) {
					for (String deviceId : nodeGPUInfoMap.keySet()) {
						if (nodeGPUInfoMap.get(deviceId).getFree() > allocDeviceMemoryFree
								&& nodeGPUInfoMap.get(deviceId).getFree() > gpuReq.getRequiredGPUMemory() * 1.1) {
							allocDevice = nodeGPUInfoMap.get(deviceId).getDeviceId();
							allocDeviceMemoryTotal = nodeGPUInfoMap.get(deviceId).getTotal();
							allocDeviceMemoryFree = nodeGPUInfoMap.get(deviceId).getFree();
						}
					}
				} else if (gpuAllocMode == GPUAllocMode.BEST) {
					for (String deviceId : nodeGPUInfoMap.keySet()) {
						if (allocDeviceMemoryFree == 0) {
							allocDeviceMemoryFree = Integer.MAX_VALUE;
						}
						if (nodeGPUInfoMap.get(deviceId).getFree() < allocDeviceMemoryFree
								&& nodeGPUInfoMap.get(deviceId).getFree() > gpuReq.getRequiredGPUMemory() * 1.1) {
							allocDevice = nodeGPUInfoMap.get(deviceId).getDeviceId();
							allocDeviceMemoryTotal = nodeGPUInfoMap.get(deviceId).getTotal();
							allocDeviceMemoryFree = nodeGPUInfoMap.get(deviceId).getFree();
						}
					}
				} else if (gpuAllocMode == GPUAllocMode.WHOLE) {
					for (String deviceId : nodeGPUInfoMap.keySet()) {
						if (nodeGPUInfoMap.get(deviceId).getComputeProcessCount() == 0
								&& nodeGPUInfoMap.get(deviceId).getFree() > gpuReq.getRequiredGPUMemory() * 1.1) {
							allocDevice = nodeGPUInfoMap.get(deviceId).getDeviceId();
							allocDeviceMemoryTotal = nodeGPUInfoMap.get(deviceId).getTotal();
							allocDeviceMemoryFree = nodeGPUInfoMap.get(deviceId).getFree();
						}
					}
				}
				LOG.info("Task({}) using {}MB Gpu memory.", gpuReq.getRequestTask(), gpuReq.getRequiredGPUMemory());
				LOG.info("allocDevice = {}, allocDeviceFree = {}/{}", allocDevice, allocDeviceMemoryFree,
						allocDeviceMemoryTotal);
				if ((int) (gpuReq.getRequiredGPUMemory() * 1.1) <= 0) {
					gpuReq.setStatusReady();
				} else if ((int) (gpuReq.getRequiredGPUMemory() * 1.1) < allocDeviceMemoryFree) {
					gpuReq.deviceAlloc(nodeGPUInfoMap.get(allocDevice));
					nodeGPUInfoMap.get(allocDevice).allocateMemory((int) (gpuReq.getRequiredGPUMemory() * 1.1),
							gpuReq.getRequestTask());
					nodeGPUInfoMap.get(allocDevice).plusComputeProcessCount();
				} else {
					gpuReq.setStatusNotReady();
					allAllocated = false;
				}
			}
			
		}

		// just for test
		for (GPURequest gpuReq : gpuDeviceAllocInfo) {
			if (gpuReq.getDevice() != null) {
				LOG.info("***GPURequestStatus = {}, GPUMemory = {}, Task = {}, host:Device = {}",
						gpuReq.getRequestStatus(), gpuReq.getRequiredGPUMemory(), gpuReq.getRequestTask(),
						gpuReq.getDevice().getDeviceId());
			} else {
				LOG.info("***GPURequestStatus = {}, GPUMemory = {}, Task = {}", gpuReq.getRequestStatus(),
						gpuReq.getRequiredGPUMemory(), gpuReq.getRequestTask());
			}
		}
		return allAllocated;
	}
	
	public void resetGpuDeviceAllocInfo() {
		for (GPURequest gpuReq : gpuDeviceAllocInfo) {
			if(gpuReq.isReady() && gpuReq.getRequiredGPUMemory() > 0) {
				gpuReq.resetRequest();
			}
		}
	}
	
	public List<GPURequest> getGPUDeviceallocInfo(){
		return this.gpuDeviceAllocInfo;
	}
}
