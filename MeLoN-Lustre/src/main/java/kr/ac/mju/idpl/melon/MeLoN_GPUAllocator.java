package kr.ac.mju.idpl.melon;

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

import kr.ac.mju.idpl.melon.MeLoN_Constants.GPUAllocType;

public class MeLoN_GPUAllocator {
	private static final Logger LOG = LoggerFactory.getLogger(MeLoN_GPUAllocator.class);
	private String[] nodes;
	private Map<String, GPUDeviceInfo> nodeGPUInfoMap = new HashMap<>();
	private GPUAllocType gpuAllocType;
	private List<GPURequest> gpuDeviceAllocInfo = new Vector<>();
	
	private boolean allAllocated;
	
	MeLoN_GPUAllocator(String[] nodes, GPUAllocType gpuAllocType){
		this.nodes = nodes;
		this.gpuAllocType = gpuAllocType;
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
	
	public void gpuDeviceAllocating() {
		try {
			updateGPUDeviceInfo();
		} catch (NumberFormatException | XPathExpressionException | SAXException
				| ParserConfigurationException | IOException | InterruptedException e) {
			e.printStackTrace();
		}
		logGPUInfo();
		allAllocated = true;
		LOG.info("=================================");
		LOG.info("***GPU allocation mode is {}", gpuAllocType);
		LOG.info("=================================");
		for (GPURequest gpuReq : gpuDeviceAllocInfo) {
			if(gpuReq.isNotReady()) {
				GPUDeviceInfo allocDevice = null;
				for (String deviceId : nodeGPUInfoMap.keySet()) {
					allocDevice = nodeGPUInfoMap.get(deviceId);
					break;
				}
//				String allocDevice = null;
//				int allocDeviceMemoryTotal = 0;
//				int allocDeviceMemoryFree = 0;
//				int allocDeviceGPUUtil = 100;
//				int allocDeviceCPC = 100;
				if (gpuAllocType == GPUAllocType.OVERPROVISION) {
					for (String deviceId : nodeGPUInfoMap.keySet()) {
						if(nodeGPUInfoMap.get(deviceId).getFree() > gpuReq.getRequiredGPUMemory() * 1.1) {
							if(nodeGPUInfoMap.get(deviceId).getGpuUtil() < (allocDevice.getGpuUtil() + 5)
									&& nodeGPUInfoMap.get(deviceId).getGpuUtil() > (allocDevice.getGpuUtil() - 5)){
								if(nodeGPUInfoMap.get(deviceId).getComputeProcessCount() != 0
										&& nodeGPUInfoMap.get(deviceId).getGPUUtilPerCPC() > allocDevice.getGPUUtilPerCPC()) {
									allocDevice = nodeGPUInfoMap.get(deviceId);
								}
							}else if(nodeGPUInfoMap.get(deviceId).getGpuUtil() <= (allocDevice.getGpuUtil() - 5)) {
								allocDevice = nodeGPUInfoMap.get(deviceId);
							}
						}
					}
//				} else if (gpuAllocMode == GPUAllocMode.WORST) {
//					for (String deviceId : nodeGPUInfoMap.keySet()) {
//						if (nodeGPUInfoMap.get(deviceId).getFree() > allocDevice.getFree()
//								&& nodeGPUInfoMap.get(deviceId).getFree() > gpuReq.getRequiredGPUMemory() * 1.1) {
//							allocDevice = nodeGPUInfoMap.get(deviceId);
//						}
//					}
//				} else if (gpuAllocMode == GPUAllocMode.BEST) {
//					for (String deviceId : nodeGPUInfoMap.keySet()) {
////						if (allocDevice.getFree() == 0) {
////							allocDevice.getFree() = Integer.MAX_VALUE;
////						}
//						if (nodeGPUInfoMap.get(deviceId).getFree() < allocDevice.getFree()
//								&& nodeGPUInfoMap.get(deviceId).getFree() > gpuReq.getRequiredGPUMemory() * 1.1) {
//							allocDevice = nodeGPUInfoMap.get(deviceId);
//						}
//					}
//				} else if (gpuAllocMode == GPUAllocMode.WHOLE) {
//					allocDevice = null;
//					for (String deviceId : nodeGPUInfoMap.keySet()) {
//						if (nodeGPUInfoMap.get(deviceId).getComputeProcessCount() == 0
//								&& nodeGPUInfoMap.get(deviceId).getFree() > gpuReq.getRequiredGPUMemory() * 1.1) {
//							LOG.info("***Device {} is 0 cpc and has enough memory.", deviceId);
//							allocDevice = nodeGPUInfoMap.get(deviceId);
//						}
//					}
				}
				LOG.info("Task({}) using {}MB Gpu memory.", gpuReq.getRequestTask(), gpuReq.getRequiredGPUMemory());
				if(allocDevice != null) {
					LOG.info("allocDevice = {}, allocDeviceFree = {}/{}", allocDevice.getDeviceId(), allocDevice.getFree(),
							allocDevice.getTotal());
				} else {
					LOG.info("Device is not allocated.");
				}
				if ((int) (gpuReq.getRequiredGPUMemory() * 1.1) <= 0) {
					gpuReq.setStatusReady();
				} else if (allocDevice != null && (int) (gpuReq.getRequiredGPUMemory() * 1.1) < allocDevice.getFree()) {
					gpuReq.deviceAlloc(allocDevice);
					allocDevice.allocateMemory((int) (gpuReq.getRequiredGPUMemory() * 1.1),
							gpuReq.getRequestTask());
					allocDevice.increaseComputeProcessCount();
					LOG.info("***Device {} is allocated to {}.", allocDevice.getDeviceId(), gpuReq.getRequestTask());
					LOG.info("***Device {} now CPC = {}.", allocDevice.getDeviceId(), allocDevice.getComputeProcessCount());
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
	
	public boolean isAllAllocated() {
		return allAllocated;
	}
	
	public synchronized Map<String, String> getGPUDeviceEnv(Container container, MeLoN_Task task) {
		LOG.info("Container {} getGPUDeviceEnv. task is {}", container.getId(), task.getJobName());
		Map<String, String> env = new ConcurrentHashMap<>();
		for (GPURequest gpuReq : gpuDeviceAllocInfo) {
			if (gpuReq.getRequestTask().equals(task.getJobName()) && gpuReq.getDevice() != null
					&& gpuReq.getDevice().getDeviceHost().equals(container.getNodeId().getHost())
					&& gpuReq.isRequested()) {
				gpuReq.setStatusAllocated();
				gpuReq.setContainerId(container.getId());
				env.put(MeLoN_Constants.CUDA_VISIBLE_DEVICES, String.valueOf(gpuReq.getDevice().getDeviceNum()));
				env.put(MeLoN_Constants.FRACTION, gpuReq.getFraction());
				LOG.info("\n***Extra envs set." + "\n***Task = " + task.getJobName() + ":" + task.getTaskIndex()
						+ "\n***Device = " + gpuReq.getDevice().getDeviceId() + ", Using "
						+ gpuReq.getRequiredGPUMemory() + "/" + gpuReq.getDevice().getTotal() + "MB, Fraction = "
						+ gpuReq.getFraction() + "\n***ContainerId = " + container.getId());
				break;
			}else if(gpuReq.getRequestTask().equals(task.getJobName()) && gpuReq.getDevice() == null) {
				gpuReq.setContainerId(container.getId());
			}
		}
		return env;
	}
	
	public void onTaskCompleted(ContainerId containerId) {
		for (GPURequest gpuReq : gpuDeviceAllocInfo) {
			if (gpuReq.isThisContainer(containerId)) {
				LOG.info("{} is finished.", containerId);
				gpuReq.finished();
			}

		}
	}
}
