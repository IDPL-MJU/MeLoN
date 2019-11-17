package kr.ac.mju.idpl.melon;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.math3.analysis.function.Constant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import kr.ac.mju.idpl.melon.rpc.RPCServer;
import kr.ac.mju.idpl.melon.util.Utils;

public class MeLoN_ApplicationMaster {
	private static final Logger LOG = LoggerFactory.getLogger(MeLoN_ApplicationMaster.class);

	private String appExecutionType = null;
	private String gpuAllocMode = "WORST";

	private Configuration yarnConf;
	private Configuration hdfsConf;

	private FileSystem resourceFs;

	private AMRMClientAsync<ContainerRequest> amRMClient;
	private NMClientAsync nmClientAsync;
	private NMCallbackHandler containerListener;

	private List<Container> runningContainers = new ArrayList<>();
	// private ApplicationAttemptId appAttemptID;

	private String amHostname = "";
	private int amPort = 0;
	private String amTrackingUrl = "";

	private int numTotalContainers;
	private int containerMemory;
	private int requestPriority;

	private Map<String, List<ContainerRequest>> askedContainerMap = new HashMap<>();
	private Map<Integer, List<Container>> appSessionContainersMap = new ConcurrentHashMap<>();

	private ContainerId containerId;
	private String appIdString;
	private Configuration melonConf = new Configuration(false);
	private Map<String, LocalResource> localResources = new ConcurrentHashMap<>();
	private String hdfsClasspath;
	private String adminUser;
	private String adminPassword;

	private AtomicInteger numCompletedContainers = new AtomicInteger();
	private AtomicInteger numAllocatedContainers = new AtomicInteger();
	private AtomicInteger numFailedContainers = new AtomicInteger();
	private AtomicInteger numRequestedContainers = new AtomicInteger();

	private Map<String, String> shellEnvs = new HashMap<>();
	private Map<String, String> containerEnvs = new HashMap<>();
	// private String melonHome;
	private String appJar;
	private String domainController;

	private RPCServer rpcServer;

	private volatile boolean done;
	private volatile boolean success;

	private List<Thread> launchTreads = new ArrayList<Thread>();
	private Options opts;

	private String[] nodes = new String[] { "master.hadoop.com", "slave1.hadoop.com", "slave2.hadoop.com" };
	private Map<String, GPUDeviceInfo> nodeGPUInfoMap = new HashMap<>();
	private List<GPURequest> gpuDeviceAllocInfo = new Vector<>();

	public MeLoN_ApplicationMaster() throws Exception {
		yarnConf = new Configuration(false);
		hdfsConf = new Configuration(false);
		opts = new Options();
		initOptions();
	}

	private void initOptions() {
		opts.addOption("hdfs_classpath", true, "Path to jars on HDFS for workers.");
		opts.addOption("python_bin_path", true, "The relative path to python binary.");
		opts.addOption("python_venv", true, "The python virtual environment zip.");
	}

	private boolean init(String[] args) {
		LOG.info("Starting init...");
		Utils.initYarnConf(yarnConf);
		Utils.initHdfsConf(hdfsConf);
		try {
			resourceFs = FileSystem.get(hdfsConf);
		} catch (IOException e) {
			LOG.error("Failed to create FileSystem object", e);
			return false;
		}
		CommandLine cliParser;
		try {
			cliParser = new GnuParser().parse(opts, args);
		} catch (ParseException e) {
			LOG.error("Got exception while parsing options", e);
			return false;
		}
		melonConf.addResource(new Path(MeLoN_Constants.MELON_FINAL_XML));
		Map<String, String> envs = System.getenv();
		String[] shellEnvsStr = melonConf.getStrings(MeLoN_ConfigurationKeys.SHELL_ENVS);
		shellEnvs = Utils.parseKeyValue(shellEnvsStr);
		String[] containersEnvsStr = melonConf.getStrings(MeLoN_ConfigurationKeys.CONTAINER_ENVS);
		containerEnvs = Utils.parseKeyValue(containersEnvsStr);
		containerId = ContainerId.fromString(envs.get(ApplicationConstants.Environment.CONTAINER_ID.name()));
		appIdString = containerId.getApplicationAttemptId().getApplicationId().toString();
		hdfsClasspath = cliParser.getOptionValue("hdfs_classpath");
		appExecutionType = melonConf.get("melon.application.execution-type");
		gpuAllocMode = melonConf.get("melon.application.gpu-alloc");

		return true;
	}

	private void updateGPUDeviceInfo() throws IOException, InterruptedException, SAXException,
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

				String deviceId = host + ":" + deviceNum;
				if (!nodeGPUInfoMap.containsKey(deviceId)) {
					nodeGPUInfoMap.put(deviceId, new GPUDeviceInfo(host, deviceNum, totalMemoryUsage, usedMemoryUsage));
				} else {
					nodeGPUInfoMap.get(deviceId).updateMemoryUsage(usedMemoryUsage);
				}
			}
		}
	}

	private void logMonitoredInfo() {
		LOG.info("=================================");
		for (String deviceId : nodeGPUInfoMap.keySet()) {
			LOG.info("***DeviceID={}, total={}(MB), used={}(MB), free={}(MB)",
					nodeGPUInfoMap.get(deviceId).getDeviceId(), nodeGPUInfoMap.get(deviceId).getTotal(),
					nodeGPUInfoMap.get(deviceId).getUsed(), nodeGPUInfoMap.get(deviceId).getFree());
		}
		LOG.info("=================================");
	}

	private int parseMibStrToMbInt(String memoryUsageStr) {
		memoryUsageStr = memoryUsageStr.toLowerCase();
		int mib = memoryUsageStr.indexOf("mib");
		if (-1 != mib) {
			return Integer.parseInt(memoryUsageStr.substring(0, mib).trim()) * 104858 / 100000;
		}
		return 0;
	}

	private void printUsage() {
		// TODO Auto-generated method stub

	}

	private boolean run(String[] args) throws IOException, YarnException, InterruptedException {
		long started = System.currentTimeMillis();
		LOG.info("This application's execution type is " + appExecutionType + ".");
		if (!init(args)) {
			return false;
		}

		LOG.info("Starting amRMClient...");
		AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
		amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
		amRMClient.init(yarnConf);
		amRMClient.start();

		amHostname = System.getenv(ApplicationConstants.Environment.NM_HOST.name());
		// amHostname = NetUtils.getHostname();
		rpcServer = new RPCServer.Builder().setHostname(amHostname).setYarnConf(yarnConf)
				.setTaskExecutorJVMArgs(melonConf.get(MeLoN_ConfigurationKeys.TASK_EXECUTOR_JVM_OPTS,
						MeLoN_ConfigurationKeys.TASK_EXECUTOR_JVM_OPTS_DEFAULT))
				.setMelonConf(melonConf).build();
		amPort = rpcServer.getRpcPort();
		containerEnvs.put(MeLoN_Constants.AM_HOST, amHostname);
		containerEnvs.put(MeLoN_Constants.AM_PORT, Integer.toString(amPort));

		String amIPPort = NetUtils.getLocalInetAddress(amHostname).getHostAddress() + ":" + amPort;
		RegisterApplicationMasterResponse response = amRMClient.registerApplicationMaster(amHostname, amPort,
				amTrackingUrl);
		LOG.info("MeLoN_ApplicationMaster is registered with response : {}", response.toString());

		NMCallbackHandler containerListener = new NMCallbackHandler();
		nmClientAsync = new NMClientAsyncImpl(containerListener);
		nmClientAsync.init(yarnConf);
		nmClientAsync.start();
		LOG.info("Starting NMCallbackHandler...");

		LOG.info("Starting application RPC server at: " + amHostname + ":" + amPort);
		rpcServer.start();
		rpcServer.setResources(yarnConf, hdfsConf, localResources, containerEnvs, hdfsClasspath);
		List<MeLoN_ContainerRequest> requests = rpcServer.getContainerRequests();
		LOG.info("Requests : " + requests.toString());
		for (MeLoN_ContainerRequest request : requests) {
			LOG.info("***gpuDeviceAllocInfo put jobName = {}, GPUMemory = {}", request.getJobName(),
					String.valueOf(request.getGpuMemory()));

			gpuDeviceAllocInfo.add(new GPURequest(request.getJobName(), request.getGpuMemory()));
		}

		LOG.info("***requests size = {}", requests.size());
		LOG.info("***gpuDeviceAllocInfo size = {}", gpuDeviceAllocInfo.size());

		boolean allReq = false;
		boolean allAlloc = false;
		while (!done) {
			while (!allReq) {
				while (!allAlloc) {
					LOG.info("***==========Updating GPU Resource Info...==========");
					try {
						updateGPUDeviceInfo();
					} catch (NumberFormatException | XPathExpressionException | SAXException
							| ParserConfigurationException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					logMonitoredInfo();
					allAlloc = gpuDeviceAllocating();

					if (appExecutionType.equals("amtest")) {
						LOG.info("This application is AMTest mode. Application will be finished.");
						return true;
					}
					if (!allAlloc && appExecutionType.equals("distributed")) {
						resetGpuDeviceAllocInfo();
					} else if (appExecutionType.equals("batch")) {
						break;
					}
				}
				LOG.info("***==========Adding Container Requests...==========");
				allReq = true;
				for (MeLoN_ContainerRequest request : requests) {
					LOG.info("Requesting container ...");
					ContainerRequest containerAsk = setupContainerAskForRM(request);
					if (containerAsk != null) {
						if (!askedContainerMap.containsKey(request.getJobName())) {
							askedContainerMap.put(request.getJobName(), new ArrayList<>());
						}
						LOG.info("***Task type is " + request.getJobName());
						askedContainerMap.get(request.getJobName()).add(containerAsk);
						LOG.info("***addContainerRequest");
						amRMClient.addContainerRequest(containerAsk);
						LOG.info("***done");
					} else {
						allReq = false;
					}
				}
			}
			int numTotalTrackedTasks = rpcServer.getTotalTrackedTasks();
			if ((numTotalTrackedTasks > 0 ? (float) rpcServer.getNumCompletedTrackedTasks() / numTotalTrackedTasks
					: 0) == 1.0f) {
				stopRunningContainers();
				LOG.info("Training has finished. - All tasks");
				break;
			}
			if (rpcServer.isTrainingFinished()) {
				LOG.info("Training has finished. - rpcServer finished");
				break;
			}

			// Pause before refresh job status
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				LOG.error("Thread interrupted", e);
			}
		}
		nmClientAsync.stop();
		amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "Application complete!", null);
		amRMClient.stop();
		rpcServer.updateTrainingFinalStatus();
		FinalApplicationStatus status = rpcServer.getTrainingFinalStatus();
		if (status != FinalApplicationStatus.SUCCEEDED) {
			LOG.info("Training finished with failure!");
		} else {
			LOG.info("Training finished successfully!");
		}
		return status == FinalApplicationStatus.SUCCEEDED;
	}

	private boolean gpuDeviceAllocating() {
		boolean allAlloc = true;
		for (GPURequest gpuReq : gpuDeviceAllocInfo) {
			String allocDevice = null;
			int allocDeviceMemoryTotal = 0;
			int allocDeviceMemoryFree = 0;
			if (gpuAllocMode.equals("WORST")) {
				for (String deviceId : nodeGPUInfoMap.keySet()) {
					if (nodeGPUInfoMap.get(deviceId).getFree() > allocDeviceMemoryFree
							&& nodeGPUInfoMap.get(deviceId).getFree() > gpuReq.getRequiredGPUMemory() * 1.1) {
						allocDevice = nodeGPUInfoMap.get(deviceId).getDeviceId();
						allocDeviceMemoryTotal = nodeGPUInfoMap.get(deviceId).getTotal();
						allocDeviceMemoryFree = nodeGPUInfoMap.get(deviceId).getFree();
					}
				}
			} else if (gpuAllocMode.equals("BEST")) {
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
			} else if (gpuAllocMode.equals("WHOLE")) {
				for (String deviceId : nodeGPUInfoMap.keySet()) {
					if (nodeGPUInfoMap.get(deviceId).getUsed() < 500
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
				// entry.put("DEVICE", "none");
				// entry.put("DEVICE_TOTAL_GPU_MEMORY", "none");
				// entry.put("STATUS", "ready");
				gpuReq.setStatusReady();
			} else if ((int) (gpuReq.getRequiredGPUMemory() * 1.1) < allocDeviceMemoryFree) {
				// entry.put("DEVICE", allocDevice);
				// entry.put("DEVICE_TOTAL_GPU_MEMORY", String.valueOf(allocDeviceMemoryTotal));
				// entry.put("STATUS", "ready");
				gpuReq.deviceAlloc(nodeGPUInfoMap.get(allocDevice));
				nodeGPUInfoMap.get(allocDevice).allocateMemory((int) (gpuReq.getRequiredGPUMemory() * 1.1),
						gpuReq.getRequestTask());
			} else {
				// entry.put("DEVICE", null);
				// entry.put("DEVICE_TOTAL_GPU_MEMORY", null);
				// entry.put("STATUS", "non-ready");
				gpuReq.setStatusNotReady();
				allAlloc = false;
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
		return allAlloc;
	}

	private void resetGpuDeviceAllocInfo() {
		LOG.info("Reset gpuDeviceAllocInfo...");
		for (GPURequest gpuReq : gpuDeviceAllocInfo) {
			gpuReq.resetRequest();
		}
	}

	private void stopRunningContainers() throws InterruptedException {
		// List<Container> allContainers = sessionContainersMap.get(session.sessionId);
		if (runningContainers != null) {
			for (Container container : runningContainers) {
				MeLoN_Task task = rpcServer.getTask(container.getId());
				if (!task.isCompleted()) {
					nmClientAsync.stopContainerAsync(container.getId(), container.getNodeId());
				}
			}
		}

		// Give 15 seconds for containers to exit
		Thread.sleep(15000);
		boolean result = rpcServer.getNumCompletedTasks() == rpcServer.getTotalTasks();
		if (!result) {
			LOG.warn("Not all containers were stopped or completed. Only " + rpcServer.getNumCompletedTasks()
					+ " out of " + rpcServer.getTotalTasks() + " finished.");
		}
	}

	private ContainerRequest setupContainerAskForRM(MeLoN_ContainerRequest request) {
		Priority priority = Priority.newInstance(request.getPriority());
		Resource capability = Resource.newInstance((int) request.getMemory(), request.getvCores());
		Utils.setCapabilityGPU(capability, request.getGpus());
		boolean requested = false;
		String[] node = null;
		for (GPURequest gpuReq : gpuDeviceAllocInfo) {
			if(gpuReq.getRequestTask().equals(request.getJobName()) && gpuReq.isReady()) {
				if(gpuReq.getDevice() != null) {
					node = new String[] {gpuReq.getDevice().getDeviceHost()};
					LOG.info("Launching Task({}) at {}.",gpuReq.getRequestTask(), gpuReq.getDevice().getDeviceId());
				}else {
					node = null;
					LOG.info("Launching Task({}) at somewhere:none...", gpuReq.getRequestTask());
				}
				gpuReq.setStatusRequested();
				requested = true;
				break;
			}
		}
		ContainerRequest containerAsk;
		if (requested) {
			containerAsk = new ContainerRequest(capability, node, null, priority, false);
			LOG.info("Requested container ask: " + containerAsk.toString());
		} else {
			containerAsk = null;
		}
		return containerAsk;
	}

	public static void main(String[] args) throws Exception {
		MeLoN_ApplicationMaster appMaster = new MeLoN_ApplicationMaster();
		boolean succeeded = appMaster.run(args);
		if (succeeded) {
			LOG.info("Application finished successfully.");
			System.exit(0);
		} else {
			LOG.error("Failed to finish MeLoN_ApplicationMaster successfully.");
			System.exit(-1);
		}
	}

	public synchronized Map<String, String> getGPUDeviceEnv(Container container, MeLoN_Task task) {
		Map<String, String> env = new ConcurrentHashMap<>();
		for (GPURequest gpuReq : gpuDeviceAllocInfo) {
			if(gpuReq.getRequestTask().equals(task.getJobName()) && gpuReq.getDevice() != null 
					&& gpuReq.getDevice().getDeviceHost().equals(container.getNodeId().getHost())
					&& gpuReq.isRequested()) {
				gpuReq.setStatusAllocated();
				env.put("CUDA_VISIBLE_DEVICES", String.valueOf(gpuReq.getDevice().getDeviceNum()));
				env.put("FRACTION", gpuReq.getFraction());
				LOG.info("\n***Extra envs set." + "\n***Task = " + task.getJobName() + ":" + task.getTaskIndex()
						+ "\n***Device = " + gpuReq.getDevice().getDeviceId() + ", Using " + gpuReq.getRequiredGPUMemory() + "/"
						+ gpuReq.getDevice().getTotal() + "MB, Fraction = " + gpuReq.getFraction() + "\n***ContainerId = "
						+ container.getId());
				break;
			}
		}
		return env;
	}

	private class NMCallbackHandler implements NMClientAsync.CallbackHandler {

		@Override
		public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
			LOG.info("Successfully started container " + containerId);
		}

		@Override
		public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
			LOG.info("Container Status: id = {}, status = {}", containerId, containerStatus);
		}

		@Override
		public void onContainerStopped(ContainerId containerId) {
			LOG.info("Container {} finished with exitStatus {}.", containerId, ContainerExitStatus.KILLED_BY_APPMASTER);
			processFinishedContainer(containerId, ContainerExitStatus.KILLED_BY_APPMASTER);
		}

		@Override
		public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
			LOG.error("Failed to query the status of container {}", containerId);
		}

		@Override
		public void onStartContainerError(ContainerId containerId, Throwable t) {
			LOG.info("onStartContainerError {}", containerId);
			LOG.error("Failed to start container {}", containerId);
			// need processing something
		}

		@Override
		public void onStopContainerError(ContainerId containerId, Throwable t) {
			LOG.info("onStopContainerError {}", containerId);
			LOG.error("Failed to stop container {}", containerId);
		}

	}

	private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {

		@Override
		public float getProgress() {
			int numTotalTrackedTasks = rpcServer.getTotalTrackedTasks();
			return numTotalTrackedTasks > 0 ? (float) rpcServer.getNumCompletedTrackedTasks() / numTotalTrackedTasks
					: 0;
		}

		@Override
		public void onContainersAllocated(List<Container> containers) {
			LOG.info("Allocated: " + containers.size() + " containers.");
			for (Container container : containers) {
				LOG.info("Launching a task in container" + ", containerId = " + container.getId() + ", containerNode = "
						+ container.getNodeId().getHost() + ":" + container.getNodeId().getPort()
						+ ", resourceRequest = " + container.getResource() + ", priority = " + container.getPriority());
				Thread thread = new Thread(new ContainerLauncher(container));
				thread.start();
				runningContainers.add(container);
			}
		}

		@Override
		public void onContainersCompleted(List<ContainerStatus> completedContainers) {
			LOG.info("Completed containers: " + completedContainers.size());
			for (ContainerStatus containerStatus : completedContainers) {
				int exitStatus = containerStatus.getExitStatus();
				LOG.info("ContainerID = " + containerStatus.getContainerId() + ", state = " + containerStatus.getState()
						+ ", exitStatus = " + exitStatus);
				String diagnotics = containerStatus.getDiagnostics();
				if (ContainerExitStatus.SUCCESS != exitStatus) {
					LOG.error("Container Exit Status is not SUCCESS. Diagnotics: {}", diagnotics);
				} else {
					LOG.info("Container Exit Status is SUCCESS. Diagnotics: {}", diagnotics);
				}
				processFinishedContainer(containerStatus.getContainerId(), exitStatus);
			}
			int numTotalTrackedTasks = rpcServer.getTotalTrackedTasks();
			LOG.info("numTotalTrackedTasks: {}", numTotalTrackedTasks);
			LOG.info("rpcServer.getNumCompletedTrackedTasks(): {}", rpcServer.getNumCompletedTrackedTasks());
			float prgrs = numTotalTrackedTasks > 0
					? (float) rpcServer.getNumCompletedTrackedTasks() / numTotalTrackedTasks
					: 0;
			LOG.info("getProgress: {}", getProgress());
		}

		@Override
		public void onError(Throwable throwable) {
			LOG.error("Received error in AM to RM call", throwable);
			done = true;
			amRMClient.stop();
			nmClientAsync.stop();
			// need more detail
		}

		@Override
		public void onNodesUpdated(List<NodeReport> arg0) {
			LOG.info("onNodesUpdated called in RMCAllbackHandler");
		}

		@Override
		public void onShutdownRequest() {
			LOG.info("onShutdownRequest called in RMCallbackHandler");
			done = true;
		}

	}

	private class ContainerLauncher implements Runnable {
		Container container;
		// NMCallbackHandler containerListener;

		public ContainerLauncher(Container container) {
			this.container = container;
		}

		public void run() {
			MeLoN_Task task = rpcServer.getAndInitMatchingTaskByPriority(container.getPriority().getPriority());
			if (task == null) {
				LOG.error("Task was null. Nothing to schedule.");
			}
			task.setContainer(container);
			task.setStatus(TaskStatus.READY);

			Map<String, String> containerLaunchEnvs = new ConcurrentHashMap<>(containerEnvs);
			String jobName = task.getJobName();
			String taskIndex = task.getTaskIndex();
			containerLaunchEnvs.put(MeLoN_Constants.JOB_NAME, jobName);
			containerLaunchEnvs.put(MeLoN_Constants.TASK_INDEX, taskIndex);
			containerLaunchEnvs.put(MeLoN_Constants.TASK_NUM, String.valueOf(rpcServer.getTotalTrackedTasks()));
			// containerLaunchEnvs.put(MeLoN_Constants.SESSION_ID,
			// String.valueOf(appSession.sessionId));

			containerLaunchEnvs.put("APP_EXECUTION_TYPE", appExecutionType);

			containerLaunchEnvs.putAll(getGPUDeviceEnv(container, task));

			// Add job type specific resources
			Map<String, LocalResource> containerResources = new ConcurrentHashMap<>(localResources);
			String[] resources = melonConf.getStrings(MeLoN_ConfigurationKeys.getResourcesKey(task.getJobName()));
			Utils.addResources(resources, containerResources, resourceFs);

			// All resources available to all containers
			resources = melonConf.getStrings(MeLoN_ConfigurationKeys.CONTAINER_RESOURCES);
			Utils.addResources(resources, containerResources, resourceFs);

			task.addContainer(container);
			rpcServer.addContainer(container.getId(), task);
			LOG.info("Setting Container [" + container.getId() + "] for task [" + task.getId() + "]..");

			List<String> vargs = new ArrayList<>();
			vargs.add(rpcServer.getTaskCommand());
			vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/te.stdout");
			vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/te.stderr");
			String command = String.join(" ", vargs);
			List<String> commands = new ArrayList<String>();
			commands.add(command);
			LOG.info("Constructed command " + commands);
			LOG.info("Container environment: " + containerLaunchEnvs);

			ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
			ctx.setLocalResources(containerResources);
			ctx.setEnvironment(containerLaunchEnvs);
			ctx.setCommands(commands);

			// appSessionContainersMap.computeIfAbsent(appSession.sessionId, key ->
			// Collections.synchronizedList(new ArrayList<>())).add(container);

			nmClientAsync.startContainerAsync(container, ctx);
			task.setStatus(TaskStatus.RUNNING);
			LOG.info("Container {} launched!", container.getId());
		}

	}

	private void processFinishedContainer(ContainerId containerId, int exitStatus) {
		MeLoN_Task task = rpcServer.getTask(containerId);
		if (task != null) {
			// // Ignore tasks from past sessions.
			// if (task.getSessionId() != session.sessionId) {
			// return;
			// }
			LOG.info("Container {} for task {}:{} finished with exitStatus: {}.", containerId, task.getJobName(),
					task.getTaskIndex(), exitStatus);
			rpcServer.onTaskCompleted(task.getJobName(), task.getTaskIndex(), exitStatus);

		} else {
			LOG.warn("No task found for container : [" + containerId + "]!");
		}

	}
}
