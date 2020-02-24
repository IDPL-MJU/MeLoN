package kr.ac.mju.idpl.melon;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
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
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kr.ac.mju.idpl.melon.MeLoN_Constants.AppExecutionType;
import kr.ac.mju.idpl.melon.MeLoN_Constants.GPUAssignmentType;
import kr.ac.mju.idpl.melon.MeLoN_Task.TaskStatus;
import kr.ac.mju.idpl.melon.gpu.assignment.*;
import kr.ac.mju.idpl.melon.gpu.assignment.strategy.*;
import kr.ac.mju.idpl.melon.metric.AppExecutionResult;
import kr.ac.mju.idpl.melon.metric.ExecutorExecutionResult;
import kr.ac.mju.idpl.melon.rpc.RPCServer;
import kr.ac.mju.idpl.melon.util.Utils;

public class MeLoN_ApplicationMaster {
	private static final Logger LOG = LoggerFactory.getLogger(MeLoN_ApplicationMaster.class);

	// Execution Configurations
	private AppExecutionType appExecutionType = null;
	private GPUAssignmentType gpuAssignmentType = null;

	// Configurations
	private Configuration yarnConf;
	private Configuration hdfsConf;

	private FileSystem resourceFs;

	private AMRMClientAsync<ContainerRequest> amRMClient;
	private NMClientAsync nmClientAsync;

	private List<Container> runningContainers = new ArrayList<>();
	// private ApplicationAttemptId appAttemptID;

	private String amHostname = "";
	private int amPort = 0;
	private String amTrackingUrl = "";

	private int numTotalContainers;

	private Map<String, List<ContainerRequest>> askedContainerMap = new HashMap<>();
	private Map<Integer, List<Container>> sessionContainersMap = new ConcurrentHashMap<>();
	private AppExecutionResult appExecutionResult = new AppExecutionResult();
	private Map<String, ExecutorExecutionResult> executorExecutionResults;

	private long appStartTime;
	private long finishTime;
	private long appExecutionTime;

	private ContainerId containerId;
	private String appIdString;
	private Configuration melonConf = new Configuration(false);
	private Map<String, LocalResource> localResources = new ConcurrentHashMap<>();
	private String hdfsClasspath;

	private AtomicInteger numCompletedContainers = new AtomicInteger();
	private AtomicInteger numAllocatedContainers = new AtomicInteger();
	private AtomicInteger numFailedContainers = new AtomicInteger();
	private AtomicInteger numRequestedContainers = new AtomicInteger();

	private Map<String, String> containerEnvs = new HashMap<>();
	// private String melonHome;
	// private String appJar;
	// private String domainController;

	private MeLoN_Session session;
	private MeLoN_Session.Builder sessionBuilder;
	private RPCServer rpcServer;
	private MeLoN_GPUAssignor gpuAssignor;

	private volatile boolean done;

	private List<Thread> launchTreads = new ArrayList<Thread>();
	private Options opts;

	private String[] nodes = new String[] { "master.hadoop.com", "slave1.hadoop.com", "slave2.hadoop.com" };
	private Map<String, MeLoN_GPUDeviceInfo> nodeGPUInfoMap = new HashMap<>();

	public MeLoN_ApplicationMaster() throws Exception {
		yarnConf = new Configuration(false);
		hdfsConf = new Configuration(false);
		opts = new Options();
		initOptions();
	}

	private void initOptions() {
	}

	private boolean init(String[] args) {
		LOG.info("Starting initialization ...");
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
		String[] containersEnvsStr = melonConf.getStrings(MeLoN_ConfigurationKeys.CONTAINER_ENVS);
		containerEnvs = Utils.parseKeyValue(containersEnvsStr);
		containerId = ContainerId.fromString(envs.get(ApplicationConstants.Environment.CONTAINER_ID.name()));
		appIdString = containerId.getApplicationAttemptId().getApplicationId().toString();
//		hdfsClasspath = cliParser.getOptionValue("hdfs_classpath");
		appExecutionType = AppExecutionType.valueOf(melonConf.get(MeLoN_ConfigurationKeys.EXECUTION_TYPE).toUpperCase());
		gpuAssignmentType = GPUAssignmentType.valueOf(melonConf.get(MeLoN_ConfigurationKeys.GPU_ASSIGNMENT_TYPE).toUpperCase());

		return true;
	}

	private boolean run(String[] args) throws IOException, YarnException, InterruptedException {
		LOG.info("Starting ApplicationMaster ...");
		appStartTime = System.currentTimeMillis();
		if (!init(args)) {
			return false;
		}
		LOG.info("Execution Type : " + appExecutionType);
		LOG.info("GPU Allocation Type : " + gpuAssignmentType);

		LOG.info("Starting amRMClient...");
		AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
		amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
		amRMClient.init(yarnConf);
		amRMClient.start();

		amHostname = System.getenv(ApplicationConstants.Environment.NM_HOST.name());
		rpcServer = new RPCServer.Builder().setHostname(amHostname).setYarnConf(yarnConf).build();
		session = buildSession();
		rpcServer.setNewSession(session);
		amPort = rpcServer.getRpcPort();
		
		// put AppMaster's information into container environments
		containerEnvs.put(MeLoN_Constants.AM_HOST, amHostname);
		containerEnvs.put(MeLoN_Constants.AM_PORT, Integer.toString(amPort));

//		String amIPPort = NetUtils.getLocalInetAddress(amHostname).getHostAddress() + ":" + amPort;
		RegisterApplicationMasterResponse response = amRMClient.registerApplicationMaster(amHostname, amPort,
				amTrackingUrl);
		LOG.info("MeLoN_ApplicationMaster is registered with response : {}", response.toString());

		LOG.info("Starting NMCallbackHandler...");
		NMCallbackHandler containerListener = new NMCallbackHandler();
		nmClientAsync = new NMClientAsyncImpl(containerListener);
		nmClientAsync.init(yarnConf);
		nmClientAsync.start();
		
		LOG.info("Starting application RPC server at: " + amHostname + ":" + amPort);
		rpcServer.start();
		session.setResources(yarnConf, hdfsConf, localResources, containerEnvs, hdfsClasspath);
		List<MeLoN_ContainerRequest> requests = session.getContainerRequests();
		LOG.info("Requests : " + requests.toString());
		
		GPUAssignmentStrategy strategy;
		switch(gpuAssignmentType) {
			case OVERPROVISION:
				strategy = new Overprovision();
				break;
			default :
				strategy = new Exclusive();
		}
		gpuAssignor = new MeLoN_GPUAssignor(nodes, strategy, requests, appExecutionType);

		boolean allReq = false;
		rpcServer.reset();
		nodeGPUInfoMap.clear();
		while (!done) {
			if (!allReq) {
				gpuAssignor.gpuDeviceAssignment();
				allReq = true;
				for (MeLoN_ContainerRequest request : requests) {
					LOG.info("Requesting container ...");
					ContainerRequest containerAsk = setupContainerAskForRM(request);
					if (containerAsk != null) {
						if (!askedContainerMap.containsKey(request.getJobName())) {
							askedContainerMap.put(request.getJobName(), new ArrayList<>());
						}
						askedContainerMap.get(request.getJobName()).add(containerAsk);
						amRMClient.addContainerRequest(containerAsk);
					} else{
						int requestedNum = 0;
						for(String jobName : askedContainerMap.keySet()) {
							requestedNum += askedContainerMap.get(jobName).size();
						}
						if(requestedNum < requests.size()) {
							allReq = false;
						}
					}
				}
			}
			int numTotalTrackedTasks = session.getTotalTrackedTasks();
			if ((numTotalTrackedTasks > 0 ? (float) session.getNumCompletedTrackedTasks() / numTotalTrackedTasks
					: 0) == 1.0f) {
				stopRunningContainers();
				LOG.info("Training has finished. - All tasks");
				break;
			}
			if (session.isTrainingFinished()) {
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
		finishTime = System.currentTimeMillis();
		appExecutionTime = finishTime - appStartTime;
		nmClientAsync.stop();
		amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "Application complete!", null);
		amRMClient.stop();
		session.updateTrainingFinalStatus();
		FinalApplicationStatus status = session.getTrainingFinalStatus();
		if (status != FinalApplicationStatus.SUCCEEDED) {
			LOG.info("Training finished with failure!");
		} else {
			LOG.info("Training finished successfully!");
		}
		printResult();
		return status == FinalApplicationStatus.SUCCEEDED;
	}
	
	private void printResult() {
		appExecutionResult.setApplicationId(appIdString);
		appExecutionResult.setAppExecutionTime(appExecutionTime);
		appExecutionResult.setAppExecutionType(appExecutionType.name());
		appExecutionResult.setGpuAssignmentType(gpuAssignmentType.name());
		executorExecutionResults = rpcServer.getExecutorExecutionResults();
		LOG.info("===============Application Summary===============");
		LOG.info("Application ID : {}", appExecutionResult.getApplicationId());
		LOG.info("App Execution Type : {}", appExecutionResult.getAppExecutionType());
		LOG.info("GPU Assignment Type : {}", appExecutionResult.getGpuAssignmentType());
		LOG.info("App Execution Time : {} (sec)", appExecutionResult.getAppExecutionTime() / 1000);
		for (String entry : executorExecutionResults.keySet()) {
			LOG.info("Worker ID = {}", entry);
		}
		if (executorExecutionResults != null) {
			LOG.info("================Worker Summary===============");
			for (Map.Entry<String, ExecutorExecutionResult> entry : executorExecutionResults.entrySet()) {
				ExecutorExecutionResult eer = entry.getValue();
				LOG.info("Task ID : {}, Exit Code : {}", eer.getTaskId(), eer.getExitCode());
				LOG.info("Device : {}:{}, Fraction : {}", eer.getHost(), eer.getDevice(), eer.getFraction());
				LOG.info("Process Execution Time : {} (sec)", eer.getProcessExecutionTime() / 1000);
				LOG.info("Executor Execution Time : {} (sec)", eer.getExecutorExecutionTime() / 1000);
				LOG.info("----------------------------------------");
			}
		}
	}

	private void reset() {
		List<Container> containers = sessionContainersMap.get(session.sessionId);
		for (Container container : containers) {
			nmClientAsync.stopContainerAsync(container.getId(), container.getNodeId());
			LOG.info("Stop a task in container: containerId = " + container.getId() + ", containerNode = "
					+ container.getNodeId().getHost());
		}

		// Reset session
		session = sessionBuilder.build();
		rpcServer.reset();
		session.sessionId += 1;
	}
	
	private MeLoN_Session buildSession() {
		this.sessionBuilder = new MeLoN_Session.Builder().setMelonConf(melonConf)
				.setTaskExecutorJVMArgs(melonConf.get(MeLoN_ConfigurationKeys.TASK_EXECUTOR_JVM_OPTS,
						MeLoN_ConfigurationKeys.TASK_EXECUTOR_JVM_OPTS_DEFAULT));
		return sessionBuilder.build();
	}

	private void stopRunningContainers() throws InterruptedException {
		List<Container> allContainers = sessionContainersMap.get(session.sessionId);
		if (runningContainers != null) {
			for (Container container : runningContainers) {
				MeLoN_Task task = session.getTask(container.getId());
				if (!task.isCompleted()) {
					nmClientAsync.stopContainerAsync(container.getId(), container.getNodeId());
				}
			}
		}

		// Give 15 seconds for containers to exit
		Thread.sleep(15000);
		boolean result = session.getNumCompletedTasks() == session.getTotalTasks();
		if (!result) {
			LOG.warn("Not all containers were stopped or completed. Only " + session.getNumCompletedTasks()
					+ " out of " + session.getTotalTasks() + " finished.");
		}
	}

	private ContainerRequest setupContainerAskForRM(MeLoN_ContainerRequest request) {
		Priority priority = Priority.newInstance(request.getPriority());
		Resource capability = Resource.newInstance((int) request.getMemory(), request.getvCores());
		Utils.setCapabilityGPU(capability, request.getGpus());
		boolean done = false;
		List<MeLoN_GPURequest> gpuRequests;
		String[] node = null;
		ContainerRequest containerAsk = null;
		gpuRequests = gpuAssignor.getGPURequests();
		if(!gpuRequests.isEmpty()) {
			for (MeLoN_GPURequest gpuReq : gpuAssignor.getGPURequests()) {
				if (gpuReq.getJobName().equals(request.getJobName())) {
					if (gpuReq.isAssigned()) {
						node = new String[] { gpuReq.getDevice().getDeviceHost() };
						LOG.info("Task({}) will be launched at {}.", gpuReq.getJobName(), gpuReq.getDevice().getDeviceId());
						gpuReq.setStatusRequested();
						done = true;
						break;
					}
				}
			} 
		} else {
			LOG.info("Task({}) will be launched at somewhere.", request.getJobName());
			done = true;
		}
		if (done) {
			containerAsk = new ContainerRequest(capability, node, null, priority, false);
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

//	public synchronized Map<String, String> getGPUDeviceEnv(Container container, MeLoN_Task task) {
//		Map<String, String> env = new ConcurrentHashMap<>();
//		for (GPURequest gpuReq : gpuAllocator.getGPUDeviceallocInfo()) {
//			if (gpuReq.getRequestTask().equals(task.getJobName()) && gpuReq.getDevice() != null
//					&& gpuReq.getDevice().getDeviceHost().equals(container.getNodeId().getHost())
//					&& gpuReq.isRequested()) {
//				gpuReq.setStatusAllocated();
//				gpuReq.setContainerId(container.getId());
//				env.put(MeLoN_Constants.CUDA_VISIBLE_DEVICES, String.valueOf(gpuReq.getDevice().getDeviceNum()));
//				env.put(MeLoN_Constants.FRACTION, gpuReq.getFraction());
//				LOG.info("\n***Extra envs set." + "\n***Task = " + task.getJobName() + ":" + task.getTaskIndex()
//						+ "\n***Device = " + gpuReq.getDevice().getDeviceId() + ", Using "
//						+ gpuReq.getRequiredGPUMemory() + "/" + gpuReq.getDevice().getTotal() + "MB, Fraction = "
//						+ gpuReq.getFraction() + "\n***ContainerId = " + container.getId());
//				break;
//			}
//		}
//		return env;
//	}

	private class NMCallbackHandler implements NMClientAsync.CallbackHandler {

		@Override
		// The API is called when NodeManager responds to indicate its acceptance of the
		// starting container request
		public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
			LOG.info("Successfully started container " + containerId);
		}

		@Override
		// The API is called when NodeManager responds with the status of the container
		public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
			LOG.info("Container Status: id = {}, status = {}", containerId, containerStatus);
		}

		@Override
		// The API is called when NodeManager responds to indicate the container is
		// stopped.
		public void onContainerStopped(ContainerId containerId) {
			LOG.info("Container {} finished with exitStatus {}.", containerId, ContainerExitStatus.KILLED_BY_APPMASTER);
			processFinishedContainer(containerId, ContainerExitStatus.KILLED_BY_APPMASTER);
		}

		@Override
		// The API is called when an exception is raised in the process of querying the
		// status of a container
		public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
			LOG.error("Failed to query the status of container {}", containerId);
		}

		@Override
		// The API is called when an exception is raised in the process of starting a
		// container
		public void onStartContainerError(ContainerId containerId, Throwable t) {
			LOG.info("onStartContainerError {}", containerId);
			LOG.error("Failed to start container {}", containerId);
			// need processing something
		}

		@Override
		// The API is called when an exception is raised in the process of stopping a
		// container
		public void onStopContainerError(ContainerId containerId, Throwable t) {
			LOG.info("onStopContainerError {}", containerId);
			LOG.error("Failed to stop container {}", containerId);
		}

	}

	private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {

		@Override
		public float getProgress() {
			int numTotalTrackedTasks = session.getTotalTrackedTasks();
			return numTotalTrackedTasks > 0 ? (float) session.getNumCompletedTrackedTasks() / numTotalTrackedTasks
					: 0;
		}

		@Override
		// Called when the ResourceManager responds to a heartbeat with allocated
		// containers.
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
		// Called when the ResourceManager responds to a heartbeat with completed
		// containers.
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
			int numTotalTrackedTasks = session.getTotalTrackedTasks();
			LOG.info("numTotalTrackedTasks: {}", numTotalTrackedTasks);
			LOG.info("rpcServer.getNumCompletedTrackedTasks(): {}", session.getNumCompletedTrackedTasks());
			float prgrs = numTotalTrackedTasks > 0
					? (float) session.getNumCompletedTrackedTasks() / numTotalTrackedTasks
					: 0;
			LOG.info("getProgress: {}", getProgress());
		}

		@Override
		// Called when error comes from RM communications as well as from errors in the
		// callback itself from the app.
		public void onError(Throwable throwable) {
			LOG.error("Received error in AM to RM call", throwable);
			done = true;
			amRMClient.stop();
			nmClientAsync.stop();
			// need more detail
		}

		@Override
		// Called when nodes tracked by the ResourceManager have changed in health,
		// availability etc.
		public void onNodesUpdated(List<NodeReport> arg0) {
			LOG.info("onNodesUpdated called in RMCAllbackHandler");
		}

		@Override
		// Called when the ResourceManager wants the ApplicationMaster to shutdown for
		// being out of sync etc.
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
			MeLoN_Task task = session.getAndInitMatchingTaskByPriority(container.getPriority().getPriority());
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
			containerLaunchEnvs.put(MeLoN_Constants.TASK_NUM, String.valueOf(session.getTotalTrackedTasks()));
			// containerLaunchEnvs.put(MeLoN_Constants.SESSION_ID,
			// String.valueOf(appSession.sessionId));

			containerLaunchEnvs.put(MeLoN_Constants.APP_EXECUTION_TYPE, appExecutionType.name());
			containerLaunchEnvs.put(MeLoN_Constants.APP_ID, appIdString);

			containerLaunchEnvs.putAll(gpuAssignor.getGPUDeviceEnv(container, task));

			// Add job type specific resources
			Map<String, LocalResource> containerResources = new ConcurrentHashMap<>(localResources);
			String[] resources = melonConf.getStrings(MeLoN_ConfigurationKeys.getResourcesKey(task.getJobName()));
			Utils.addResources(resources, containerResources, resourceFs);

			// All resources available to all containers
			resources = melonConf.getStrings(MeLoN_ConfigurationKeys.CONTAINER_RESOURCES);
			Utils.addResources(resources, containerResources, resourceFs);

			task.addContainer(container);
			session.addContainer(container.getId(), task);
			LOG.info("Setting Container [" + container.getId() + "] for task [" + task.getId() + "]..");

			List<String> vargs = new ArrayList<>();
			vargs.add(session.getTaskCommand());
			vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/melon.stdout");
			vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/melon.stderr");
			String command = String.join(" ", vargs);
			List<String> commands = new ArrayList<String>();
			commands.add(command);
			LOG.info("Constructed command " + commands);
			LOG.info("Container environment: " + containerLaunchEnvs);

			ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
			ctx.setLocalResources(containerResources);
			ctx.setEnvironment(containerLaunchEnvs);
			ctx.setCommands(commands);

			sessionContainersMap
					.computeIfAbsent(session.sessionId, key -> Collections.synchronizedList(new ArrayList<>()))
					.add(container);

			nmClientAsync.startContainerAsync(container, ctx);
			task.setStatus(TaskStatus.RUNNING);
			LOG.info("Container {} launched!", container.getId());
		}

	}

	private void processFinishedContainer(ContainerId containerId, int exitStatus) {
		MeLoN_Task task = session.getTask(containerId);
		if (task != null) {
			// Ignore tasks from past sessions.
			if (task.getSessionId() != session.sessionId) {
				return;
			}
			gpuAssignor.onTaskCompleted(containerId);
			LOG.info("Container {} for task {}:{} finished with exitStatus: {}.", containerId, task.getJobName(),
					task.getTaskIndex(), exitStatus);
			session.onTaskCompleted(task.getJobName(), task.getTaskIndex(), exitStatus);

		} else {
			LOG.warn("No task found for container : [" + containerId + "]!");
		}

	}
}
