package kr.ac.mju.idpl.melon;

import java.io.File;
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

import kr.ac.mju.idpl.melon.rpc.RPCServer;
import kr.ac.mju.idpl.melon.util.Utils;

public class MeLoN_ApplicationMaster {
	private static final Logger LOG = LoggerFactory.getLogger(MeLoN_ApplicationMaster.class);
	private Configuration yarnConf;
	private Configuration hdfsConf;

	private FileSystem resourceFs;

	private AMRMClientAsync<ContainerRequest> amRMClient;
	private NMClientAsync nmClientAsync;
	private NMCallbackHandler containerListener;

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
	private String melonHome;
	private String appJar;
	private String domainController;

	private RPCServer rpcServer;

	private volatile boolean done;
	private volatile boolean success;

	private List<Thread> launchTreads = new ArrayList<Thread>();
	private Options opts;

	public MeLoN_ApplicationMaster() throws Exception {
		yarnConf = new Configuration(false);
		hdfsConf = new Configuration(false);
		opts = new Options();
		initOptions();
	}

	private void initOptions() {
		opts.addOption("hdfs_classpath", true, "Path to jars on HDFS for workers.");
		opts.addOption("python_binary_path", true, "The relative path to python binary.");
		opts.addOption("python_venv", true, "The python virtual environment zip.");
	}

	private boolean init(String[] args) {
		LOG.info("Starting init...");
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

		return true;
	}

	private void printUsage() {
		// TODO Auto-generated method stub

	}

	private boolean run(String[] args) throws IOException, YarnException {
		long started = System.currentTimeMillis();
		if (!init(args)) {
			return false;
		}

		AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
		amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
		amRMClient.init(yarnConf);
		amRMClient.start();
		LOG.info("Starting amRMClient...");

		amHostname = System.getenv(ApplicationConstants.Environment.NM_HOST.name());
		// amHostname = NetUtils.getHostname();
		rpcServer = new RPCServer.Builder().setHostname(amHostname).setYarnConf(yarnConf)
				.setTaskExecutorJVMArgs(melonConf.get(MeLoN_ConfigurationKeys.TASK_EXECUTOR_JVM_OPTS,
						MeLoN_ConfigurationKeys.TASK_EXECUTOR_JVM_OPTS_DEFAULT))
				.setMelonConf(melonConf).build();
		// rpcServer = new RPCServer(amHostname, yarnConf);
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

		rpcServer.start();
		rpcServer.setResources(yarnConf, hdfsConf, localResources, containerEnvs, hdfsClasspath);
		List<MeLoN_ContainerRequest> requests = rpcServer.getContainerRequests();
		for (MeLoN_ContainerRequest request : requests) {
			ContainerRequest containerAsk = setupContainerAskForRM(request);
			if (!askedContainerMap.containsKey(request.getTaskType())) {
				askedContainerMap.put(request.getTaskType(), new ArrayList<>());
			}
			askedContainerMap.get(request.getTaskType()).add(containerAsk);
			amRMClient.addContainerRequest(containerAsk);
		}
		while (true) {
			if (rpcServer.isTrainingFinished()) {
				LOG.info("Training has finished.");
				break;
			}

			// Pause before refresh job status
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				LOG.error("Thread interrupted", e);
			}
		}
		rpcServer.updateTrainingFinalStatus();
		FinalApplicationStatus status = rpcServer.getTrainingFinalStatus();
		if (status != FinalApplicationStatus.SUCCEEDED) {
			LOG.info("Training finished with failure!");
		}else {
			LOG.info("Training finished successfully!");
		}
		return status == FinalApplicationStatus.SUCCEEDED;
	}

	private ContainerRequest setupContainerAskForRM(MeLoN_ContainerRequest request) {
		Priority priority = Priority.newInstance(request.getPriority());
		Resource capability = Resource.newInstance((int) request.getMemory(), request.getvCores());
		Utils.setCapabilityGPU(capability, request.getGpus());
		ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priority);
		LOG.info("Requested container ask: " + containerAsk.toString());
		return containerAsk;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		MeLoN_ApplicationMaster appMaster = new MeLoN_ApplicationMaster();
		boolean succeeded = appMaster.run(args);
		if (succeeded) {
			LOG.info("Application finished successfully.");
			System.exit(0);
		} else {
			LOG.error("Failed to finish MeLoN_ApplicationMaster seccessfully.");
			System.exit(-1);
		}
	}

	private class NMCallbackHandler implements NMClientAsync.CallbackHandler {

		@Override
		public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
			LOG.info("Successfully started container " + containerId);
		}

		@Override
		public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
			LOG.info("Container Status: id =" + containerId + ", status =" + containerStatus);
		}

		@Override
		public void onContainerStopped(ContainerId containerId) {
			LOG.info("Container " + containerId + " finished with exitStatus " + ContainerExitStatus.KILLED_BY_APPMASTER
					+ ".");
		}

		@Override
		public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
			LOG.error("Failed to query the status of container " + containerId);
		}

		@Override
		public void onStartContainerError(ContainerId containerId, Throwable t) {
			LOG.error("Failed to start container " + containerId);
		}

		@Override
		public void onStopContainerError(ContainerId containerId, Throwable t) {
			LOG.error("Failed to stop container " + containerId);
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
					LOG.error(diagnotics);
				} else {
					LOG.info(diagnotics);
				}
			}
		}

		@Override
		public void onError(Throwable throwable) {
			LOG.error("Received error in AM to RM call", throwable);
			done = true;
			amRMClient.stop();
		}

		@Override
		public void onNodesUpdated(List<NodeReport> arg0) {
			LOG.info("onNodesUpdated called in RMCAllbackHandler");
		}

		@Override
		public void onShutdownRequest() {
			LOG.info("onShutdownRequest called in RMCallbackHandler");
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
			task.setStatus(MeLoN_TaskStatus.READY);

			// Add job type specific resources
			Map<String, LocalResource> containerResources = new ConcurrentHashMap<>(localResources);
			String[] resources = melonConf.getStrings(MeLoN_ConfigurationKeys.getResourcesKey(task.getTaskType()));
			Utils.addResources(resources, containerResources, resourceFs);

			// All resources available to all containers
			resources = melonConf.getStrings(MeLoN_ConfigurationKeys.CONTAINER_RESOURCES);
			Utils.addResources(resources, containerResources, resourceFs);

			task.addContainer(container);
			rpcServer.addContainer(container.getId(), task);
			LOG.info("Setting Container [" + container.getId() + "] for task [" + task.getId() + "]..");

			Map<String, String> containerLaunchEnvs = new ConcurrentHashMap<>(containerEnvs);

			String taskType = task.getTaskType();
			String taskIndex = task.getTaskIndex();
			containerLaunchEnvs.put(MeLoN_Constants.TASK_TYPE, taskType);
			containerLaunchEnvs.put(MeLoN_Constants.TASK_INDEX, taskIndex);
			containerLaunchEnvs.put(MeLoN_Constants.TASK_NUM, String.valueOf(rpcServer.getTotalTrackedTasks()));
			// containerLaunchEnvs.put(MeLoN_Constants.SESSION_ID,
			// String.valueOf(appSession.sessionId));

			List<String> vargs = new ArrayList<>();
			vargs.add(rpcServer.getTaskCommand());
			vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/MeLoN_TaskExecutor.stdout");
			vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/MeLoN_TaskExecutor.stderr");
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
			task.setStatus(MeLoN_TaskStatus.RUNNING);
			LOG.info("Container {} launched!", container.getId());

		}
	}
}
