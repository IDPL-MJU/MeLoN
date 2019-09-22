package kr.ac.mju.idpl.melon.rpc;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import kr.ac.mju.idpl.melon.MeLoN_Constants;
import kr.ac.mju.idpl.melon.MeLoN_ContainerRequest;
import kr.ac.mju.idpl.melon.MeLoN_Task;
import kr.ac.mju.idpl.melon.MeLoN_TaskExecutor;
import kr.ac.mju.idpl.melon.util.Utils;

public class RPCServer extends Thread implements RPCProtocol {
	private static final Logger LOG = LoggerFactory.getLogger(RPCServer.class);

	// for communication with MeLoN_TaskExecutor
	private Random RANDOM_NUMBER_GENERATOR = new Random();
	private String rpcAddress;
	private int rpcPort;
	Configuration yarnConf;
	private Server server;

	// application information
	private String appId;
	private long startingTime;
	private long finishingTime;
	private String jvmArgs;
	private Configuration melonConf;
	private Map<ContainerId, MeLoN_Task> containerIdMap = new HashMap<>();
	private Set<String> registeredTasks = new HashSet<>();
	private Map<ContainerId, String> containerSpec = new ConcurrentHashMap<>(); // <ContainerId, hostname:port>
	private Map<String, MeLoN_ContainerRequest> containerRequests;
	private Map<String, MeLoN_Task[]> jobTasks = new ConcurrentHashMap<>();
	private boolean trainingFinished = false;
	private FinalApplicationStatus trainingFinalStatus = FinalApplicationStatus.UNDEFINED;

	public RPCServer(Builder builder) {
		this.jvmArgs = builder.jvmArgs;
		this.containerRequests = Utils.parseContainerRequests(builder.melonConf);
		for (Map.Entry<String, MeLoN_ContainerRequest> entry : containerRequests.entrySet()) {
			jobTasks.put(entry.getKey(), new MeLoN_Task[entry.getValue().getNumInstances()]);
		}
		this.melonConf = builder.melonConf;
		this.rpcAddress = builder.hostname;
		this.rpcPort = 10000 + RANDOM_NUMBER_GENERATOR.nextInt(5000) + 1;
		this.yarnConf = builder.yarnConf;
	}

	public String getTaskCommand() {
		StringJoiner cmd = new StringJoiner(" ");
		cmd.add("$JAVA_HOME/bin/java").add(jvmArgs).add(MeLoN_TaskExecutor.class.getName());
		return cmd.toString();
	}

	public List<MeLoN_ContainerRequest> getContainerRequests() {
		List<MeLoN_ContainerRequest> requests = new ArrayList<>();
		for (Map.Entry<String, MeLoN_Task[]> entry : jobTasks.entrySet()) {
			MeLoN_Task[] tasks = entry.getValue();
			for (MeLoN_Task task : tasks) {
				if (task == null) {
					requests.add(containerRequests.get(entry.getKey()));
				}
			}
		}
		return requests;
	}

	public synchronized MeLoN_Task getAndInitMatchingTaskByPriority(int priority) {
		for (Map.Entry<String, MeLoN_ContainerRequest> entry : containerRequests.entrySet()) {
			String taskType = entry.getKey();
			if (entry.getValue().getPriority() != priority) {
				LOG.debug("Ignoring taskType {" + taskType + "} as priority doesn't match");
				continue;
			}
			MeLoN_Task[] tasks = jobTasks.get(taskType);
			for (int i = 0; i < tasks.length; i++) {
				if (tasks[i] == null) {
					tasks[i] = new MeLoN_Task(taskType, String.valueOf(i));
					return tasks[i];
				}
			}
		}
		return null;
	}

	public void updateTrainingFinalStatus() {
		int failureCount = 0;
		if (trainingFinalStatus == FinalApplicationStatus.FAILED) {
			return;
		}
		for (Map.Entry<String, MeLoN_Task[]> entry : jobTasks.entrySet()) {
			String jobName = entry.getKey();
			MeLoN_Task[] tasks = entry.getValue();

			// If the task type is not tracked, continue.
			if (!Utils.isJobTypeTracked(jobName, melonConf)) {
				continue;
			}

			for (MeLoN_Task task : tasks) {
				if (task == null) {
					String msg = "Job is null, this should not happen.";
					LOG.error(msg);
					trainingFinalStatus = FinalApplicationStatus.FAILED;
					return;
				}
				boolean isCompleted = task.isCompleted();
				if (!isCompleted) {
					LOG.error("Job " + task + " hasn't finished yet.");
					trainingFinalStatus = FinalApplicationStatus.FAILED;
					return;
				}

				int exitStatus = task.getExitStatus();
				if (exitStatus != 0) {
					failureCount++;
				}
			}
		}

		if (failureCount > 0) {
			LOG.error("At least one job task exited with non-zero status, failedCnt=" + failureCount);
			trainingFinalStatus = FinalApplicationStatus.FAILED;
		} else {
			LOG.info("Training completed with no job failures, setting final status SUCCEEDED.");
			trainingFinalStatus = FinalApplicationStatus.SUCCEEDED;
		}
	}

	public boolean isTrainingFinished() {
		return trainingFinished;
	}

	public FinalApplicationStatus getTrainingFinalStatus() {
		return trainingFinalStatus;
	}

	public void setResources(Configuration yarnConf, Configuration hdfsConf, Map<String, LocalResource> localResources,
			Map<String, String> shellEnv, String hdfsClasspathDir) {

		Map<String, String> env = System.getenv();
		String melonConfPath = env.get(MeLoN_Constants.MELON_CONF_PREFIX + MeLoN_Constants.PATH_SUFFIX);
		long melonConfTimestamp = Long
				.parseLong(env.get(MeLoN_Constants.MELON_CONF_PREFIX + MeLoN_Constants.TIMESTAMP_SUFFIX));
		long melonConfLength = Long
				.parseLong(env.get(MeLoN_Constants.MELON_CONF_PREFIX + MeLoN_Constants.LENGTH_SUFFIX));

		LocalResource melonConfResource = LocalResource.newInstance(
				ConverterUtils.getYarnUrlFromURI(URI.create(melonConfPath)), LocalResourceType.FILE,
				LocalResourceVisibility.PRIVATE, melonConfLength, melonConfTimestamp);
		localResources.put(MeLoN_Constants.MELON_FINAL_XML, melonConfResource);

		try {
			if (hdfsClasspathDir != null) {
				FileSystem fs = FileSystem.get(new URI(hdfsClasspathDir), hdfsConf);
				Utils.addResource(hdfsClasspathDir, localResources, fs);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
				.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
		for (String c : yarnConf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
				YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
			classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
			classPathEnv.append(c.trim());
		}
		shellEnv.put("CLASSPATH", classPathEnv.toString());
	}

	public boolean allTasksScheduled() {
		for (MeLoN_Task[] tasks : jobTasks.values()) {
			for (MeLoN_Task task : tasks) {
				if (task == null) {
					return false;
				}
			}
		}
		return true;
	}

	public int getTotalTasks() {
		return jobTasks.values().stream().reduce(0, (currTotal, taskArr) -> currTotal + taskArr.length,
				(count1, count2) -> count1 + count2);
	}

	public int getTotalTrackedTasks() {
		return jobTasks.entrySet().stream().filter(entry -> Utils.isJobTypeTracked(entry.getKey(), melonConf))
				.mapToInt(entry -> entry.getValue().length).sum();
	}

	public int getNumCompletedTrackedTasks() {
		return (int) jobTasks.entrySet().stream().filter(entry -> Utils.isJobTypeTracked(entry.getKey(), melonConf))
				.flatMap(entry -> Arrays.stream(entry.getValue())).filter(task -> task != null && task.isCompleted())
				.count();
	}

	private MeLoN_Task getTask(String taskType, String taskIndex) {
		for (Map.Entry<String, MeLoN_Task[]> entry : jobTasks.entrySet()) {
			MeLoN_Task[] tasks = entry.getValue();
			for (MeLoN_Task task : tasks) {
				String type = task.getTaskType();
				String index = task.getTaskIndex();
				if (type.equals(taskType) && index.equals(taskIndex)) {
					return task;
				}
			}
		}
		return null;
	}

	public void addContainer(ContainerId containerId, MeLoN_Task task) {
		containerIdMap.put(containerId, task);
	}

	public void run() {
		try {
			server = new RPC.Builder(yarnConf).setProtocol(RPCProtocol.class).setInstance(this)
					.setBindAddress(rpcAddress).setPort(rpcPort).build();
			server.start();
		} catch (Exception e) {
			throw new RuntimeException();
		}
	}

	@Override
	public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash)
			throws IOException {
		return ProtocolSignature.getProtocolSignature(this, protocol, clientVersion, clientMethodsHash);
	}

	@Override
	public long getProtocolVersion(String protocol, long version) throws IOException {
		return RPCProtocol.versionID;
	}

	@Override
	public String getClusterSpec() throws IOException, YarnException {
		Map<String, List<String>> map = new HashMap<>();

		for (Map.Entry<String, MeLoN_Task[]> entry : jobTasks.entrySet()) {
			String taskType = entry.getKey();
			MeLoN_Task[] tasks = entry.getValue();

			List<String> builder = new ArrayList<>();
			for (MeLoN_Task task : tasks) {
				if (task == null) {
					continue;
				}

				String hostPort = task.getHostPort();
				builder.add(hostPort);
			}
			map.put(taskType, builder);
		}
		ObjectMapper objectMapper = new ObjectMapper();
		return objectMapper.writeValueAsString(map);
	}

	@Override
	public String registerWorkerSpec(String taskId, String spec) throws IOException, YarnException {
		int idx = taskId.indexOf(":");
		MeLoN_Task task = getTask(taskId.substring(0, idx), taskId.substring(idx + 1));
		if (task.getHost() == null) {
			LOG.info("Received cluster spec registration request from task " + taskId + " with spec: " + spec);
			task.setHostPort(spec);
			registeredTasks.add(taskId);
		}

		int totalTasks = getTotalTasks();
		if (registeredTasks.size() == totalTasks) {
			LOG.info("All " + totalTasks + " tasks registered.");
			return getClusterSpec();
		} else {
			LOG.info(registeredTasks.size() + "/" + totalTasks + " tasks registered.");
			return null;
		}
	}

	@Override
	public String registerExecutionResult(int exitCode, String taskType, String taskIndex) throws Exception {
		LOG.info("Received result registration request with exit code " + exitCode + " from " + taskType + " " + taskIndex);
		MeLoN_Task task = getTask(taskType, taskIndex);
		return "RECEIVED";
	}

	public int getRpcPort() {
		return rpcPort;
	}

	public static class Builder {
		private String jvmArgs;
		private Configuration melonConf;
		private String hostname;
		private Configuration yarnConf;

		public RPCServer build() {
			return new RPCServer(this);
		}

		public Builder setTaskExecutorJVMArgs(String jvmArgs) {
			this.jvmArgs = jvmArgs;
			return this;
		}

		public Builder setMelonConf(Configuration melonConf) {
			this.melonConf = melonConf;
			return this;
		}

		public Builder setHostname(String hostname) {
			this.hostname = hostname;
			return this;
		}

		public Builder setYarnConf(Configuration yarnConf) {
			this.yarnConf = yarnConf;
			return this;
		}
	}

}
