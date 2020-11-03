package kr.ac.mju.idpl.melon;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import kr.ac.mju.idpl.melon.util.Utils;

public class MeLoN_Session {
	private static final Logger LOG = LoggerFactory.getLogger(MeLoN_Session.class);
	public int sessionId = 0;
	private String jvmArgs;
	private Configuration melonConf;
	private Map<String, MeLoN_ContainerRequest> containerRequests;
	private Map<String, MeLoN_Task[]> jobTasks = new ConcurrentHashMap<>();
	private FinalApplicationStatus trainingFinalStatus = FinalApplicationStatus.UNDEFINED;
	private Map<ContainerId, MeLoN_Task> containerIdMap = new HashMap<>();
	private boolean trainingFinished = false;

	public MeLoN_Session(Builder builder) {
		this.jvmArgs = builder.jvmArgs;
		this.melonConf = builder.melonConf;
		this.containerRequests = Utils.parseContainerRequests(builder.melonConf);

		for (Map.Entry<String, MeLoN_ContainerRequest> entry : containerRequests.entrySet()) {
			LOG.info("jobtasks put request : " + entry.getValue().getJobName() + " - mem:"
					+ entry.getValue().getMemory() + ", vcores:" + entry.getValue().getvCores());
			jobTasks.put(entry.getKey(), new MeLoN_Task[entry.getValue().getNumInstances()]);
		}
	}

	public String getTaskCommand() {
		StringJoiner cmd = new StringJoiner(" ");
		cmd.add("$JAVA_HOME/bin/java").add(jvmArgs).add(MeLoN_TaskExecutor.class.getName());
		return cmd.toString();
	}

	public boolean isTrainingFinished() {
		return trainingFinished;
	}

	public FinalApplicationStatus getTrainingFinalStatus() {
		return trainingFinalStatus;
	}

	public void setResources(Configuration yarnConf, Configuration hdfsConf, Map<String, LocalResource> localResources,
			Map<String, String> containerEnvs, String hdfsClasspathDir) {

		Map<String, String> env = System.getenv();
		
		// put melon configuration file to localResources
		String melonConfPath = env.get(MeLoN_Constants.MELON_CONF_PREFIX + MeLoN_Constants.PATH_SUFFIX);
		long melonConfTimestamp = Long
				.parseLong(env.get(MeLoN_Constants.MELON_CONF_PREFIX + MeLoN_Constants.TIMESTAMP_SUFFIX));
		long melonConfLength = Long
				.parseLong(env.get(MeLoN_Constants.MELON_CONF_PREFIX + MeLoN_Constants.LENGTH_SUFFIX));
		LocalResource melonConfResource = LocalResource.newInstance(
				ConverterUtils.getYarnUrlFromURI(URI.create(melonConfPath)), LocalResourceType.FILE,
				LocalResourceVisibility.PRIVATE, melonConfLength, melonConfTimestamp);
		localResources.put(MeLoN_Constants.MELON_FINAL_XML, melonConfResource);

		// put melon jar file to localResources
		String melonJarPath = env.get(MeLoN_Constants.MELON_JAR_PREFIX + MeLoN_Constants.PATH_SUFFIX);
		LOG.info("melonJarPath : " + melonJarPath);
		long melonJarTimestamp = Long
				.parseLong(env.get(MeLoN_Constants.MELON_JAR_PREFIX + MeLoN_Constants.TIMESTAMP_SUFFIX));
		long melonJarLength = Long.parseLong(env.get(MeLoN_Constants.MELON_JAR_PREFIX + MeLoN_Constants.LENGTH_SUFFIX));
		LocalResource melonJarResource = LocalResource.newInstance(
				ConverterUtils.getYarnUrlFromURI(URI.create(melonJarPath)), LocalResourceType.FILE,
				LocalResourceVisibility.PRIVATE, melonJarLength, melonJarTimestamp);
		localResources.put(MeLoN_Constants.MELON_JAR, melonJarResource);

		try {
			if (hdfsClasspathDir != null) {
				LOG.info("hdfsClasspathDir is null");
				FileSystem fs = FileSystem.get(new URI(hdfsClasspathDir), hdfsConf);
				Utils.addResource(hdfsClasspathDir, localResources, fs);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		// put classPathEnv to containerEnvs
		StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
				.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
		for (String c : yarnConf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
				YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
			classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
			classPathEnv.append(c.trim());
		}
		containerEnvs.put("CLASSPATH", classPathEnv.toString());
	}

	public List<MeLoN_ContainerRequest> getContainerRequests() {
		LOG.info("Calling getContainerRequests");
		List<MeLoN_ContainerRequest> requests = new ArrayList<>();
		for (Map.Entry<String, MeLoN_Task[]> entry : jobTasks.entrySet()) {
			MeLoN_Task[] tasks = entry.getValue();
			for (MeLoN_Task task : tasks) {
				if (task == null) {
					requests.add(containerRequests.get(entry.getKey()));
				}
			}
		}
		for(int i=0;i<requests.size();i++) {
			LOG.info(i+"request name:"+requests.get(i).getJobName()+" and numinstance:"+requests.get(i).getNumInstances());
		}
		LOG.info("Return containerRequests");
		return requests;
	}

	public synchronized MeLoN_Task getAndInitMatchingTaskByPriority(int priority) {
		int test = 0;
		for (Map.Entry<String, MeLoN_ContainerRequest> entry : containerRequests.entrySet()) {
			test++;
			LOG.info("***loop = {}.", test);
			String jobName = entry.getKey();
			LOG.info("***JOB Name = {}.", jobName);
			if (entry.getValue().getPriority() != priority) {
				LOG.debug("Ignoring jobName {" + jobName + "} as priority doesn't match");
				continue;
			}
			MeLoN_Task[] tasks = jobTasks.get(jobName);
			for (int i = 0; i < tasks.length; i++) {
				LOG.info("***Index = {}.", i);
				if (tasks[i] == null) {
					LOG.info("***Index {} is null.", i);
					tasks[i] = new MeLoN_Task(jobName, String.valueOf(i), sessionId);
					return tasks[i];
				}
			}
		}
		return null;
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
		return jobTasks.entrySet().stream().filter(entry -> Utils.isJobNameTracked(entry.getKey(), melonConf))
				.mapToInt(entry -> entry.getValue().length).sum();
	}

	public int getNumCompletedTasks() {
		return (int) jobTasks.values().stream().flatMap(arr -> Arrays.stream(arr)).filter(task -> task.isCompleted())
				.count();
	}

	public int getNumCompletedTrackedTasks() {
		return (int) jobTasks.entrySet().stream().filter(entry -> Utils.isJobNameTracked(entry.getKey(), melonConf))
				.flatMap(entry -> Arrays.stream(entry.getValue())).filter(task -> task != null && task.isCompleted())
				.count();
	}

	public Map<String, List<String>> getClusterSpec() {
		Map<String, List<String>> map = new HashMap<>();
		for (Map.Entry<String, MeLoN_Task[]> entry : jobTasks.entrySet()) {
			String jobName = entry.getKey();
			MeLoN_Task[] tasks = entry.getValue();

			List<String> builder = new ArrayList<>();
			for (MeLoN_Task task : tasks) {
				if (task == null) {
					continue;
				}

				String hostPort = task.getHostPort();
				builder.add(hostPort);
			}
			map.put(jobName, builder);
		}
		return map;
	}

	public void onTaskCompleted(String jobName, String jobIndex, int exitCode) {
		LOG.info("Job {}:{} finished with exitCode: {}", jobName, jobIndex, exitCode);
		MeLoN_Task task = getTask(jobName, jobIndex);
		Preconditions.checkNotNull(task);
		task.setExitStatus(exitCode);
		if (exitCode != ContainerExitStatus.SUCCESS && exitCode != ContainerExitStatus.KILLED_BY_APPMASTER) {
			trainingFinished = true;
			setTrainingFinalStatus(FinalApplicationStatus.FAILED);
		}
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
			if (!Utils.isJobNameTracked(jobName, melonConf)) {
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

	public void setTrainingFinalStatus(FinalApplicationStatus trainingFinalStatus) {
		this.trainingFinalStatus = trainingFinalStatus;
	}

	public MeLoN_Task getTask(String jobName, String taskIndex) {
		for (Map.Entry<String, MeLoN_Task[]> entry : jobTasks.entrySet()) {
			MeLoN_Task[] tasks = entry.getValue();
			for (MeLoN_Task task : tasks) {
				String type = task.getJobName();
				String index = task.getTaskIndex();
				if (type.equals(jobName) && index.equals(taskIndex)) {
					return task;
				}
			}
		}
		return null;
	}

	public MeLoN_Task getTask(ContainerId containerId) {
		return containerIdMap.get(containerId);
	}

	public void addContainer(ContainerId containerId, MeLoN_Task task) {
		containerIdMap.put(containerId, task);
	}

	public static class Builder {
		private String jvmArgs;
		private Configuration melonConf;

		public MeLoN_Session build() {
			return new MeLoN_Session(this);
		}

		public Builder setTaskExecutorJVMArgs(String jvmArgs) {
			this.jvmArgs = jvmArgs;
			return this;
		}

		public Builder setMelonConf(Configuration melonConf) {
			this.melonConf = melonConf;
			return this;
		}
	}
}
