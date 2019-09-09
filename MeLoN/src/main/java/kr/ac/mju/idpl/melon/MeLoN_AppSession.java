package kr.ac.mju.idpl.melon;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kr.ac.mju.idpl.melon.util.Utils;

public class MeLoN_AppSession {

	private static final Logger LOG = LoggerFactory.getLogger(MeLoN_AppSession.class);
	public int sessionId = 0;
	private String appId;
	private String jvmArgs;
	private Configuration melonConf;
	private Map<ContainerId, MeLoN_Task> containerIdMap = new HashMap<>();
	private long startingTime;
	private long finishingTime;

	private Map<String, MeLoN_ContainerRequest> containerRequests;
	private Map<String, MeLoN_Task[]> jobTasks = new ConcurrentHashMap<>();

	private MeLoN_AppSession(Builder builder) {
		this.containerRequests = Utils.parseContainerRequests(builder.melonConf);
		this.jvmArgs = builder.jvmArgs;
		this.melonConf = builder.melonConf;
		for (Map.Entry<String, MeLoN_ContainerRequest> entry : containerRequests.entrySet()) {
			jobTasks.put(entry.getKey(), new MeLoN_Task[entry.getValue().getNumInstances()]);
		}
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
			String jobName = entry.getKey();
			if (entry.getValue().getPriority() != priority) {
				LOG.debug("Ignoring jobname {" + jobName + "} as priority doesn't match");
				continue;
			}
			MeLoN_Task[] tasks = jobTasks.get(jobName);
			for (int i = 0; i < tasks.length; i++) {
				if (tasks[i] == null) {
					tasks[i] = new MeLoN_Task(jobName, String.valueOf(i), sessionId);
					return tasks[i];
				}
			}
		}
		return null;
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

	public int getTotalTrackedTasks() {
		return jobTasks.entrySet().stream().filter(entry -> Utils.isJobTypeTracked(entry.getKey(), melonConf))
				.mapToInt(entry -> entry.getValue().length).sum();
	}

	public static class Builder {
		private String jvmArgs;
		private Configuration melonConf;

		public MeLoN_AppSession build() {
			return new MeLoN_AppSession(this);
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

	public class MeLoN_Task {

		private String jobName;
		private String taskIndex;
		private String taskUrl;
		private int sessionId;
		private Container container;
		private MeLoN_TaskStatus status;

		public MeLoN_Task(String jobName, String taskIndex, int sessionId) {
			this.jobName = jobName;
			this.taskIndex = taskIndex;
			this.sessionId = sessionId;
		}

		public String getJobName() {
			return jobName;
		}

		public String getTaskIndex() {
			return taskIndex;
		}

		public int getSessionId() {
			return sessionId;
		}

		public Container getContainer() {
			return container;
		}

		public void setContainer(Container container) {
			this.container = container;
		}

		public void addContainer(Container container) {
			setContainer(container);
			containerIdMap.put(container.getId(), this);
		}

		public String getId() {
			return this.jobName + ":" + this.taskIndex;
		}

		public void setStatus(MeLoN_TaskStatus status) {
			this.status = status;
		}

	}

}
