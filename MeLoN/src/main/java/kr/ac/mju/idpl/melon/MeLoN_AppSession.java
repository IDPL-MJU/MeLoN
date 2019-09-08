package kr.ac.mju.idpl.melon;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
	private String appId;
	private String jvmArgs;
	private Configuration melonConf;
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

	}

}
