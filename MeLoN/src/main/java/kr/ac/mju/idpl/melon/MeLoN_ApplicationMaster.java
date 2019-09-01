package kr.ac.mju.idpl.melon;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.math3.analysis.function.Constant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kr.ac.mju.idpl.melon.util.Utils;

public class MeLoN_ApplicationMaster {
	private static final Logger LOG = LoggerFactory.getLogger(MeLoN_ApplicationMaster.class);
	private Configuration yarnConf;
	private Configuration hdfsConf;

	private AMRMClientAsync<ContainerRequest> amRMClient;
	private NMClientAsync nmClientAsync;
	private NMCallbackHandler containerListener;

	// private ApplicationAttemptId appAttemptID;

	private String appMasterHostname = "";
	private int appMasterHostPort = 0;
	private String appMasterTrackingUrl = "";

	private int numTotalContainers;
	private int containerMemory;
	private int requestPriority;

	private ContainerId containerId;
	private String appIdString;
	private Configuration melonConf = new Configuration(false);
	private String hdfsClasspath;
	private String adminUser;
	private String adminPassword;
	

	private AtomicInteger numCompletedContainers = new AtomicInteger();
	private AtomicInteger numAllocatedContainers = new AtomicInteger();
	private AtomicInteger numFailedContainers = new AtomicInteger();
	private AtomicInteger numRequestedContainers = new AtomicInteger();

	private Map<String, String> shellEnv = new HashMap<>();
	private Map<String, String> containerEnv = new HashMap<>();
	private String melonHome;
	private String appJar;
	private String domainController;

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
		String[] shellEnvs = melonConf.getStrings(MeLoN_ConfigurationKeys.SHELL_ENVS);
		shellEnv = Utils.parseKeyValue(shellEnvs);
		String[] containersEnvs = melonConf.getStrings(MeLoN_ConfigurationKeys.CONTAINER_ENVS);
		containerEnv = Utils.parseKeyValue(containersEnvs);

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
		
		String amHostname = NetUtils.getHostname();
		RegisterApplicationMasterResponse response = amRMClient.registerApplicationMaster(amHostname, -1, "");
		LOG.info("MeLoN_ApplicationMaster is registered with response : {}", response.toString());

		NMCallbackHandler containerListener = new NMCallbackHandler();
		nmClientAsync = new NMClientAsyncImpl(containerListener);
		nmClientAsync.init(yarnConf);
		nmClientAsync.start();
		LOG.info("Starting NMCallbackHandler...");
		
		
		
		
		return true;
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
			float progress = numOfContainers <= 0 ? 0 : (float) numCompletedContainers.get() / numOfContainers;
			return progress;
		}

		@Override
		public void onContainersAllocated(List<Container> arg0) {
			// TODO Auto-generated method stub

		}

		@Override
		public void onContainersCompleted(List<ContainerStatus> arg0) {
			// TODO Auto-generated method stub

		}

		@Override
		public void onError(Throwable arg0) {
			// TODO Auto-generated method stub

		}

		@Override
		public void onNodesUpdated(List<NodeReport> arg0) {
			// TODO Auto-generated method stub

		}

		@Override
		public void onShutdownRequest() {
			// TODO Auto-generated method stub

		}

	}
}
