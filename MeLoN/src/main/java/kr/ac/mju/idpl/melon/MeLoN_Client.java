package kr.ac.mju.idpl.melon;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.math3.analysis.function.Constant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

import kr.ac.mju.idpl.melon.MeLoN_Constants;
import kr.ac.mju.idpl.melon.util.Utils;
import kr.ac.mju.idpl.melon.MeLoN_ApplicationMaster;
import kr.ac.mju.idpl.melon.MeLoN_Client;

public class MeLoN_Client {
	private static final Logger LOG = LoggerFactory.getLogger(MeLoN_Client.class);
	private YarnConfiguration yarnConf;
	private YarnClient yarnClient;
	private ApplicationId appId;
	private FileSystem fs;

	private Configuration melonConf;
	private Configuration hdfsConf;

	private String appName;
	private Options opts;
	private String appJar = "";

	private String hdfsConfAddress = null;
	private String yarnConfAddress = null;
	private Map<String, String> shellEnvs = new HashMap<>();
	private Map<String, String> containerEnvs = new HashMap<>();
	private Path appResourcesPath;

	private final String melonAMClass = MeLoN_ApplicationMaster.class.getName();
	private int amPriority;
	private String amQueue = "";
	private int amVCores;
	private long amMemory;
	private int amGpus;

	private String taskParams;
	private String pythonBinaryPath;
	private String pythonVenv;
	private String executes;

	private int containerMemory;
	private int numContainers;

	private String adminId;
	private String adminPwd;
	private String appUri;

	public MeLoN_Client() {
		yarnConf = new YarnConfiguration();
		hdfsConf = new Configuration();
		melonConf = new Configuration(false);
		yarnClient = YarnClient.createYarnClient();
		initOptions();
	}

	private void initHdfsConf() {
		if (hdfsConfAddress != null) {
			hdfsConf.addResource(new Path(hdfsConfAddress));
		} else if (System.getenv(MeLoN_Constants.HADOOP_CONF_DIR) != null) {
			hdfsConf.addResource(new Path(System.getenv(MeLoN_Constants.HADOOP_CONF_DIR) + File.separatorChar
					+ MeLoN_Constants.CORE_SITE_CONF));
			hdfsConf.addResource(new Path(System.getenv(MeLoN_Constants.HADOOP_CONF_DIR) + File.separatorChar
					+ MeLoN_Constants.HDFS_SITE_CONF));
		}
	}

	private void initYarnConf() {
		if (this.yarnConfAddress != null) {
			this.yarnConf.addResource(new Path(this.yarnConfAddress));
		} else if (System.getenv(MeLoN_Constants.HADOOP_CONF_DIR) != null) {
			yarnConf.addResource(new Path(System.getenv(MeLoN_Constants.HADOOP_CONF_DIR) + File.separatorChar
					+ MeLoN_Constants.CORE_SITE_CONF));
			yarnConf.addResource(new Path(System.getenv(MeLoN_Constants.HADOOP_CONF_DIR) + File.separatorChar
					+ MeLoN_Constants.YARN_SITE_CONF));
		}
	}

	private void initOptions() {
		opts = new Options();
		// opts.addOption("appName", true, "Application Name. Default value - MeLoN");
		// opts.addOption("priority", true, "Application Priority. Default value - 0");
		// opts.addOption("hdfs_classpath", true, "Path to jars on HDFS for workers.");
		opts.addOption("python_bin_path", true, "The relative path to python binary.");
		opts.addOption("python_venv", true, "The python virtual environment zip.");
		opts.addOption("jar", true, "JAR file containing the application master. Default - ./MeLoN.jar");
		opts.addOption("task_params", true, "The task params to pass into python entry point.");
		opts.addOption("executes", true, "The file to execute on containers.");
		opts.addOption("task_params", true, "The task params to pass into python entry point.");
		opts.addOption("shell_env", true, "Environment for shell script, specified as env_key=env_val pairs");
		opts.addOption("conf", true, "User specified configuration, as key=val pairs");
		opts.addOption("conf_file", true, "Name of user specified conf file, on the classpath");
		opts.addOption("src_dir", true, "Name of directory of source files.");
		opts.addOption("help", false, "Print usage.");
	}

	private void initMelonConf(CommandLine cliParser) throws IOException {
		melonConf.addResource("melon-default.xml");
		if (cliParser.hasOption("conf_file")) {
			Path confFilePath = new Path(cliParser.getOptionValue("conf_file"));

			if (confFilePath.toUri().getScheme() == null) {
				melonConf.addResource(confFilePath);
			} else {
				melonConf.addResource(confFilePath.getFileSystem(yarnConf).open(confFilePath));
			}
		} else {
			melonConf.addResource("melon.xml");
		}

		if (cliParser.hasOption("conf")) {
			String[] cliConfs = cliParser.getOptionValues("conf");
			for (Map.Entry<String, String> cliConf : Utils.parseKeyValue(cliConfs).entrySet()) {
				String[] alreadySetConf = melonConf.getStrings(cliConf.getKey());
				if (alreadySetConf != null && MeLoN_ConfigurationKeys.MULTI_VALUE_CONF.contains(cliConf.getKey())) {
					ArrayList<String> newValues = new ArrayList<>(Arrays.asList(alreadySetConf));
					newValues.add(cliConf.getValue());
					melonConf.setStrings(cliConf.getKey(), newValues.toArray(new String[0]));
				} else {
					melonConf.set(cliConf.getKey(), cliConf.getValue());
				}
			}
		}
	}

	public boolean init(String[] args) throws ParseException, IOException {
		LOG.info("Starting init...");
		CommandLine cliParser = new GnuParser().parse(opts, args);
		if (args.length == 0) {
			throw new IllegalArgumentException("No args specified for client to initialize.");
		}
		if (cliParser.hasOption("help")) {
			printUsage();
			return false;
		}
		initMelonConf(cliParser);
		hdfsConfAddress = melonConf.get(MeLoN_ConfigurationKeys.HDFS_CONF_PATH);
		yarnConfAddress = melonConf.get(MeLoN_ConfigurationKeys.YARN_CONF_PATH);
		initHdfsConf();
		initYarnConf();
		yarnClient.init(yarnConf);

		String amMemoryString = melonConf.get(MeLoN_ConfigurationKeys.AM_MEMORY,
				MeLoN_ConfigurationKeys.AM_MEMORY_DEFAULT);
		amMemory = Integer.parseInt(Utils.parseMemoryString(amMemoryString));
		amVCores = melonConf.getInt(MeLoN_ConfigurationKeys.AM_VCORES, MeLoN_ConfigurationKeys.AM_VCORES_DEFAULT);
		amGpus = melonConf.getInt(MeLoN_ConfigurationKeys.AM_GPUS, MeLoN_ConfigurationKeys.AM_GPUS_DEFAULT);

		pythonBinaryPath = cliParser.getOptionValue("python_binary_path");
		pythonVenv = cliParser.getOptionValue("python_venv");
		executes = buildTaskCommand(pythonBinaryPath, pythonVenv, cliParser.getOptionValue("executes"), taskParams);
		taskParams = cliParser.getOptionValue("task_params");
		melonConf.set(MeLoN_ConfigurationKeys.CONTAINERS_COMMAND, executes);
	    
		if (amMemory < 0) {
			throw new IllegalArgumentException(
					"Invalid memory specified for application master exiting." + "Specified Memory =" + amMemory);
		}
		if (amVCores < 0) {
			throw new IllegalArgumentException("Invalid virtual cores specified for application master, exiting."
					+ " Specified virtual cores=" + amVCores);
		}
		if (Utils.getNumTotalTasks(melonConf) == 0 && amGpus > 0) {
			LOG.warn("It seems you reserved " + amGpus + " GPUs in application master (driver, which doesn't perform "
					+ "training) during distributed training.");
		}

		if (melonConf.get(MeLoN_ConfigurationKeys.SHELL_ENVS) != null) {
			String[] envs = melonConf.getStrings(MeLoN_ConfigurationKeys.SHELL_ENVS);
			shellEnvs.putAll(Utils.parseKeyValue(envs));
		}
		if (cliParser.hasOption("shell_envs")) {
			String[] envs = cliParser.getOptionValues("shell_envs");
			shellEnvs.putAll(Utils.parseKeyValue(envs));
		}
		if (!shellEnvs.isEmpty()) {
			melonConf.setStrings(MeLoN_ConfigurationKeys.SHELL_ENVS, shellEnvs.entrySet().toArray(new String[0]));
		}

		if (melonConf.get(MeLoN_ConfigurationKeys.CONTAINER_ENVS) != null) {
			String[] envs = melonConf.getStrings(MeLoN_ConfigurationKeys.CONTAINER_ENVS);
			containerEnvs.putAll(Utils.parseKeyValue(envs));
		}
		if (cliParser.hasOption("container_envs")) {
			String[] envs = cliParser.getOptionValues("container_envs");
			containerEnvs.putAll(Utils.parseKeyValue(envs));
		}
		if (!containerEnvs.isEmpty()) {
			melonConf.setStrings(MeLoN_ConfigurationKeys.CONTAINER_ENVS,
					containerEnvs.entrySet().toArray(new String[0]));
		}

		if (!cliParser.hasOption("jar")) {
			throw new IllegalArgumentException("No jar file specified for application master");
		}
		appJar = cliParser.getOptionValue("jar");
		
		return true;
	}

	public String buildTaskCommand(String pythonVenv, String pythonBinaryPath, String executes, String taskParams) {
		if (executes != null) {
			String containerCmd = executes;
			String pythonInterpreter;
			if (pythonBinaryPath != null) {
				if (pythonBinaryPath.startsWith("/") || pythonVenv == null) {
					pythonInterpreter = pythonBinaryPath;
				} else {
					pythonInterpreter = MeLoN_Constants.PYTHON_VENV_DIR + File.separatorChar + pythonBinaryPath;
				}
				containerCmd = pythonInterpreter + " " + executes; 
			}
			if (taskParams != null) {
				containerCmd += "" + taskParams;
			}
			return containerCmd;
		} else {
			return null;
		}
	}
	private void addToLocalResources(FileSystem fs, String srcPath, String dstPath, LocalResourceType resourceType, Map<String, LocalResource> localResources) throws IOException {
		Path src = new Path(srcPath);
		Path dst = new Path(appResourcesPath, dstPath);
		fs.copyFromLocalFile(false, true, src, dst);
		FileStatus dstFileStatus = fs.getFileStatus(dst);
	    fs.setPermission(dst, new FsPermission((short) 0770));
	    
	    LocalResource lr = LocalResource.newInstance(
	    		ConverterUtils.getYarnUrlFromURI(dst.toUri()), 	// setResource
	    		resourceType, 									// setType
	    		LocalResourceVisibility.APPLICATION, 			// setVisibility
	    		dstFileStatus.getLen(), 						// setSize
	    		dstFileStatus.getModificationTime());			//setTimestamp
	    
		localResources.put(dstPath, lr);
	}
	private void setCapabilityGPU(Resource capability, int gpuCount) {
		if(gpuCount > 0) {
			Method method;
			try {
				method = capability.getClass().getMethod("setResourceValue", String.class, long.class);
				method.invoke(capability, "yarn.io/gpu", gpuCount);
			} catch (NoSuchMethodException e) {
				// TODO Auto-generated catch block
				LOG.error("There is no 'setResourceValue' API in this version of YARN");
				throw new RuntimeException(e);
			} catch (IllegalAccessException | InvocationTargetException e) {
				// TODO Auto-generated catch block
				LOG.error("Failed to invoke 'setResourceValue' method to set GPU resources", e);
				throw new RuntimeException(e);
			}
		}
	}

	public int run() throws IOException, YarnException {
		LOG.info("Starting YarnClient...");
		yarnClient.start();
		YarnClientApplication app = yarnClient.createApplication();
		GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

		long maxMem = appResponse.getMaximumResourceCapability().getMemorySize();
		if (amMemory > maxMem) {
			LOG.warn("Truncating requested AM memory: " + amMemory + " to cluster's max: " + maxMem);
			amMemory = maxMem;
		}

		int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
		if (amVCores > maxVCores) {
			LOG.warn("Truncating requested AM vcores: " + amVCores + " to cluster's max: " + maxVCores);
			amVCores = maxVCores;
		}

		fs = FileSystem.get(hdfsConf);
		
		ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
		appId = appContext.getApplicationId();
		appResourcesPath = new Path(fs.getHomeDirectory(), appName + File.separator + appId.toString());
		appContext.setApplicationName(appName);
		
		Resource capability = Resource.newInstance((int) amMemory, amVCores);
		setCapabilityGPU(capability, amGpus);
		appContext.setResource(capability);

		appContext.setQueue(amQueue);
		
		ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
		Map<String, LocalResource> localResources = new HashMap<>();
		addToLocalResources(fs, appJar, "appJar", LocalResourceType.FILE, localResources);
		amContainer.setLocalResources(localResources);

		Map<String, String> env = new HashMap<>();

		StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
				.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");

		for (String c : yarnConf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
				YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
			classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
			classPathEnv.append(c.trim());
		}
		env.put("CLASSPATH", classPathEnv.toString());
		amContainer.setEnvironment(env);

		List<String> vargs = new ArrayList<String>(30);
		vargs.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
		vargs.add("-Xmx" + (int) amMemory + "m");
		vargs.add(melonAMClass);
		vargs.add("--jar" + appUri);
		vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + File.separatorChar
				+ "/MeLoN_ApplicationMaster.stdout");
		vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + File.separatorChar
				+ "/MeLoN_ApplicationMaster.stderr");
		String command = String.join(" ", vargs);
		List<String> commands = new ArrayList<String>();
		commands.add(command);
		LOG.info("Completed setting up Application Master command " + command);
		amContainer.setCommands(commands);

		appContext.setAMContainerSpec(amContainer);
		yarnClient.submitApplication(appContext);
		// ApplicationReport report = yarnClient.getApplicationReport(appId);

		return monitorApplication();
	}

	private int monitorApplication() throws YarnException, IOException {
		while (true) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {

			}

			ApplicationReport report = yarnClient.getApplicationReport(appId);

			LOG.info("Got applicaion report from ASM for appId=" + appId.getId() + ", clientToAMToken="
					+ report.getClientToAMToken() + ", appDiagnotics=" + report.getDiagnostics() + ", appMasterHost="
					+ report.getHost() + ", appQueue=" + report.getQueue() + ", appMasterRpcPort=" + report.getRpcPort()
					+ ", appStartTime=" + report.getStartTime() + ", yarnAppState="
					+ report.getYarnApplicationState().toString() + ", distributedFinalState="
					+ report.getFinalApplicationStatus() + ", appTrackingUrl=" + report.getTrackingUrl() + ", appUser= "
					+ report.getUser());

			YarnApplicationState appState = report.getYarnApplicationState();
			FinalApplicationStatus finalApplicationStatus = report.getFinalApplicationStatus();
			if (YarnApplicationState.FINISHED == appState) {
				if (FinalApplicationStatus.SUCCEEDED == finalApplicationStatus) {
					LOG.info("Application has completed successfully. Breaking monitoring loop.");
					return 0;
				} else {
					LOG.info("Application did finished unsuccessfully." + "YarnState=" + appState.toString()
							+ ", FinalStatus=" + finalApplicationStatus.toString() + ". Breaking monitoring loop.");
					return -1;
				}
			} else if (YarnApplicationState.KILLED == appState || YarnApplicationState.FAILED == appState) {
				LOG.info("Application did not finish." + "YarnState=" + appState.toString() + "FinalStatus="
						+ finalApplicationStatus.toString() + ". Breaking monitoring loop");
				return -1;
			}
		}
	}

	public void printUsage() {
		new HelpFormatter().printHelp("MeLoN_Client", opts);
	}

	public static void main(String[] args) {
		int exitCode = 0;
		try {
			MeLoN_Client client = new MeLoN_Client();
			try {
				boolean doInit = client.init(args);
				if (!doInit) {
					LOG.error("Failed to init MeLoN_Client.");
					exitCode = -1;
				}
			} catch (IllegalArgumentException e) {
				client.printUsage();
				exitCode = -1;
			}
			exitCode = client.run();
		} catch (Throwable t) {
			LOG.error("Failed to finish MeLoN_Client seccessfully.");
			exitCode = -1;
		}
		if(exitCode == 0) {
			LOG.info("Application submitted successfully.");
		}
		System.exit(exitCode);

	}

}
