package kr.ac.mju.idpl.melon;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.FileVisitResult;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
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

import kr.ac.mju.idpl.melon.MeLoN_Constants;
import kr.ac.mju.idpl.melon.util.Utils;
import kr.ac.mju.idpl.melon.MeLoN_ApplicationMaster;
import kr.ac.mju.idpl.melon.MeLoN_Client;

public class MeLoN_Client {
	private static final Logger LOG = LoggerFactory.getLogger(MeLoN_Client.class);

	// Configurations
	private YarnClient yarnClient;
	private YarnConfiguration yarnConf;
	private Configuration hdfsConf;
	private Options opts;

	// Containers setup
	private String hdfsConfAddress = null;
	private String yarnConfAddress = null;
	private String appName;
	private ApplicationId appId;
	private long amMemory;
	private int amVCores;
	private int amGpus;
	private String taskParams = null;
	private String pythonBinaryPath = null;
	private String pythonVenv = null;
	private String executes = null;
	private String srcDir = null;
	private String melonJarPath = null;
	private Configuration melonConf;
	private String melonFinalConfPath;
	private Map<String, String> shellEnvs = new HashMap<>();
	private Map<String, String> containerEnvs = new HashMap<>();
	private Path appResourcesPath;

	private FileSystem fs;

	private final String melonAMClass = MeLoN_ApplicationMaster.class.getName();
	// private int amPriority;
	private String amQueue = "";

	private String appUri;

	public MeLoN_Client() {
		initOptions();
		yarnConf = new YarnConfiguration();
		hdfsConf = new Configuration();
		melonConf = new Configuration(false);
		yarnClient = YarnClient.createYarnClient();
	}

	private void initHdfsConf() {
		LOG.info("Initializing HDFS configurations...");
		if (System.getenv(MeLoN_Constants.HADOOP_CONF_DIR) != null) {
			hdfsConf.addResource(new Path(System.getenv(MeLoN_Constants.HADOOP_CONF_DIR) + File.separatorChar
					+ MeLoN_Constants.CORE_SITE_CONF));
			hdfsConf.addResource(new Path(System.getenv(MeLoN_Constants.HADOOP_CONF_DIR) + File.separatorChar
					+ MeLoN_Constants.HDFS_SITE_CONF));
		}
		if (hdfsConfAddress != null) {
			hdfsConf.addResource(new Path(hdfsConfAddress));
		}
		LOG.info("Finished initializing HDFS configurations...");
	}

	private void initYarnConf() {
		LOG.info("Initializing YARN configurations...");
		if (System.getenv(MeLoN_Constants.HADOOP_CONF_DIR) != null) {
			yarnConf.addResource(new Path(System.getenv(MeLoN_Constants.HADOOP_CONF_DIR) + File.separatorChar
					+ MeLoN_Constants.CORE_SITE_CONF));
			yarnConf.addResource(new Path(System.getenv(MeLoN_Constants.HADOOP_CONF_DIR) + File.separatorChar
					+ MeLoN_Constants.YARN_SITE_CONF));
		}
		if (yarnConfAddress != null) {
			yarnConf.addResource(new Path(this.yarnConfAddress));
		} 
		LOG.info("Finished initializing YARN configurations...");
	}

	private void initOptions() {
		opts = new Options();
		// opts.addOption("appName", true, "Application Name. Default value - melon");
		// opts.addOption("priority", true, "Application Priority. Default value - 0");
		// opts.addOption("hdfs_classpath", true, "Path to jars on HDFS for workers.");
		opts.addOption("python_venv", true, "The python virtual environment zip. Default : venv.zip");
		opts.addOption("python_bin_path", true, "The relative path to python binary. Default : Python/bin/python");
		opts.addOption("executes", true, "The file to execute on containers.");
		opts.addOption("task_params", true, "The task params to pass into python entry point.");
		opts.addOption("shell_env", true, "Environment for shell script, specified as env_key=env_val pairs");
		opts.addOption("conf", true, "User specified configuration, as key=val pairs");
		opts.addOption("conf_file", true, "Name of user specified conf file, on the classpath. Default : melon.xml");
		opts.addOption("src_dir", true, "Name of directory of source files. Default : src");
		opts.addOption("jar", true, "JAR file containing the application master. Default : melon.jar");
		opts.addOption("help", false, "Print usage.");
	}

	private void initMelonConf(CommandLine cliParser) throws IOException {
		LOG.info("Starting init melon configurations");
		LOG.info("Initializing from a default configuration file. 'melon-default.xml'");
		this.melonConf.addResource(new Path("melon-default.xml"));
		if (cliParser.hasOption("conf_file")) {
			// assume local file only
			Path confFilePath = new Path(cliParser.getOptionValue("conf_file"));
			LOG.info("Adding " + confFilePath + " to melon configurations.");
			melonConf.addResource(confFilePath);
		} else {
			LOG.info("Adding " + "melon.xml" + " to melon configurations.");
			melonConf.addResource(new Path("melon.xml"));
		}

		if (cliParser.hasOption("conf")) {
			LOG.info("Adding a 'conf' option value(KeyValuePair) to melon configurations.");
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
		LOG.info("Finished initializing melon configurations");
	}

	public boolean init(String[] args) throws ParseException, IOException {
		LOG.info("Starting init MeLoN_Clinet...");
		CommandLine cliParser = new GnuParser().parse(opts, args);
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
		pythonBinaryPath = cliParser.getOptionValue("python_bin_path", "Python/bin/python");
		pythonVenv = cliParser.getOptionValue("python_venv", "venv.zip");
		taskParams = cliParser.getOptionValue("task_params");
		executes = buildTaskCommand(pythonVenv, pythonBinaryPath, cliParser.getOptionValue("executes"), taskParams);
		melonConf.set(MeLoN_ConfigurationKeys.CONTAINERS_COMMAND, executes);

		srcDir = cliParser.getOptionValue("src_dir", "src");
		melonJarPath = cliParser.getOptionValue("jar", "melon.jar");

		if (amMemory < 0) {
			throw new IllegalArgumentException(
					"Invalid memory specified for application master exiting." + "Specified Memory =" + amMemory);
		}
		if (amVCores < 0) {
			throw new IllegalArgumentException("Invalid virtual cores specified for application master, exiting."
					+ " Specified virtual cores=" + amVCores);
		}
		if (Utils.getNumTotalTasks(melonConf) == 0 && amGpus > 0) {
			LOG.warn("It seems you reserved " + amGpus
					+ " GPUs in application master (driver, which doesn't perform training) during distributed training.");
		}

		List<String> shellEnvsPair = new ArrayList<>();
		if (melonConf.get(MeLoN_ConfigurationKeys.SHELL_ENVS) != null) {
			String[] envs = melonConf.getStrings(MeLoN_ConfigurationKeys.SHELL_ENVS);
			shellEnvsPair.addAll(Arrays.asList(envs));
			shellEnvs.putAll(Utils.parseKeyValue(envs));
		}
		if (cliParser.hasOption("shell_envs")) {
			String[] envs = cliParser.getOptionValues("shell_envs");
			shellEnvsPair.addAll(Arrays.asList(envs));
			shellEnvs.putAll(Utils.parseKeyValue(envs));
		}
		if (!shellEnvs.isEmpty()) {
			melonConf.setStrings(MeLoN_ConfigurationKeys.SHELL_ENVS, shellEnvsPair.toArray(new String[0]));
		}

		List<String> containerEnvsPair = new ArrayList<>();
		if (melonConf.get(MeLoN_ConfigurationKeys.CONTAINER_ENVS) != null) {
			String[] envs = melonConf.getStrings(MeLoN_ConfigurationKeys.CONTAINER_ENVS);
			containerEnvsPair.addAll(Arrays.asList(envs));
			containerEnvs.putAll(Utils.parseKeyValue(envs));
		}
		if (cliParser.hasOption("container_envs")) {
			String[] envs = cliParser.getOptionValues("container_envs");
			containerEnvsPair.addAll(Arrays.asList(envs));
			containerEnvs.putAll(Utils.parseKeyValue(envs));
		}
		if (!containerEnvs.isEmpty()) {
			melonConf.setStrings(MeLoN_ConfigurationKeys.CONTAINER_ENVS, containerEnvsPair.toArray(new String[0]));
		}
		return true;
	}

	public String buildTaskCommand(String pythonVenv, String pythonBinaryPath, String executes, String taskParams) {
		LOG.info("Building a container task command.");
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
			LOG.info("The container task command was builded. [ " + containerCmd + " ]");
			return containerCmd;
		} else {
			LOG.info("The container task command wasn't builded. (There is no option value 'executes' in the command line.");
			return null;
		}
	}

	private void addToLocalResource(String srcPath, FileSystem fs, String dstPath, LocalResourceType resourceType,
			Map<String, LocalResource> localResources) throws IOException {
		Path src = new Path(srcPath);
		Path dst = new Path(appResourcesPath, dstPath);
		fs.copyFromLocalFile(false, true, src, dst);
		FileStatus dstFileStatus = fs.getFileStatus(dst);
		fs.setPermission(dst, new FsPermission((short) 0770));

		LocalResource lr = LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(dst.toUri()), // setResource
				resourceType, // setType
				LocalResourceVisibility.APPLICATION, // setVisibility
				dstFileStatus.getLen(), // setSize
				dstFileStatus.getModificationTime()); // setTimestamp

		localResources.put(dstPath, lr);
	}

	private void uploadFileAndSetConfResources(Path src, FileSystem fs, Path hdfsPath, String fileName,
			LocalResourceType resourceType, String resourcecKey, Configuration melonConf) throws IOException {
		LOG.info("Uploading resource files and Updating the resource list in melon configurations.");
		Path dst = new Path(hdfsPath, fileName);
		fs.copyFromLocalFile(false, true, src, dst);
		fs.setPermission(dst, new FsPermission((short) 0770));
		String dstAddress = dst.toString();
		if (resourceType == LocalResourceType.ARCHIVE) {
			dstAddress += MeLoN_Constants.ARCHIVE_SUFFIX;
		}
		if (dstAddress != null) {
			String[] resources = melonConf.getStrings(resourcecKey);
			List<String> updatedResources = new ArrayList<>();
			if (resources != null) {
				updatedResources = new ArrayList<>(Arrays.asList(resources));
			}
			updatedResources.add(dstAddress);
			melonConf.setStrings(resourcecKey, updatedResources.toArray(new String[0]));
		}
	}

	public int run() throws IOException, YarnException, ParseException {
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
		appName = melonConf.get(MeLoN_ConfigurationKeys.APPLICATION_NAME,
				MeLoN_ConfigurationKeys.APPLICATION_NAME_DEFAULT);
		appContext.setApplicationName(appName);
		appId = appContext.getApplicationId();
		
		amQueue = melonConf.get(MeLoN_ConfigurationKeys.YARN_QUEUE_NAME, MeLoN_ConfigurationKeys.YARN_QUEUE_NAME_DEFAULT);
		appContext.setQueue(amQueue);
		
		appResourcesPath = new Path(fs.getHomeDirectory(), appName + File.separator + appId.toString());

		melonFinalConfPath = processMelonFinalConf();

		Resource capability = Resource.newInstance((int) amMemory, amVCores);
		Utils.setCapabilityGPU(capability, amGpus);
		appContext.setResource(capability);

		appContext.setQueue(amQueue);

		ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
		Map<String, LocalResource> localResources = new HashMap<>();
		addToLocalResource(melonFinalConfPath, fs, MeLoN_Constants.MELON_FINAL_XML, LocalResourceType.FILE,
				localResources);
		addToLocalResource(melonJarPath, fs, MeLoN_Constants.MELON_JAR, LocalResourceType.FILE, localResources);

		String[] amResources = melonConf.getStrings(MeLoN_ConfigurationKeys.getResourcesKey(MeLoN_Constants.AM_NAME));
		Utils.addResources(amResources, localResources, fs);
		amResources = melonConf.getStrings(MeLoN_ConfigurationKeys.CONTAINER_RESOURCES);
		Utils.addResources(amResources, localResources, fs);
		amContainer.setLocalResources(localResources);

		setAMEnvironment(localResources, fs);
		amContainer.setEnvironment(containerEnvs);
		amContainer.setCommands(buildAMCommand());

		appContext.setAMContainerSpec(amContainer);
		LOG.info("*********am.resources : " + localResources.toString());
		LOG.info("*********am.resources : " + localResources);

		LOG.info("Submitting YARN application" + "["+ appId + "]");
		yarnClient.submitApplication(appContext);
		LOG.info("***melonFinalConf : " + melonConf.getValByRegex("melon\\.([a-z]+)\\.([a-z]+)"));
		// ApplicationReport report = yarnClient.getApplicationReport(appId);
		//return monitorApplication();
		return 0;
	}

	public List<String> buildAMCommand() {
		List<String> vargs = new ArrayList<String>(30);
		vargs.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
		vargs.add("-Xmx" + (int) amMemory + "m");
		vargs.add(melonAMClass);
		vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + File.separatorChar
				+ "/MeLoN_ApplicationMaster.stdout");
		vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + File.separatorChar
				+ "/MeLoN_ApplicationMaster.stderr");
		String command = String.join(" ", vargs);
		List<String> commands = new ArrayList<String>();
		commands.add(command);
		LOG.info("Completed setting up Application Master command " + command);
		return commands;
	}

	private void setAMEnvironment(Map<String, LocalResource> localResources, FileSystem fs) throws IOException {
		LocalResource mFinalConfResource = localResources.get(MeLoN_Constants.MELON_FINAL_XML);
		Path mFinalConfPath = new Path(fs.getHomeDirectory(), mFinalConfResource.getResource().getFile());
		FileStatus mFinalConfStatus = fs.getFileStatus(mFinalConfPath);
		long mFinalConfLength = mFinalConfStatus.getLen();
		long mFinalConfTimestamp = mFinalConfStatus.getModificationTime();
		containerEnvs.put(MeLoN_Constants.MELON_CONF_PREFIX + MeLoN_Constants.PATH_SUFFIX, mFinalConfPath.toString());
		containerEnvs.put(MeLoN_Constants.MELON_CONF_PREFIX + MeLoN_Constants.LENGTH_SUFFIX, Long.toString(mFinalConfLength));
		containerEnvs.put(MeLoN_Constants.MELON_CONF_PREFIX + MeLoN_Constants.TIMESTAMP_SUFFIX,
				Long.toString(mFinalConfTimestamp));
		LocalResource mJarResource = localResources.get(MeLoN_Constants.MELON_JAR);
		Path mJarPath = new Path(fs.getHomeDirectory(), mJarResource.getResource().getFile());
		FileStatus mJarStatus = fs.getFileStatus(mJarPath);
		long mJarLength = mJarStatus.getLen();
		long mJarTimestamp = mJarStatus.getModificationTime();
		containerEnvs.put(MeLoN_Constants.MELON_JAR_PREFIX + MeLoN_Constants.PATH_SUFFIX, mJarPath.toString());
		containerEnvs.put(MeLoN_Constants.MELON_JAR_PREFIX + MeLoN_Constants.LENGTH_SUFFIX, Long.toString(mJarLength));
		containerEnvs.put(MeLoN_Constants.MELON_JAR_PREFIX + MeLoN_Constants.TIMESTAMP_SUFFIX,
				Long.toString(mJarTimestamp));

		// Setting all required classpaths including the classpath to "." for the app jar
		StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$())
				.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
		for (String c : yarnConf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
				YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
			classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
			classPathEnv.append(c.trim());
		}
		containerEnvs.put("CLASSPATH", classPathEnv.toString());
	}

	private String processMelonFinalConf() throws IOException, ParseException {
		FileSystem fs = FileSystem.get(hdfsConf);
		if (srcDir != null) {
			LOG.info("Uploading src directory...");
			if (Utils.isArchive(srcDir)) {
				uploadFileAndSetConfResources(new Path(srcDir), fs, appResourcesPath,
						MeLoN_Constants.MELON_SRC_ZIP_NAME, LocalResourceType.FILE,
						MeLoN_ConfigurationKeys.CONTAINER_RESOURCES, melonConf);
			} else {
				LOG.info("Zipping the src directory to upload ...");
				zipFolder(Paths.get(srcDir), Paths.get(MeLoN_Constants.MELON_SRC_ZIP_NAME));
				uploadFileAndSetConfResources(new Path(MeLoN_Constants.MELON_SRC_ZIP_NAME), fs, appResourcesPath,
						MeLoN_Constants.MELON_SRC_ZIP_NAME, LocalResourceType.FILE,
						MeLoN_ConfigurationKeys.CONTAINER_RESOURCES, melonConf);
			}
		}
		if (pythonVenv != null) {
			LOG.info("Uploading a python venv zip file ...");
			uploadFileAndSetConfResources(new Path(pythonVenv), fs, appResourcesPath, MeLoN_Constants.PYTHON_VENV_ZIP,
					LocalResourceType.FILE, MeLoN_ConfigurationKeys.CONTAINER_RESOURCES, melonConf);
		}
		URL coreSiteUrl = yarnConf.getResource(MeLoN_Constants.CORE_SITE_CONF);
		if (coreSiteUrl != null) {
			uploadFileAndSetConfResources(new Path(coreSiteUrl.getPath()), fs, appResourcesPath,
					MeLoN_Constants.CORE_SITE_CONF, LocalResourceType.FILE, MeLoN_ConfigurationKeys.CONTAINER_RESOURCES,
					melonConf);
		}
		addConfToResources(yarnConf, yarnConfAddress, MeLoN_Constants.YARN_SITE_CONF);
		addConfToResources(hdfsConf, hdfsConfAddress, MeLoN_Constants.HDFS_SITE_CONF);

		processConfResources(melonConf, fs);

		String melonFinalConf = appId.toString() + "-" + MeLoN_Constants.MELON_FINAL_XML;
		OutputStream os = new FileOutputStream(melonFinalConf);
		melonConf.writeXml(os);

		return melonFinalConf;
	}

	private void processConfResources(Configuration melonConf, FileSystem fs) throws IOException, ParseException {
		LOG.info("Processing resource configurations ...");
		Set<String> resourceKeys = melonConf.getValByRegex(MeLoN_ConfigurationKeys.RESOURCES_REGEX).keySet();
		for (String resourceKey : resourceKeys) {
			String[] resources = melonConf.getStrings(resourceKey);
			if (resources == null) {
				continue;
			}
			for (String resource : resources) {
				LocalizableResource lr = new LocalizableResource(resource, fs);
				// If it is local file, we upload to remote fs first
				if (lr.isLocalFile()) {
					Path rsrcPath = lr.getRsrcPath();
					File file = new File(rsrcPath.toString());
					if (!file.exists()) {
						LOG.error(resource + " doesn't exist in local filesystem");
						throw new IOException(resource + " doesn't exist in local filesystem.");
					}
					if (file.isFile()) {
						// If it is archive format, set it as ARCHIVE format.
						if (lr.isArchive()) {
							uploadFileAndSetConfResources(rsrcPath, fs, appResourcesPath, lr.getLocalizedFileName(),
									LocalResourceType.ARCHIVE, MeLoN_ConfigurationKeys.CONTAINER_RESOURCES, melonConf);
						} else {
							uploadFileAndSetConfResources(rsrcPath, fs, appResourcesPath, lr.getLocalizedFileName(),
									LocalResourceType.FILE, MeLoN_ConfigurationKeys.CONTAINER_RESOURCES, melonConf);
						}
					} else {
						// file is directory
						File tmpDir = com.google.common.io.Files.createTempDir();
						tmpDir.deleteOnExit();
						try {
							java.nio.file.Path dest = Paths.get(tmpDir.getAbsolutePath(), file.getName());
							zipFolder(Paths.get(resource), dest);
							uploadFileAndSetConfResources(new Path(dest.toString()), fs, appResourcesPath,
									lr.getLocalizedFileName(), LocalResourceType.ARCHIVE,
									MeLoN_ConfigurationKeys.CONTAINER_RESOURCES, melonConf);
						} finally {
							try {
								FileUtils.deleteDirectory(tmpDir);
							} catch (IOException e) {
								// ignore the deletion failure and continue
								LOG.warn("Failed to delete temp directory " + tmpDir, e);
							}
						}
					}
				}
			}
			// Filter out original local file locations
			resources = melonConf.getStrings(resourceKey);
			resources = Stream.of(resources).filter((filePath) -> new Path(filePath).toUri().getScheme() != null)
					.toArray(String[]::new);
			melonConf.setStrings(resourceKey, resources);
		}

	}

	private void addConfToResources(Configuration conf, String confAddress, String confFileName) throws IOException {
		Path confSitePath = null;
		if (confAddress != null) {
			confSitePath = new Path(confAddress);
		} else {
			URL confSiteUrl = conf.getResource(confFileName);
			if (confSiteUrl != null) {
				confSitePath = new Path(confSiteUrl.getPath());
			}
		}
		if (confSitePath != null) {
			uploadFileAndSetConfResources(confSitePath, FileSystem.get(hdfsConf), appResourcesPath, confFileName,
					LocalResourceType.FILE, MeLoN_ConfigurationKeys.CONTAINER_RESOURCES, melonConf);
		}
	}

	private void zipFolder(java.nio.file.Path srcFolderPath, java.nio.file.Path zipPath) throws IOException {
		ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(zipPath.toFile()));
		java.nio.file.Files.walkFileTree(srcFolderPath, new SimpleFileVisitor<java.nio.file.Path>() {
			public FileVisitResult visitFile(java.nio.file.Path file, BasicFileAttributes attrs) throws IOException {
				zos.putNextEntry(new ZipEntry(srcFolderPath.relativize(file).toString()));
				java.nio.file.Files.copy(file, zos);
				zos.closeEntry();
				return FileVisitResult.CONTINUE;
			}
		});
		zos.close();
	}

	private int monitorApplication() throws YarnException, IOException {
		while (true) {
			try {
				Thread.sleep(5000);
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
					LOG.info("Application finished unsuccessfully." + "YarnState=" + appState.toString()
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
		int exitCode = -1;
		try {
			MeLoN_Client client = new MeLoN_Client();
			boolean doInit = client.init(args);
			if (!doInit) {
				LOG.error("Failed to init MeLoN_Client.");
				exitCode = -1;
			}
			exitCode = client.run();
		} catch (Throwable t) {
			LOG.error("Failed to finish MeLoN_Client seccessfully. with (" + t + ")");
			exitCode = -1;
		}
		if (exitCode == 0) {
			LOG.info("Application submitted successfully.");
		}
		System.exit(exitCode);

	}

}
