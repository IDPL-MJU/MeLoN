package kr.ac.mju.idpl.melon.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kr.ac.mju.idpl.melon.LocalizableResource;
import kr.ac.mju.idpl.melon.MeLoN_ConfigurationKeys;
import kr.ac.mju.idpl.melon.MeLoN_Constants;
import kr.ac.mju.idpl.melon.MeLoN_ContainerRequest;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;

public class Utils {
	private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

	public static void unzipArchive(String src, String dst) {
		LOG.info("Unpacking " + src + " to destination " + dst);
		try {
			ZipFile zipFile = new ZipFile(src);
			zipFile.extractAll(dst);
		} catch (ZipException e) {
			LOG.error("Failed to unzip " + src, e);
		}
	}

	public static void extractResources() {
		if (new File(MeLoN_Constants.MELON_SRC_ZIP_NAME).exists()) {
			LOG.info("Unpacking src directory..");
			Utils.unzipArchive(MeLoN_Constants.MELON_SRC_ZIP_NAME, MeLoN_Constants.MELON_SRC_ZIP_DIR);
		}
		File venvZip = new File(MeLoN_Constants.PYTHON_VENV_ZIP);
		if (venvZip.exists() && venvZip.isFile()) {
			LOG.info("Unpacking Python virtual environment..");
			Utils.unzipArchive(MeLoN_Constants.PYTHON_VENV_ZIP, MeLoN_Constants.PYTHON_VENV_DIR);
		} else {
			LOG.info("No virtual environment uploaded.");
		}
	}

	public static void initYarnConf(Configuration yarnConf) {
		addCoreConfs(yarnConf);
		addComponentConfs(yarnConf, MeLoN_Constants.YARN_DEFAULT_CONF, MeLoN_Constants.YARN_SITE_CONF);
	}

	public static void initHdfsConf(Configuration hdfsConf) {
		addCoreConfs(hdfsConf);
		addComponentConfs(hdfsConf, MeLoN_Constants.HDFS_DEFAULT_CONF, MeLoN_Constants.HDFS_SITE_CONF);
	}

	private static void addCoreConfs(Configuration conf) {
		URL coreDefault = Utils.class.getClassLoader().getResource(MeLoN_Constants.CORE_DEFAULT_CONF);
		if (coreDefault != null) {
			conf.addResource(coreDefault);
		}
		if (new File(MeLoN_Constants.CORE_SITE_CONF).exists()) {
			conf.addResource(new Path(MeLoN_Constants.CORE_SITE_CONF));
		}
	}

	private static void addComponentConfs(Configuration conf, String defaultConfName, String siteConfName) {
		URL defaultConf = Utils.class.getClassLoader().getResource(defaultConfName);
		if (defaultConf != null) {
			conf.addResource(defaultConf);
		}
		if (new File(siteConfName).exists()) {
			conf.addResource(new Path(siteConfName));
		}
	}

	public static void addResources(String[] resources, Map<String, LocalResource> localResources, FileSystem fs) {
		if (null != resources) {
			for (String resource : resources) {
				addResource(resource, localResources, fs);
			}
		}
	}

	public static void addResource(String resource, Map<String, LocalResource> localResources, FileSystem fs) {
		try {
			if (resource != null) {
				// Check the format of the path, if the path is of path#archive, we set resource
				// type as ARCHIVE
				LocalizableResource lr = new LocalizableResource(resource, fs);
				if (lr.isDirectory()) {
					Path dirPath = lr.getRsrcPath();
					FileStatus[] ls = fs.listStatus(dirPath);
					for (FileStatus fileStatus : ls) {
						// We only add first level files.
						if (!fileStatus.isDirectory()) {
							addResource(fileStatus.getPath().toString(), localResources, fs);
						}
					}
				} else {
					localResources.put(lr.getLocalizedFileName(), lr.toLocalResource());
				}
			}
		} catch (IOException | ParseException e) {
			LOG.error("Failed to add " + resource + " to local resources.", e);
		}
	}

	public static String parseMemoryString(String memory) {
		memory = memory.toLowerCase();
		int m = memory.indexOf('m');
		int g = memory.indexOf('g');
		if (-1 != m) {
			return memory.substring(0, m);
		}
		if (-1 != g) {
			return String.valueOf(Integer.parseInt(memory.substring(0, g)) * 1024);
		}
		return memory;
	}

	public static int getNumTotalTasks(Configuration conf) {
		return getAllJobNames(conf).stream().mapToInt(type -> conf.getInt("melon." + type + ".instances", 0)).sum();
	}

	public static Set<String> getAllJobNames(Configuration conf) {
		return conf.getValByRegex(MeLoN_ConfigurationKeys.INSTANCES_REGEX).keySet().stream().map(Utils::getJobName)
				.collect(Collectors.toSet());
	}

	public static String getJobName(String confKey) {
		Pattern instancePattern = Pattern.compile(MeLoN_ConfigurationKeys.INSTANCES_REGEX);
		Matcher instanceMatcher = instancePattern.matcher(confKey);
		if (instanceMatcher.matches()) {
			return instanceMatcher.group(1);
		} else {
			return null;
		}
	}

	public static Map<String, String> parseKeyValue(String[] keyValues) {
		Map<String, String> keyValue = new HashMap<>();
		if (keyValues == null) {
			return keyValue;
		}
		for (String kv : keyValues) {
			String trimmedKeyValue = kv.trim();
			int index = kv.indexOf('=');
			if (index == -1) {
				keyValue.put(trimmedKeyValue, "");
				continue;
			}
			String key = trimmedKeyValue.substring(0, index);
			String val = "";
			if (index < (trimmedKeyValue.length() - 1)) {
				val = trimmedKeyValue.substring(index + 1);
			}
			keyValue.put(key, val);
		}
		return keyValue;
	}

	public static void setCapabilityGPU(Resource capability, int gpuCount) {
		if (gpuCount > 0) {
			Method method;
			try {
				method = capability.getClass().getMethod("setResourceValue", String.class, long.class);
				method.invoke(capability, "yarn.io/gpu", gpuCount);
			} catch (NoSuchMethodException e) {
				LOG.error("There is no 'setResourceValue' API in this version of YARN");
				throw new RuntimeException(e);
			} catch (IllegalAccessException | InvocationTargetException e) {
				LOG.error("Failed to invoke 'setResourceValue' method to set GPU resources", e);
				throw new RuntimeException(e);
			}
		}
	}

	public static Map<String, MeLoN_ContainerRequest> parseContainerRequests(Configuration conf) {
		Set<String> jobNames = getAllJobNames(conf);
		Map<String, MeLoN_ContainerRequest> containerRequests = new HashMap<>();
		int priority = 0;
		for (String jobName : jobNames) {
			int numInstances = conf.getInt("melon." + jobName + ".instances", 0);
			String memoryString = conf.get("melon." + jobName + ".memory", "2g");
			long memory = Long.parseLong(parseMemoryString(memoryString));
			int vCores = conf.getInt("melon." + jobName + ".vcores", 1);
			int gpus = conf.getInt("melon." + jobName + ".gpus", 0);

			/*
			 * The priority of different task types MUST be different. Otherwise the
			 * requests will overwrite each other on the RM scheduling side. See YARN-7631
			 * for details. For now we set the priorities of different task types
			 * arbitrarily.
			 */
			if (numInstances > 0) {
				// We rely on unique priority behavior to match allocation request to task in
				// Hadoop 2.7
				containerRequests.put(jobName,
						new MeLoN_ContainerRequest(jobName, numInstances, memory, vCores, gpus, priority));
				priority++;
			}
		}
		return containerRequests;
	}

	public static boolean isArchive(String path) {
		File f = new File(path);
		int fileSignature = 0;
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(f, "r");
			fileSignature = raf.readInt();
		} catch (IOException e) {
			// handle if you like
		} finally {
			IOUtils.closeQuietly(raf);
		}
		return fileSignature == 0x504B0304 // zip
				|| fileSignature == 0x504B0506 // zip
				|| fileSignature == 0x504B0708 // zip
				|| fileSignature == 0x74657374 // tar
				|| fileSignature == 0x75737461 // tar
				|| (fileSignature & 0xFFFF0000) == 0x1F8B0000; // tar.gz
	}

	public static boolean isJobNameTracked(String jobName, Configuration melonConf) {
		return !Arrays.asList(getUntrackedJobTypes(melonConf)).contains(jobName);
	}

	public static String[] getUntrackedJobTypes(Configuration conf) {
		return conf.getStrings(MeLoN_ConfigurationKeys.UNTRACKED_JOB_NAMES,
				MeLoN_ConfigurationKeys.UNTRACKED_JOB_NAMES_DEFAULT);
	}
}
