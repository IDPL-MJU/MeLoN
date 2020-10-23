package kr.ac.mju.idpl.melon;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class MeLoN_Constants {
	public static final String AM_NAME = "am";
	public static final String HDFS_SITE_CONF = "hdfs-site.xml";
	public static final String HDFS_DEFAULT_CONF = "hdfs-default.xml";
	public static final String YARN_SITE_CONF = YarnConfiguration.YARN_SITE_CONFIGURATION_FILE;
	public static final String YARN_DEFAULT_CONF = "yarn-default.xml";
	public static final String CORE_SITE_CONF = YarnConfiguration.CORE_SITE_CONFIGURATION_FILE;
	public static final String CORE_DEFAULT_CONF = "core-site.xml";
	public static final String HADOOP_CONF_DIR = ApplicationConstants.Environment.HADOOP_CONF_DIR.key();

	public static final String PYTHON_VENV_DIR = "venv";

	public static final String MELON_JAR = "melon.jar";
	public static final String MELON_FINAL_XML = "melon-final.xml";
	public static final String MELON_SRC_ZIP_NAME = "src.zip";
	public static final String MELON_SRC_ZIP_DIR = "./";
	public static final String MELON_SRC_DIR = "./src";
	public static final String PYTHON_VENV_ZIP_DIR = "./";
	public static final String PYTHON_VENV_ZIP = "venv.zip";

	public static final String MELON_CONF_PREFIX = "MELON_CONF";
	public static final String MELON_JAR_PREFIX = "MELON_JAR";
	public static final String ARCHIVE_SUFFIX = "#archive";
	public static final String RESOURCE_DIVIDER = "::";

	public static final String PATH_SUFFIX = "_PATH";
	public static final String TIMESTAMP_SUFFIX = "_TIMESTAMP";
	public static final String LENGTH_SUFFIX = "_LENGTH";

	// TensorFlow job constants
	public static final String JOB_NAME = "JOB_NAME";
	public static final String TASK_INDEX = "TASK_INDEX";
	public static final String TASK_NUM = "TASK_NUM";
	public static final String CLUSTER_SPEC = "CLUSTER_SPEC";
	// public static final String SESSION_ID = "SESSION_ID";

	public static final String AM_HOST = "AM_HOST";
	public static final String AM_PORT = "AM_PORT";
	public static final String APP_EXECUTION_TYPE = "APP_EXECUTION_TYPE";
	public static final String APP_ID = "APP_ID";

	public static final String PATH = "PATH";
	public static final String LD_LIBRARY_PATH = "LD_LIBRARY_PATH";
	public static final String CUDA_DEVICE_ORDER = "CUDA_DEVICE_ORDER";
	public static final String CUDA_VISIBLE_DEVICES = "CUDA_VISIBLE_DEVICES";
	public static final String FRACTION = "FRACTION";
	public enum AppExecutionType {BATCH, DISTRIBUTED, TEST_CLIENT, TEST_AM, TEST_SHELL}
	public enum GPUAllocType {EXCLUSIVE, OVERPROVISION}
	public enum FileSystemType {HDFS, LUSTRE}
	
	//FileSystem
	public static final String LUSTRE_FILESYSTEM_URI = "/mnt/lustre";
	
}