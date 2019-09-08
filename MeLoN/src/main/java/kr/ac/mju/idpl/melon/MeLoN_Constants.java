package kr.ac.mju.idpl.melon;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class MeLoN_Constants {
	public static final String AM_NAME = "am";
	public static final String HDFS_SITE_CONF = "hdfs-site.xml";
	public static final String YARN_SITE_CONF = YarnConfiguration.YARN_SITE_CONFIGURATION_FILE;
	public static final String CORE_SITE_CONF = YarnConfiguration.CORE_SITE_CONFIGURATION_FILE;
	public static final String HADOOP_CONF_DIR = ApplicationConstants.Environment.HADOOP_CONF_DIR.key();

	public static final String PYTHON_VENV_DIR = "venv";

	public static final String MELON_FINAL_XML = "melon-final.xml";
	public static final String MELON_SRC_ZIP_NAME = "melon_src.zip";
	public static final String PYTHON_VENV_ZIP = "venv.zip";

	public static final String MELON_CONF_PREFIX = "MELON_CONF";
	public static final String ARCHIVE_SUFFIX = "#archive";
	public static final String RESOURCE_DIVIDER = "::";

	public static final String PATH_SUFFIX = "_PATH";
	public static final String TIMESTAMP_SUFFIX = "_TIMESTAMP";
	public static final String LENGTH_SUFFIX = "_LENGTH";
}
