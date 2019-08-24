package kr.ac.mju.idpl.melon;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class MeLoN_Constants {
	public static final String HDFS_SITE_CONF = "hdfs-site.xml";
	public static final String YARN_SITE_CONF = YarnConfiguration.YARN_SITE_CONFIGURATION_FILE;
	public static final String CORE_SITE_CONF = YarnConfiguration.CORE_SITE_CONFIGURATION_FILE;
	public static final String HADOOP_CONF_DIR = ApplicationConstants.Environment.HADOOP_CONF_DIR.key();
	
	public static final String PYTHON_VENV_DIR = "venv";
	
	public static final String MELON_FINAL_XML = "melon-final.xml";
}
