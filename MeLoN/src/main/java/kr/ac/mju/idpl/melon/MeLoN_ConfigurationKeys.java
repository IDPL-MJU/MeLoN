package kr.ac.mju.idpl.melon;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MeLoN_ConfigurationKeys {
	public static final String MELON_PREFIX = "melon.";

	public static final String AM_PREFIX = MELON_PREFIX + "am.";
	public static final String AM_MEMORY = MELON_PREFIX + AM_PREFIX + "memory";
	public static final String AM_MEMORY_DEFAULT = "1024";
	public static final String AM_VCORES = MELON_PREFIX + AM_PREFIX + "vcores";
	public static final int AM_VCORES_DEFAULT = 1;
	public static final String AM_GPUS = MELON_PREFIX + AM_PREFIX + "gpus";
	public static final int AM_GPUS_DEFAULT = 0;

	public static final String APPLICATION_PREFIX = MELON_PREFIX + "application.";
	public static final String APPLICATION_NAME = MELON_PREFIX + APPLICATION_PREFIX + "name";
	public static final String APPLICATION_NAME_DEFAULT = "MeLoN_App";
	public static final String HDFS_CONF_PATH = MELON_PREFIX + APPLICATION_PREFIX + "hdfs-conf-path";
	public static final String YARN_CONF_PATH = MELON_PREFIX + APPLICATION_PREFIX + "yarn-conf-path";

	public static final String PYTHON_VENV = APPLICATION_PREFIX + "python-venv";
	public static final String PYTHON_BIN_PATH = APPLICATION_PREFIX + "python-bin-path";
	
	public static final String TASK_PREFIX = MELON_PREFIX + "task.";
	public static final String TASK_EXECUTOR_JVM_OPTS = TASK_PREFIX + "executor-jvm-opts";
	public static final String TASK_EXECUTOR_JVM_OPTS_DEFAULT = "-Xmx1536m";

	public static final String UNTRACKED_JOBTYPPES = MELON_PREFIX + "untracked.jobtypes";
	public static final String UNTRACKED_JOBTYPPES_DEFAULT = "ps";

	public static final String SHELL_ENVS = MELON_PREFIX + "shell.envs";
	public static final String CONTAINER_ENVS = MELON_PREFIX + "container.envs";
	public static final String CONTAINER_RESOURCES = MELON_PREFIX + "container.resources";

	public static final String CONTAINERS_COMMAND = MELON_PREFIX + "containers.command";

	public static final String INSTANCES_REGEX = "melon\\.([a-z]+)\\.instances";
	public static final String RESOURCES_REGEX = "melon\\.([a-z]+)\\.resources";
	// Configurations that can take multiple values.
	public static final List<String> MULTI_VALUE_CONF = Collections
			.unmodifiableList(Arrays.asList(CONTAINER_ENVS, SHELL_ENVS));

	// Job specific resources
	public static String getResourcesKey(String jobName) {
		return String.format(MELON_PREFIX + "%s.resources", jobName);
	}
}
