package kr.ac.mju.idpl.melon;

import org.apache.hadoop.yarn.api.records.ContainerId;

public class ExecutorExecutionResult {
	private int exitCode;
	private String host;
	private String device;
	private String fraction;
	private String jobName;
	private int taskIndex;
	private long containerExecutingTime;
	private long processExecutingTime;
	public ExecutorExecutionResult(int exitCode, String host, String device, String fraction, String jobName,
			int taskIndex, long containerExecutingTime, long processExecutingTime) {
		super();
		this.exitCode = exitCode;
		this.host = host;
		this.device = device;
		this.fraction = fraction;
		this.jobName = jobName;
		this.taskIndex = taskIndex;
		this.containerExecutingTime = containerExecutingTime;
		this.processExecutingTime = processExecutingTime;
	}
	public int getExitCode() {
		return exitCode;
	}
	public String getHost() {
		return host;
	}
	public String getDevice() {
		return device;
	}
	public String getFraction() {
		return fraction;
	}
	public String getJobName() {
		return jobName;
	}
	public int getTaskIndex() {
		return taskIndex;
	}
	public String getTaskId() {
		return jobName + ":" + taskIndex;
	}
	public long getContainerExecutingTime() {
		return containerExecutingTime;
	}
	public long getProcessExecutingTime() {
		return processExecutingTime;
	}
	
}
