package kr.ac.mju.idpl.melon;

import org.apache.hadoop.yarn.api.records.Container;

public class MeLoN_Task {
	private String taskType;
	private String taskIndex;
	private String taskUrl;
	private String host;
	private int port = -1;
	private Container container;
	private MeLoN_TaskStatus status;
	private boolean completed = false;
	private int exitStatus = -1;

	public boolean isCompleted() {
		return completed;
	}

	public MeLoN_Task(String taskType, String taskIndex) {
		this.taskType = taskType;
		this.taskIndex = taskIndex;
	}

	public String getTaskType() {
		return taskType;
	}

	public String getTaskIndex() {
		return taskIndex;
	}

	public Container getContainer() {
		return container;
	}
	
	public int getExitStatus() {
		return exitStatus;
	}
	
	public String getHost() {
		return host;
	}

	public String getHostPort() {
		return String.format("%s:%d", host, port < 0 ? 0 : port);
	}

	public void setContainer(Container container) {
		this.container = container;
	}

	public void addContainer(Container container) {
		setContainer(container);
	}

	public String getId() {
		return this.taskType + ":" + this.taskIndex;
	}

	public void setStatus(MeLoN_TaskStatus status) {
		this.status = status;
	}

	public void setHostPort(String hostPort) {
		this.host = hostPort.split(":")[0];
		this.port = Integer.parseInt(hostPort.split(":")[1]);
	}

}
