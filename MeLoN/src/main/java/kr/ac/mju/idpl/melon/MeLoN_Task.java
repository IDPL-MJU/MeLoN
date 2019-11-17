package kr.ac.mju.idpl.melon;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;

public class MeLoN_Task {
	private String jobName;
	private String taskIndex;
	private String taskUrl;
	private String host;
	private int port = -1;
	private Container container;
	private TaskStatus status;
	private boolean completed = false;
	private int exitStatus = -1;

	public boolean isCompleted() {
		return completed;
	}

	public MeLoN_Task(String jobName, String taskIndex) {
		this.jobName = jobName;
		this.taskIndex = taskIndex;
	}

	public String getJobName() {
		return jobName;
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
		return this.jobName + ":" + this.taskIndex;
	}

	public void setStatus(TaskStatus status) {
		this.status = status;
	}

	public void setExitStatus(int status) {
		if (exitStatus == -1) {
	        this.exitStatus = status;
	        switch (status) {
	          case ContainerExitStatus.SUCCESS:
	            setStatus(TaskStatus.SUCCEEDED);
	            break;
	          case ContainerExitStatus.KILLED_BY_APPMASTER:
	            setStatus(TaskStatus.FINISHED);
	            break;
	          default:
	            setStatus(TaskStatus.FAILED);
	            break;
	        }
	        this.completed = true;
		}
	}

	public void setHostPort(String hostPort) {
		this.host = hostPort.split(":")[0];
		this.port = Integer.parseInt(hostPort.split(":")[1]);
	}

}
