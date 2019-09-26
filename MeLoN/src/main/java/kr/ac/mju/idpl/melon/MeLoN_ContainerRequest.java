package kr.ac.mju.idpl.melon;

public class MeLoN_ContainerRequest {
	private String jobName;
	private int numInstances;
	private int vCores;
	private long memory;
	private int gpus;
	private int priority;
	public MeLoN_ContainerRequest(String jobName, int numInstances, long memory, int vCores, int gpus, int priority) {
		super();
		this.jobName = jobName;
		this.numInstances = numInstances;
		this.memory = memory;
		this.vCores = vCores;
		this.gpus = gpus;
		this.priority = priority;
	}
	public String getJobName() {
		return jobName;
	}
	public int getNumInstances() {
		return numInstances;
	}
	public int getvCores() {
		return vCores;
	}
	public long getMemory() {
		return memory;
	}
	public int getGpus() {
		return gpus;
	}
	public int getPriority() {
		return priority;
	}
	

}
