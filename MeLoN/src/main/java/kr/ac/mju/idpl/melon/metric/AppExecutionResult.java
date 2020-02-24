package kr.ac.mju.idpl.melon.metric;

public class AppExecutionResult {
	private String applicationId;
	private long appExecutionTime;
	private long lastSessionExecutionTime;
	private String appExecutionType;
	private String gpuAssignmentType;
	private int amMemory;
	private int amVCores;
	private int workerMemory;
	private int workerVCores;
	private long workerGPUMemory;
	private int workerInstances;
	
	public void setApplicationId(String applicationId) {
		this.applicationId = applicationId;
	}
	public void setAppExecutionTime(long appExecutionTime) {
		this.appExecutionTime = appExecutionTime;
	}
	public void setLastSessionExecutionTime(long lastSessionExecutionTime) {
		this.lastSessionExecutionTime = lastSessionExecutionTime;
	}
	public void setAppExecutionType(String appExecutionType) {
		this.appExecutionType = appExecutionType;
	}
	public void setGpuAssignmentType(String gpuAssignmentType) {
		this.gpuAssignmentType = gpuAssignmentType;
	}
	public void setAmMemory(int amMemory) {
		this.amMemory = amMemory;
	}
	public void setAmVCores(int amVCores) {
		this.amVCores = amVCores;
	}
	public void setWorkerMemory(int workerMemory) {
		this.workerMemory = workerMemory;
	}
	public void setWorkerVCores(int workerVCores) {
		this.workerVCores = workerVCores;
	}
	public void setWorkerGPUMemory(long workerGPUMemory) {
		this.workerGPUMemory = workerGPUMemory;
	}
	public void setWorkerInstances(int workerInstances) {
		this.workerInstances = workerInstances;
	}
	public String getApplicationId() {
		return applicationId;
	}
	public long getAppExecutionTime() {
		return appExecutionTime;
	}
	public long getLastSessionExecutionTime() {
		return lastSessionExecutionTime;
	}
	public String getAppExecutionType() {
		return appExecutionType;
	}
	public String getGpuAssignmentType() {
		return gpuAssignmentType;
	}
	public int getAmMemory() {
		return amMemory;
	}
	public int getAmVCores() {
		return amVCores;
	}
	public int getWorkerMemory() {
		return workerMemory;
	}
	public int getWorkerVCores() {
		return workerVCores;
	}
	public long getWorkerGPUMemory() {
		return workerGPUMemory;
	}
	public int getWorkerInstances() {
		return workerInstances;
	}
	
	
	
}
