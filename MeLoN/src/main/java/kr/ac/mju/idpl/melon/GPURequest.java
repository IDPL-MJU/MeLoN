package kr.ac.mju.idpl.melon;

public class GPURequest {
	private GPUDeviceInfo device;
	private String requestTask;
	private int requiredGPUMemory;
	private Status requestStatus;
	private enum Status {
		NOT_READY,
		READY,
		REQUESTED,
		ALLOCATED
	}
	
	public GPURequest(String requestTask, int requiredGPUMemory) {
		this.device = null;
		this.requestTask = requestTask;
		this.requiredGPUMemory = requiredGPUMemory;
		setStatusNotReady();
	}
	
	public GPUDeviceInfo getDevice() {
		return device;
	}

	public String getRequestTask() {
		return requestTask;
	}

	public int getRequiredGPUMemory() {
		return requiredGPUMemory;
	}

	public Status getRequestStatus() {
		return requestStatus;
	}
	
	public String getFraction() {
		return "0." + String.format("%03d", (int) (this.requiredGPUMemory * 1000 / this.device.getTotal()));
	}

	public void setStatusReady() {
		this.requestStatus = Status.READY;
	}
	
	public void setStatusNotReady() {
		this.requestStatus = Status.NOT_READY;
	}
	
	public void setStatusRequested() {
		this.requestStatus = Status.REQUESTED;
	}
	
	public void setStatusAllocated() {
		this.requestStatus = Status.ALLOCATED;
	}
	
	public boolean isReady() {
		return this.requestStatus == Status.READY;
	}
	
	public boolean isRequested() {
		return this.requestStatus == Status.REQUESTED;
	}
	
	public boolean isAllocated() {
		return this.requestStatus == Status.ALLOCATED;
	}
	public void deviceAlloc(GPUDeviceInfo device) {
		this.device = device;
		setStatusReady();
	}
	
	public void resetRequest() {
		this.device = null;
		setStatusNotReady();
	}
}
