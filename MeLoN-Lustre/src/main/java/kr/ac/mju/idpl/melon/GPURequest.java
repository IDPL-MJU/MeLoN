package kr.ac.mju.idpl.melon;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;

public class GPURequest {
	private GPUDeviceInfo device;
	private String requestTask;
	private ContainerId containerId;
	private int requiredGPUMemory;
	private Status requestStatus;
	private enum Status {
		NOT_READY,
		READY,
		REQUESTED,
		ALLOCATED,
		FINISHED
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
	
	public ContainerId getContainerId() {
		return containerId;
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

	public void setContainerId(ContainerId containerId) {
		this.containerId = containerId;
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
	
	public void setStatusFinished() {
		this.requestStatus = Status.FINISHED;
	}
	
	public boolean isReady() {
		return this.requestStatus == Status.READY;
	}
	
	public boolean isNotReady() {
		return this.requestStatus == Status.NOT_READY;
	}
	
	public boolean isRequested() {
		return this.requestStatus == Status.REQUESTED;
	}
	
	public boolean isAllocated() {
		return this.requestStatus == Status.ALLOCATED;
	}
	
	public boolean isFinished() {
		return this.requestStatus == Status.FINISHED;
	}
	
	public boolean isDeviceAllocated() {
		return this.device != null;
	}
	
	public boolean isThisContainer(ContainerId containerId) {
		return this.containerId.equals(containerId);
	}
	public void deviceAlloc(GPUDeviceInfo device) {
		this.device = device;
		setStatusReady();
	}

	public void finished() {
		if(this.device != null) {
			this.device.decreaseComputeProcessCount();
			this.device.deallocateMemory(requiredGPUMemory, requestTask);
			this.device = null;
		}
		setStatusFinished();
	}
	
	public void resetRequest() {
		this.device.decreaseComputeProcessCount();
		this.device.deallocateMemory((int) (this.requiredGPUMemory * 1.1), this.requestTask);
		this.device = null;
		setStatusNotReady();
	}
}
