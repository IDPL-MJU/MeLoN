package kr.ac.mju.idpl.melon.gpu.assignment;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;

public class MeLoN_GPURequest {
	private MeLoN_GPUDeviceInfo device;
	private String jobName;
	private ContainerId containerId;
	private int gpuMemory;
	private Status status;
	private enum Status {
		STANDBY,
		ASSIGNED,
		REQUESTED,
		ALLOCATED,
		FINISHED
	}
	
	public MeLoN_GPURequest(String jobName, int gpuMemory) {
		this.device = null;
		this.jobName = jobName;
		this.gpuMemory = gpuMemory;
		setStatusStandby();
	}
	
	public MeLoN_GPUDeviceInfo getDevice() {
		return device;
	}

	public String getJobName() {
		return jobName;
	}
	
	public ContainerId getContainerId() {
		return containerId;
	}

	public int getGPUMemory() {
		return gpuMemory;
	}

	public Status getRequestStatus() {
		return status;
	}
	
	public String getFraction() {
		return "0." + String.format("%03d", (int) (this.gpuMemory * 1000 / this.device.getTotal()));
	}

	public void setContainerId(ContainerId containerId) {
		this.containerId = containerId;
	}
	
	public void setStatusAssigned() {
		this.status = Status.ASSIGNED;
	}
	
	public void setStatusStandby() {
		this.status = Status.STANDBY;
	}
	
	public void setStatusRequested() {
		this.status = Status.REQUESTED;
	}

	public void setStatusAllocated() {
		this.status = Status.ALLOCATED;
	}
	
	public void setStatusFinished() {
		this.status = Status.FINISHED;
	}
	
	public boolean isAssigned() {
		return this.status == Status.ASSIGNED;
	}
	
	public boolean isStandby() {
		return this.status == Status.STANDBY;
	}
	
	public boolean isRequested() {
		return this.status == Status.REQUESTED;
	}
	
	public boolean isAllocated() {
		return this.status == Status.ALLOCATED;
	}
	
	public boolean isFinished() {
		return this.status == Status.FINISHED;
	}
	
	public boolean isDeviceAllocated() {
		return this.device != null;
	}
	
	public boolean isThisContainer(ContainerId containerId) {
		return this.containerId.equals(containerId);
	}
	public void deviceAssign(MeLoN_GPUDeviceInfo device) {
		this.device = device;
		setStatusAssigned();
	}

	public void finished() {
		if(this.device != null) {
			this.device.decreaseComputeProcessCount();
			this.device.deassignMemory(gpuMemory, jobName);
			this.device = null;
		}
		setStatusFinished();
	}
	
	public void resetRequest() {
		this.device.decreaseComputeProcessCount();
		this.device.deassignMemory((int) (this.gpuMemory * 1.1), this.jobName);
		this.device = null;
		setStatusStandby();
	}
}
