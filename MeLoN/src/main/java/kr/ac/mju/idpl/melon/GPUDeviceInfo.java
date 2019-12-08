package kr.ac.mju.idpl.melon;

import java.util.ArrayList;
import java.util.List;

public class GPUDeviceInfo {
	private String deviceHost;
	private int deviceNum;
	private String deviceId;	
	private int total;
	
	private int used;
	private int free;
	
	private int allocated;
	private int nonAllocated;
	
	private List<String> allocatedTask;
	
	private int computeProcessCount;
	private int gpuUtil;
	
	public GPUDeviceInfo(String deviceHost, int deviceNum, int total, int used, int cpc, int gpuUtil){
		this.deviceHost = deviceHost;
		this.deviceNum = deviceNum;
		this.deviceId = deviceHost + ":" + deviceNum;
		this.total = total;
		this.used = used;
		this.free = total - used;
		this.allocated = 0;
		this.nonAllocated = total - this.allocated;
		this.allocatedTask = new ArrayList<>();
		this.computeProcessCount = cpc;
		this.gpuUtil = gpuUtil;
	}
	
	private void computeNonAllocatedMemory() {
		this.nonAllocated = this.total - this.allocated;
	}
	
	public String getDeviceHost() {
		return deviceHost;
	}

	public int getDeviceNum() {
		return deviceNum;
	}

	public String getDeviceId() {
		return deviceId;
	}
	
	public int getTotal() {
		return total;
	}
	
	public int getUsed() {
		return used;
	}

	public int getFree() {
		return free - allocated;
	}

	public int getNonAllocated() {
		return nonAllocated;
	}

	public int getComputeProcessCount() {
		return computeProcessCount;
	}

	public int getGpuUtil() {
		return gpuUtil;
	}
	
	public void plusComputeProcessCount() {
		this.computeProcessCount++;
	}

	public void updateGPUInfo(int used, int cptPsCnt, int gpuUtil) {
		this.used = used;
		this.free = this.total - used;
		this.computeProcessCount = cptPsCnt;
		this.gpuUtil = gpuUtil;
	}

	public void updateMemoryUsage(int used) {
		this.used = used;
		this.free = this.total - used;
	}
	
	public synchronized void allocateMemory(int alloc, String task) {
		this.allocated += alloc;
		computeNonAllocatedMemory();
		this.allocatedTask.add(task);
	}
	
	public synchronized void deallocateMemory(int dealloc, String task) {
		for(String str : allocatedTask) {
			if(str.equals(task)) {
				this.allocated -= dealloc;
				computeNonAllocatedMemory();
				this.allocatedTask.remove(str);
				break;
			}
		}
	}
}
