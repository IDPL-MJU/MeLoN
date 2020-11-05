package kr.ac.mju.idpl.melon.gpu.assignment;

import java.util.ArrayList;
import java.util.List;

public class MeLoN_GPUDeviceInfo {
	private String deviceHost;
	private int deviceNum;
	private String deviceId;	
	private int total;
	
	private int used;
	private int free;
	private int gpuUtil;
	private int computeProcessCount;
	
	private int assigned;
	private int nonAssigned;
	private int assignedCPC;
	private int assignedGPUUtil;
	private List<String> assignedTask;
	
	public MeLoN_GPUDeviceInfo(String deviceHost, int deviceNum, int total, int used, int cpc, int gpuUtil){
		this.deviceHost = deviceHost;
		this.deviceNum = deviceNum;
		this.deviceId = deviceHost + ":" + deviceNum;
		this.total = total;
		this.used = used;
		this.free = total - used;
		this.gpuUtil = gpuUtil;
		this.computeProcessCount = cpc;
		
		this.assigned = 0;
		this.nonAssigned = total - this.assigned;
		this.assignedTask = new ArrayList<>();
		this.assignedCPC = 0;
		this.assignedGPUUtil = 0;
	}
	
	private void computeNonAssignedMemory() {
		this.nonAssigned = this.total - this.assigned;
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
		return free - assigned;
	}

	public int getNonAssigned() {
		return nonAssigned;
	}

	public int getComputeProcessCount() {
		return computeProcessCount + assignedCPC;
	}

	public int getGpuUtil() {
		return gpuUtil + assignedGPUUtil;
	}
	
	public float getGPUUtilPerCPC() {
		return getComputeProcessCount() == 0 ? gpuUtil / 1 : gpuUtil / getComputeProcessCount();
	}
	
	public void increaseComputeProcessCount() {
		this.assignedCPC++;
		this.assignedGPUUtil+=100;
	}
	
	public void decreaseComputeProcessCount() {
		this.assignedCPC--;
		this.assignedGPUUtil-=100;
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
	
	public synchronized void assignMemory(int assign, String task) {
		this.assigned += assign;
		computeNonAssignedMemory();
		this.assignedTask.add(task);
	}
	
	public synchronized void deassignMemory(int deassign, String task) {
		for(String str : assignedTask) {
			if(str.equals(task)) {
				this.assigned -= deassign;
				computeNonAssignedMemory();
				this.assignedTask.remove(str);
				break;
			}
		}
	}
}
