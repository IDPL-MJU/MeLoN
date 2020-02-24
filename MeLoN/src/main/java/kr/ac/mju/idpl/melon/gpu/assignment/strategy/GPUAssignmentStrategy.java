package kr.ac.mju.idpl.melon.gpu.assignment.strategy;

import java.util.List;
import java.util.Map;

import kr.ac.mju.idpl.melon.MeLoN_ContainerRequest;
import kr.ac.mju.idpl.melon.gpu.assignment.MeLoN_GPUDeviceInfo;
import kr.ac.mju.idpl.melon.gpu.assignment.MeLoN_GPURequest;

public interface GPUAssignmentStrategy {
	public String getStrategyName();
	public void initGPURequests(List<MeLoN_GPURequest> gpuRequests, List<MeLoN_ContainerRequest> requests);
	public boolean gpuAssignment(Map<String, MeLoN_GPUDeviceInfo> gpuDevicesInfo, List<MeLoN_GPURequest> gpuDeviceAllocInfo);
	public boolean isNecessaryGPUAssignment();
}
