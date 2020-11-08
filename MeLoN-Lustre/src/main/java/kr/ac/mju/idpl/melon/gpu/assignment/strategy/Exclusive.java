package kr.ac.mju.idpl.melon.gpu.assignment.strategy;

import java.util.List;
import java.util.Map;

import kr.ac.mju.idpl.melon.MeLoN_ContainerRequest;
import kr.ac.mju.idpl.melon.gpu.assignment.MeLoN_GPUDeviceInfo;
import kr.ac.mju.idpl.melon.gpu.assignment.MeLoN_GPURequest;

public class Exclusive implements GPUAssignmentStrategy {
	private String strategyName;
	public Exclusive () {
		strategyName = "EXCLUSIVE";
	}

	@Override
	public String getStrategyName() {
		return strategyName;
	}

	@Override
	public void initGPURequests(List<MeLoN_GPURequest> gpuRequests, List<MeLoN_ContainerRequest> requests) {
	}

	@Override
	public boolean gpuAssignment(Map<String, MeLoN_GPUDeviceInfo> gpuDevicesInfo, List<MeLoN_GPURequest> gpuDeviceAllocInfo) {
		return true;
	}

	@Override
	public boolean isNecessaryGPUAssignment() {
		return true;
	}
}
