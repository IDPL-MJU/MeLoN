package kr.ac.mju.idpl.melon.gpu.allocation.strategy;

import java.util.List;

import kr.ac.mju.idpl.melon.MeLoN_ContainerRequest;
import kr.ac.mju.idpl.melon.gpu.allocation.GPURequest;

public class StrategyOverprovision implements GPUAllocationStrategy {
	private String strategyName;
	public StrategyOverprovision () {
		strategyName = "OVERPROVISION";
	}
	@Override
	public String getStrategyName() {
		return strategyName;
	}
	@Override
	public void initGpuDevAllocInfo(List<GPURequest> gpuDeviceAllocInfo, List<MeLoN_ContainerRequest> requests) {
		for (MeLoN_ContainerRequest request : requests) {
			gpuDeviceAllocInfo.add(new GPURequest(request.getJobName(), request.getGpuMemory()));
		}
	}

}
