package kr.ac.mju.idpl.melon.gpu.allocation.strategy;

import java.util.List;

import kr.ac.mju.idpl.melon.MeLoN_ContainerRequest;
import kr.ac.mju.idpl.melon.gpu.allocation.GPURequest;

public class StrategyExclusive implements GPUAllocationStrategy {
	private String strategyName;
	public StrategyExclusive () {
		strategyName = "EXCLUSIVE";
	}

	@Override
	public String getStrategyName() {
		return strategyName;
	}

	@Override
	public void initGpuDevAllocInfo(List<GPURequest> gpuDeviceAllocInfo, List<MeLoN_ContainerRequest> requests) {
		// TODO Auto-generated method stub
		
	}
}
