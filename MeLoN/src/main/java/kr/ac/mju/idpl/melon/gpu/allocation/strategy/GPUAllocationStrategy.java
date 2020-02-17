package kr.ac.mju.idpl.melon.gpu.allocation.strategy;

import java.util.List;

import kr.ac.mju.idpl.melon.MeLoN_ContainerRequest;
import kr.ac.mju.idpl.melon.gpu.allocation.GPURequest;

public interface GPUAllocationStrategy {
	public String getStrategyName();
	public void initGpuDevAllocInfo(List<GPURequest> gpuDeviceAllocInfo, List<MeLoN_ContainerRequest> requests);
}
