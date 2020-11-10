package kr.ac.mju.idpl.melon.gpu.assignment.strategy;

import java.util.List;
import java.util.Map;

import kr.ac.mju.idpl.melon.MeLoN_ContainerRequest;
import kr.ac.mju.idpl.melon.MeLoN_Constants.AppExecutionType;
import kr.ac.mju.idpl.melon.gpu.assignment.MeLoN_GPUDeviceInfo;
import kr.ac.mju.idpl.melon.gpu.assignment.MeLoN_GPURequest;

public class Overprovision implements GPUAssignmentStrategy {
	private String strategyName;

	public Overprovision() {
		strategyName = "OVERPROVISION";
	}

	@Override
	public String getStrategyName() {
		return strategyName;
	}

	@Override
	public void initGPURequests(List<MeLoN_GPURequest> gpuRequests, List<MeLoN_ContainerRequest> requests) {
		for (MeLoN_ContainerRequest request : requests) {
			gpuRequests.add(new MeLoN_GPURequest(request.getJobName(), request.getGpuMemory()));
		}
	}

	@Override
	public boolean gpuAssignment(Map<String, MeLoN_GPUDeviceInfo> gpuDevicesInfo, List<MeLoN_GPURequest> gpuRequests) {
		boolean allAssigned = true;
		for (MeLoN_GPURequest gpuReq : gpuRequests) {
			if (gpuReq.isStandby()) {
				MeLoN_GPUDeviceInfo assignDevice = null;
				for (String deviceId : gpuDevicesInfo.keySet()) {
					assignDevice = gpuDevicesInfo.get(deviceId);
					break;
				}
				for (String deviceId : gpuDevicesInfo.keySet()) {
					if (gpuDevicesInfo.get(deviceId).getFree() > gpuReq.getGPUMemory() * 1.1) {
						if (gpuDevicesInfo.get(deviceId).getGpuUtil() < (assignDevice.getGpuUtil() + 5)
								&& gpuDevicesInfo.get(deviceId).getGpuUtil() > (assignDevice.getGpuUtil() - 5)) {
							if (gpuDevicesInfo.get(deviceId).getComputeProcessCount() != 0 && gpuDevicesInfo
									.get(deviceId).getGPUUtilPerCPC() > assignDevice.getGPUUtilPerCPC()) {
								assignDevice = gpuDevicesInfo.get(deviceId);
							}
						} else if (gpuDevicesInfo.get(deviceId).getGpuUtil() <= (assignDevice.getGpuUtil() - 5)) {
							assignDevice = gpuDevicesInfo.get(deviceId);
						}
					}
				}
				if ((int) (gpuReq.getGPUMemory() * 1.1) <= 0) {
					gpuReq.setStatusAssigned();
				} else if (assignDevice != null && (int) (gpuReq.getGPUMemory() * 1.1) < assignDevice.getFree()) {
					gpuReq.deviceAssign(assignDevice);
					assignDevice.assignMemory((int) (gpuReq.getGPUMemory() * 1.1), gpuReq.getJobName());
					assignDevice.increaseComputeProcessCount();
				} else {
					gpuReq.setStatusStandby();
					allAssigned = false;
				}
			}
		}
		return allAssigned;
	}

	@Override
	public boolean isNecessaryGPUAssignment() {
		return true;
	}
}
