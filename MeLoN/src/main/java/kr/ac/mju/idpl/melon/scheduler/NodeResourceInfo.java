package kr.ac.mju.idpl.melon.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NodeResourceInfo {
	private String host;
	private long memory;
	private Map<Integer, GPUDeviceInfo> gpuDevicesInfo = new HashMap<>();

	NodeResourceInfo(String host) {
		this.host = host;
	}

	public void putGPUDeviceInfo(GPUDeviceInfo info) {
		gpuDevicesInfo.put(info.deviceNum, info);
	}

	public int getNumGPUs() {
		return gpuDevicesInfo.size();
	}

	public GPUDeviceInfo getGPUDeviceInfo(int deviceNum) {
		return gpuDevicesInfo.get(deviceNum);
	}
	
	public void updateGPUDeviceInfo(int deviceNum, int usedMemoryUsage) {
		this.gpuDevicesInfo.get(deviceNum).setUsedMemoryUsage(usedMemoryUsage);
	}

	public static class GPUDeviceInfo {
		private String host;
		private int deviceNum;
		private int requestedMemoryUsage;
		private int usedMemoryUsage;
		private int totalMemoryUsage;

		GPUDeviceInfo(String host, int deviceNum, int totalMemoryUsage) {
			this.host = host;
			this.deviceNum = deviceNum;
			this.totalMemoryUsage = totalMemoryUsage;
		}

		GPUDeviceInfo(String host, int deviceNum, int usedMemoryUsage, int totalMemoryUsage) {
			this.host = host;
			this.deviceNum = deviceNum;
			this.usedMemoryUsage = usedMemoryUsage;
			this.totalMemoryUsage = totalMemoryUsage;
		}

		public void requestMemory(int request) {
			// need to handle exceptions
			requestedMemoryUsage += request;
		}

		public void releaseMemory(int release) {
			// need to handle exceptions
			requestedMemoryUsage -= release;
		}

		public void setUsedMemoryUsage(int usage) {
			this.usedMemoryUsage = usage;
		}

		public String getHost() {
			return this.host;
		}

		public int getDeviceNum() {
			return this.deviceNum;
		}

		public int getRequestedMemoryUsage() {
			return this.requestedMemoryUsage;
		}

		public int getUsedMemoryUsage() {
			return this.usedMemoryUsage;
		}

		public int getTotalMemoryUsage() {
			return this.totalMemoryUsage;
		}

	}
}
