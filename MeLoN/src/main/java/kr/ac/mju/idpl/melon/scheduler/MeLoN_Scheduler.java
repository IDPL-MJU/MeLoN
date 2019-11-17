package kr.ac.mju.idpl.melon.scheduler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import kr.ac.mju.idpl.melon.scheduler.NodeResourceInfo.GPUDeviceInfo;

public class MeLoN_Scheduler {
	private static final Logger LOG = LoggerFactory.getLogger(MeLoN_Scheduler.class);
//	private String[] nodes = new String[] { "master", "slave1", "slave2" };
	private String[] nodes = new String[] { "master" };
	private Map<String, NodeResourceInfo> nodeResourceInfo = new HashMap<>();

	private void clusterGPUMonitoring() throws IOException, InterruptedException, SAXException,
			ParserConfigurationException, NumberFormatException, XPathExpressionException {

		for (String host : nodes) {
			NodeResourceInfo nri = new NodeResourceInfo(host);
			if (nodeResourceInfo.containsKey(host)) {
				nri = nodeResourceInfo.get(host);
			}
//			LOG.info("=================================");
//			LOG.info("Host = {}", host);
			ProcessBuilder monitoringProcessBuilder = new ProcessBuilder("sh", "-c",
					"ssh " + host + " nvidia-smi -q -x");
			Process monitoringProcess = monitoringProcessBuilder.start();
			monitoringProcess.waitFor();
			BufferedReader br = new BufferedReader(new InputStreamReader(monitoringProcess.getInputStream()));

			String result = "";
			String line;
			for (int i = 0; (line = br.readLine()) != null; i++) {
				// skip xml document spec
				if (i > 1) {
					result = result + line.trim();
				}
			}
			InputSource is = new InputSource(new StringReader(result));
			Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(is);
			XPath xPath = XPathFactory.newInstance().newXPath();
			String expression = "/nvidia_smi_log/attached_gpus";
			int gpuNum = Integer.parseInt(xPath.compile(expression).evaluate(doc));
//			LOG.info("*********************************");
			for (int i = 1; i <= gpuNum; i++) {
				expression = "/nvidia_smi_log/gpu[" + i + "]/minor_number";
				int deviceNum = Integer.parseInt(xPath.compile(expression).evaluate(doc));
				expression = "/nvidia_smi_log/gpu[" + i + "]/fb_memory_usage/total";
				String totalMemoryUsageStr = xPath.compile(expression).evaluate(doc);
				int totalMemoryUsage = parseMibStrToMbInt(totalMemoryUsageStr);
				expression = "/nvidia_smi_log/gpu[" + i + "]/fb_memory_usage/used";
				String usedMemoryUsageStr = xPath.compile(expression).evaluate(doc);
				int usedMemoryUsage = parseMibStrToMbInt(usedMemoryUsageStr);
				expression = "/nvidia_smi_log/gpu[" + i + "]/fb_memory_usage/free";
				String freeMemoryUsageStr = xPath.compile(expression).evaluate(doc);
				int freeMemoryUsage = parseMibStrToMbInt(freeMemoryUsageStr);

				if (nri.getGPUDeviceInfo(i-1) == null) {
					GPUDeviceInfo gdi = new GPUDeviceInfo(host, deviceNum, usedMemoryUsage,
							freeMemoryUsage + usedMemoryUsage);
					nri.putGPUDeviceInfo(gdi);
				} else {
					nri.updateGPUDeviceInfo(i-1, usedMemoryUsage);
				}

//				LOG.info("***Device number = {}", deviceNum);
//				LOG.info("***Total Memory Usage = {}(MiB), {}(MB)", totalMemoryUsageStr, totalMemoryUsage);
//				LOG.info("***Used Memory Usage = {}(MiB), {}(MB)", usedMemoryUsageStr, usedMemoryUsage);
//				LOG.info("***Free Memory Usage = {}(MiB), {}(MB)", freeMemoryUsageStr, freeMemoryUsage);
//				LOG.info("*********************************");
			}
			nodeResourceInfo.put(host, nri);
//			LOG.info("=================================");
		}

	}

	private void logMonitoredInfo() {
		for (String host : nodes) {
			LOG.info("=================================");
			LOG.info("Host = {}", host);
			LOG.info("*********************************");
			LOG.info("getNumGPUs={}", nodeResourceInfo.get(host).getNumGPUs());
			for (int i = 0; i < nodeResourceInfo.get(host).getNumGPUs(); i++) {
				LOG.info("***DNum={}, total={}(MB), used={}(MB), free={}(MB)", 
						nodeResourceInfo.get(host).getGPUDeviceInfo(i).getDeviceNum()
						, nodeResourceInfo.get(host).getGPUDeviceInfo(i).getTotalMemoryUsage()
						, nodeResourceInfo.get(host).getGPUDeviceInfo(i).getUsedMemoryUsage()
						, nodeResourceInfo.get(host).getGPUDeviceInfo(i).getTotalMemoryUsage()
						- nodeResourceInfo.get(host).getGPUDeviceInfo(i).getUsedMemoryUsage());
			}
			LOG.info("=================================");
		}
	}

	private int parseMibStrToMbInt(String memoryUsageStr) {
		memoryUsageStr = memoryUsageStr.toLowerCase();
		int mib = memoryUsageStr.indexOf("mib");
		if (-1 != mib) {
			return Integer.parseInt(memoryUsageStr.substring(0, mib).trim()) * 104858 / 100000;
		}
		return 0;
	}

	private boolean run() throws IOException, InterruptedException, SAXException, ParserConfigurationException,
			NumberFormatException, XPathExpressionException {

		int i = 0;
		while (i==0) {
			LOG.info("Cluster GPU Monitoring...");
			clusterGPUMonitoring();
			
			LOG.info("Cluster GPU logging...");
			logMonitoredInfo();
			
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				LOG.error("Thread interrupted", e);
			}
			
			//i++;
		}
		return true;
	}

	public static void main(String[] args) {
		MeLoN_Scheduler scheduler = new MeLoN_Scheduler();
		try {
			LOG.info("MeLoN_Scheduler Start...");
			boolean result = scheduler.run();
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException | ParserConfigurationException | NumberFormatException | XPathExpressionException e) {
			LOG.error("Parsing nvidia-smi XML caused error.");
		}
		System.exit(0);
	}

	class GPUMonitor {
		String host;

		GPUMonitor(String host, int port, String username) {
			this.host = host;
		}
	}
}
