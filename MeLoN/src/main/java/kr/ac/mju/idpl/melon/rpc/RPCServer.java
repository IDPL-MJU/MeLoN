package kr.ac.mju.idpl.melon.rpc;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import kr.ac.mju.idpl.melon.ExecutorExecutionResult;

import kr.ac.mju.idpl.melon.MeLoN_Session;
import kr.ac.mju.idpl.melon.MeLoN_Task;


public class RPCServer extends Thread implements RPCProtocol {
	private static final Logger LOG = LoggerFactory.getLogger(RPCServer.class);

	// for communication with MeLoN_TaskExecutor
	private Random RANDOM_NUMBER_GENERATOR = new Random();
	private String rpcAddress;
	private int rpcPort;
	Configuration yarnConf;
	private Server server;

	// application information
	private String appId;
	private long startTime;
	private long finishTime;
	
	private MeLoN_Session session;
	
	
	private Set<String> registeredTasks = new HashSet<>();
	
	private Map<String, ExecutorExecutionResult> results = new ConcurrentHashMap<>();

	public RPCServer(Builder builder) {
		this.rpcAddress = builder.hostname;
		this.rpcPort = 10000 + RANDOM_NUMBER_GENERATOR.nextInt(5000) + 1;
		this.yarnConf = builder.yarnConf;
	}
	
	public void setNewSession(MeLoN_Session session) {
		this.session = session;
	}

	
	public Map<String, ExecutorExecutionResult> getExecutorExecutionResults() {
		return results;
	}


	public void run() {
		LOG.info("Running RPCServer ...");
		try {
			LOG.info("Building RPCServer ...");
			server = new RPC.Builder(yarnConf).setProtocol(RPCProtocol.class).setInstance(this)
					.setBindAddress(rpcAddress).setPort(rpcPort).build();
			LOG.info("Starting RPCServer ...");
			server.start();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash)
			throws IOException {
		return ProtocolSignature.getProtocolSignature(this, protocol, clientVersion, clientMethodsHash);
	}

	@Override
	public long getProtocolVersion(String protocol, long version) throws IOException {
		return RPCProtocol.versionID;
	}

	@Override
	public String getClusterSpec() throws IOException, YarnException {
		ObjectMapper objectMapper = new ObjectMapper();
		return objectMapper.writeValueAsString(session.getClusterSpec());
	}

	@Override
	public String registerWorkerSpec(String taskId, String spec) throws IOException, YarnException {
		int idx = taskId.indexOf(":");
		MeLoN_Task task = session.getTask(taskId.substring(0, idx), taskId.substring(idx + 1));
		if (task.getHost() == null) {
			LOG.info("Received cluster spec registration request from task " + taskId + " with spec: " + spec);
			task.setHostPort(spec);
			registeredTasks.add(taskId);
		}

		int totalTasks = session.getTotalTasks();
		if (registeredTasks.size() == totalTasks) {
			LOG.info("All " + totalTasks + " tasks registered.");
			return getClusterSpec();
		} else {
			LOG.info(registeredTasks.size() + "/" + totalTasks + " tasks registered.");
			return null;
		}
	}

	@Override
	public String registerExecutionResult(int exitCode, String host, String device, String fraction, String jobName,
			int taskIndex, long executorExecutionTime, long processExecutionTime) throws Exception {
		LOG.info("Received result registration request with exit code " + exitCode + " from " + jobName + " " + taskIndex);
		ExecutorExecutionResult result = new ExecutorExecutionResult(exitCode, host, device, fraction, jobName, taskIndex, executorExecutionTime, processExecutionTime);
		results.put(result.getTaskId(), result);
		LOG.info("Result put");
//		MeLoN_Task task = session.getTask(result.getJobName(), String.valueOf(result.getTaskIndex()));
		return "RECEIVED";
	}

	public int getRpcPort() {
		return rpcPort;
	}
	
	public void reset() {
		registeredTasks = new HashSet<>();
	}

	public static class Builder {
//		private String jvmArgs;
//		private Configuration melonConf;
		private String hostname;
		private Configuration yarnConf;

		public RPCServer build() {
			return new RPCServer(this);
		}

//		public Builder setTaskExecutorJVMArgs(String jvmArgs) {
//			this.jvmArgs = jvmArgs;
//			return this;
//		}
//
//		public Builder setMelonConf(Configuration melonConf) {
//			this.melonConf = melonConf;
//			return this;
//		}

		public Builder setHostname(String hostname) {
			this.hostname = hostname;
			return this;
		}

		public Builder setYarnConf(Configuration yarnConf) {
			this.yarnConf = yarnConf;
			return this;
		}
	}

}
