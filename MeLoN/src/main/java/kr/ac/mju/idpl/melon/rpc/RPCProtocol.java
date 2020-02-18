package kr.ac.mju.idpl.melon.rpc;

import java.io.IOException;

import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSelector;

import kr.ac.mju.idpl.melon.measure.ExecutorExecutionResult;

@TokenInfo(ClientToAMTokenSelector.class)
@ProtocolInfo(protocolName = "kr.ac.mju.idpl.melon.rpc.RPCProtocol", protocolVersion = 0)

public interface RPCProtocol extends VersionedProtocol {
	
	public long versionID = 0;
	
	public String registerWorkerSpec(String taskId, String spec) throws IOException, YarnException;
	
	public String getClusterSpec() throws IOException, YarnException;
	
	public String registerExecutionResult(int exitCode, String host, String device, String fraction, String jobName,
			int taskIndex, long executorExecutionTime, long processExecutionTime) throws Exception;
	
}
