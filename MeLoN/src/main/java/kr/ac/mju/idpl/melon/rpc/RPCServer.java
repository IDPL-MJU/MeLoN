package kr.ac.mju.idpl.melon.rpc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

public class RPCServer extends Thread implements RPCProtocol {
	private

	@Override
	public ProtocolSignature getProtocolSignature(String arg0, long arg1, int arg2) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getProtocolVersion(String arg0, long arg1) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String heartBeat() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
	
	public void run(){
		try {
			RPC.setProtocolEngine(conf, protocol, engine);
		}
		RPCServer rpcServer = new RPCServer();
		Configuration conf = new Configuration();
		Server server = RPC.
	}

}
