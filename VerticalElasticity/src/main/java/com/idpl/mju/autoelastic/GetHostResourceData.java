package com.idpl.mju.autoelastic;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

public class GetHostResourceData {
	public static void getRemainResource() {
		Process p = null;
		String[] commandCPU = {
	    		"/bin/sh",
	    		"-c",
	    		"mpstat | tail -1 | awk '{print 100-$NF}'"
	    };
		String[] commandMemory = {
	    		"/bin/sh",
	    		"-c",
	    		"free -m | awk '{print $7}' | sed -n '2p'"
	    };
		try {
			p = Runtime.getRuntime().exec(commandCPU);
		    InputStream in = p.getInputStream();
		    InputStreamReader isr = new InputStreamReader(in);
		    BufferedReader br = new BufferedReader(isr);
		    String line = null;
		    while((line=br.readLine())!= null) {
		    	Resource.remainCPUs = 100 - Double.valueOf(line);
//	        	System.out.println(Resource.remainCPUs);
		    }
		    p.destroy();
		    in.close(); 
		    
			p = Runtime.getRuntime().exec(commandMemory);
		    in = p.getInputStream();
		    isr = new InputStreamReader(in);
		    br = new BufferedReader(isr);
		    line = null;
		    while((line=br.readLine())!= null) {
		    	Resource.remainMemory = Long.valueOf(line);
//	        	System.out.println(Resource.remainMemory);
		    }
		    br.close();
		    p.waitFor();
		    p.destroy();
		    in.close(); 
		}
	    catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void getHostResource() {
		Process p = null;
		String[] commandCPU = {
	    		"/bin/sh",
	    		"-c",
	    		"grep -c processor /proc/cpuinfo"
	    };
		String[] commandMemory = {
	    		"/bin/sh",
	    		"-c",
	    		"free -b | awk '{print $2}' | sed -n '2p'"
	    };
		try {
			p = Runtime.getRuntime().exec(commandCPU);
		    InputStream in = p.getInputStream();
		    InputStreamReader isr = new InputStreamReader(in);
		    BufferedReader br = new BufferedReader(isr);
		    String line = null;
		    while((line=br.readLine())!= null) {
		    	Resource.hostCPUs = Integer.valueOf(line);
//	        	System.out.println(Resource.hostCPUs);
		    }
		    p.waitFor();
		    p.destroy();
		    in.close(); 
		    
			p = Runtime.getRuntime().exec(commandMemory);
		    in = p.getInputStream();
		    isr = new InputStreamReader(in);
		    br = new BufferedReader(isr);
		    line = null;
		    while((line=br.readLine())!= null) {
		    	Resource.hostMemory = Long.valueOf(line);
//	        	System.out.println(Resource.hostMemory);
		    }
		    br.close();
		    p.waitFor();
		    p.destroy();
		    in.close(); 
		}
	    catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void initalizingValue() {
		for(int i=0; i < Resource.hostCPUs ; i++) {
			Resource.pinnedCPUs[i] = "";
		}
	}
}
