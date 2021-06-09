package com.idpl.mju.autoelastic;

public class AutoElastic {
	
	public static void main(String[] args) {
//		String monitorDir = null;
//		String[] stat = null;
//		String checkUser = null;
//		Process p = null;
//		if(args == null) {
//			System.out.println("Set Directory for Docker Stat Monitoring.");
//			System.out.println("Usage: java -jar xxx.jar SET_YOUR_DIRECTORY");
//			return;
//		}else if(args.length == 1){
//			monitorDir = args[0];
//		}else {
//			System.out.println("Set Just One Directory.");
//			System.out.println("Usage: java -jar xxx.jar SET_YOUR_DIRECTORY");
//			return;
//		}
		
//		//root 계정 검사
//		try {
//			p = Runtime.getRuntime().exec("whoami");
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		checkUser = System.getProperty("user.name").toLowerCase().trim();
//		if ( checkUser != "root"){
//			System.out.println(checkUser);
//		    System.out.println("Root Account Require.\n");
//		    return;
//		}
		GetDockerList getDockerList = new GetDockerList();
		GetHostResourceData.getHostResource();
		GetHostResourceData.getRemainResource();
		GetDockerResourceData getDockerResourceData = new GetDockerResourceData();
		ModifyCPULimit modifyCPULimit = new ModifyCPULimit();
		ModifyMemLimit modifyMemLimit = new ModifyMemLimit();
		System.out.println("Monitor start!");
		try {
			getDockerList.run();
			GetHostResourceData.getRemainResource();
			GetHostResourceData.initalizingValue();
			while(true) {
				getDockerResourceData.run();
				GetHostResourceData.getRemainResource();
				modifyCPULimit.run();
				modifyMemLimit.run();
				Thread.sleep(3000);
			}
		}catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
		}
	}
}
