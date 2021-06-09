package com.idpl.mju.autoelastic;

public class AutoElastic {
	public static void main(String[] args) {
		String monitorDir = null;
		String[] stat = null;
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
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		checkUser = System.getProperty("user.name").toLowerCase().trim();
//		if ( checkUser != "root"){
//			System.out.println(checkUser);
//		    System.out.println("Root Account Require.\n");
//		    return;
//		}
		StatMonitor statMonitor = new StatMonitor(monitorDir);
		statMonitor.run();
		System.out.println("Monitor start!");
		ReadStats readStats = new ReadStats();
		readStats.run();
		ObserveFileModify.observeFileModified(monitorDir);
	}
}
