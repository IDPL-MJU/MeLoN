package com.idpl.mju.autoelastic;

public class StatMonitor extends Thread {
	private String targetDir = null;
	
	public StatMonitor(String monitorDir) {
		targetDir = monitorDir;
	}
	
	public void run(){
        Process p = null;
        try{
	        if( p == null)
	        p = Runtime.getRuntime().exec("/home/cjy/stats.sh");
	        // 프로세스 작업이 끝날때까지 기다린다.
	        p.waitFor(); 
        }
        catch(Exception e){
        	e.printStackTrace();
        } 
	}
}
