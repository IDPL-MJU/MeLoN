package com.idpl.mju.autoelastic;

public class UpdateResourceLimit {

	public static void pinningCPU(int containerIndex, String setCPU) {
		Process p = null;
		String[] scaleCPU = {
        		"/bin/sh",
        		"-c",
        		"sudo docker update --cpuset-cpus=" + setCPU + " " + Resource.dockerList[containerIndex]};
        try{
        	p = Runtime.getRuntime().exec(scaleCPU);
        	p.waitFor();
        	p.destroy();
        }
        catch(Exception e){
        	e.printStackTrace();
        }
	}
	
	public static void scalingCPU(int containerIndex, double setCPU) {
		Process p = null;
		String[] scaleCPU = {
        		"/bin/sh",
        		"-c",
        		"sudo docker update --cpus=" + setCPU + " " + Resource.dockerList[containerIndex]};
        try{
        	p = Runtime.getRuntime().exec(scaleCPU);
        	p.waitFor();
        	p.destroy();
        }
        catch(Exception e){
        	e.printStackTrace();
        }
	}
	public static void scalingMem(int containerIndex, int setMem) {
		Process p = null;
		String[] scaleMem = {
        		"/bin/sh",
        		"-c",
        		"sudo docker update --memory=" + setMem + "m " + "--memory-swap=-1 " + Resource.dockerList[containerIndex]};
		try {
        	p = Runtime.getRuntime().exec(scaleMem);
        	p.waitFor();
        	p.destroy();
        }
        catch(Exception e){
        	e.printStackTrace();
        }
	}
}
