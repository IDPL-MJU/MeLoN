
package com.idpl.mju.autoelastic;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class GetDockerResourceData extends Thread{
	
	private Process p;

	public GetDockerResourceData() {
		
	}
	
	public void run() {
		InitalizeCPUs();
        for(int i = 0; i < Resource.dockerCount ; i++) {
        	GetCPUPinning(i);
        	GetCPULimit(i);
    		GetMemoryLimit(i);
        	CalculateCPUUsage(p, i);
        }
        CalculateMemoryUsage(p);
	}
	private void InitalizeCPUs() {
		for(int i=0; i < Resource.hostCPUs ; i++) {
        	Resource.pinnedCPUs[i] = "";
        }
	}

	private void GetCPUPinning(int index) {
		String[] commandResource = {
        		"/bin/sh",
        		"-c",
        		"sudo docker container inspect " + Resource.dockerList[index] +" | jq 'to_entries[].value.HostConfig.CpusetCpus'"
        };
		if(!Resource.dockerList[index].equals("cadvisor")) {
			int count = 0;
			int length;
			String cpus;
			int temp2;
			String[] temp;
	        try{
	        	p = Runtime.getRuntime().exec(commandResource);
		        InputStream in = p.getInputStream();
		        InputStreamReader isr = new InputStreamReader(in);
		        BufferedReader br = new BufferedReader(isr);
		        String line = null;
		        while((line=br.readLine())!= null) {
		        	length = line.length();
		        	cpus = line.substring(1, length-1);
		        	System.out.println(Resource.dockerList[index] + " : " + cpus);
		        	temp = cpus.split(",");
		        	count = temp.length;
		        	Resource.pinnedCPUUsage[index] = count;
		        	for(int i=0; i < temp.length; i++) {
		        		temp2 = Integer.parseInt(temp[i]);
		        		Resource.pinnedCPUs[temp2] = Resource.dockerList[index];
		        	}
		        }
		        p.waitFor();
		        p.destroy();
		        in.close(); 
	        }
	        catch(Exception e){
	        	e.printStackTrace();
	        }
		}
	}
	private void GetCPULimit(int index) {
		String[] commandResource = {
        		"/bin/sh",
        		"-c",
        		"sudo docker container inspect " + Resource.dockerList[index] +" | jq 'to_entries[].value.HostConfig.NanoCpus'"
        };
        try{
        	p = Runtime.getRuntime().exec(commandResource);
	        InputStream in = p.getInputStream();
	        InputStreamReader isr = new InputStreamReader(in);
	        BufferedReader br = new BufferedReader(isr);
	        String line = null;
	        while((line=br.readLine())!= null) {
	        	Resource.dockerCPULimit[index] = Long.valueOf(line)/10000000;
	        }
	        p.waitFor();
	        p.destroy();
	        in.close(); 
        }
        catch(Exception e){
        	e.printStackTrace();
        }
	}
	private void GetMemoryLimit(int index) {
		String[] commandResource = {
        		"/bin/sh",
        		"-c",
        		"curl -s localhost:8080/api/v2.0/spec/" + Resource.dockerList[index] + "?type=docker | jq -r 'to_entries[].value.memory.limit'"
        };
        try{
        	p = Runtime.getRuntime().exec(commandResource);
	        InputStream in = p.getInputStream();
	        InputStreamReader isr = new InputStreamReader(in);
	        BufferedReader br = new BufferedReader(isr);
	        String line = null;
	        while((line=br.readLine())!= null) {
	        	Resource.dockerMemoryLimit[index] = Long.valueOf(line);
	        }
		    p.waitFor();
	        p.destroy();
	        in.close(); 
        }
        catch(Exception e){
        	e.printStackTrace();
        }
	}

	public void CalculateMemoryUsage(Process p) {
		String[] commandMemory = {
        		"/bin/sh",
        		"-c",
        		"curl -s localhost:8080/api/v1.3/docker | jq -r 'to_entries[].value.stats[-1].memory.usage'"
        };
        try{
        	p = Runtime.getRuntime().exec(commandMemory);
	        InputStream in = p.getInputStream();
	        InputStreamReader isr = new InputStreamReader(in);
	        BufferedReader br = new BufferedReader(isr);
	        int count = 0;
	        String line = null;
	        while((line=br.readLine())!= null) {
	        	Resource.dockerMemory[count] = Long.valueOf(line);
	        	Resource.dockerMemoryUsage[count] = ((double)Resource.dockerMemory[count] / Resource.dockerMemoryLimit[count]) * 100;
//	        	System.out.println(Resource.dockerMemoryUsage[count]);
	        	count++;
	        }
		    p.waitFor();
	        p.destroy();
	        in.close(); 
        }
        catch(Exception e){
        	e.printStackTrace();
        } 
	}
	
	public void CalculateCPUUsage(Process p, int index) {
        String[] commandCPUs = {
        		"/bin/sh",
        		"-c",
        		"curl -s localhost:8080/api/v2.0/stats/"+ Resource.dockerList[index] +"?type=docker | jq -r 'to_entries[].value[-1,-2].cpu.usage.total, to_entries[].value[-1,-2].timestamp'"
        };
        try{
        	p = Runtime.getRuntime().exec(commandCPUs);
	        InputStream in = p.getInputStream();
	        InputStreamReader isr = new InputStreamReader(in);
	        BufferedReader br = new BufferedReader(isr);
	        
	        String line = null;
	        int i = 0;
	        
	        long[] cpuTotal = new long[2];
	        LocalDateTime[] timeStamp = new LocalDateTime[2];
	        
	        while((line=br.readLine())!= null) {
//	        	System.out.println(line);
	        	if(i < 2) {
	        		cpuTotal[i] = Long.parseLong(line);
	        		i++;
	        	}else {
	        		timeStamp[i-2] = LocalDateTime.parse(line.replace("Z", ""));
	        		i++;
	        	}
	        }
	        Resource.dockerCPUUsage[index] = ((double)((cpuTotal[0]-cpuTotal[1]) / (double)ChronoUnit.NANOS.between(timeStamp[1], timeStamp[0]))*100);
//	        System.out.println(Resource.dockerCPUUsage[index]);
		    p.waitFor();
	        p.destroy();
	        in.close(); 
        }
        catch(Exception e){
        	e.printStackTrace();
        }
	}
}
