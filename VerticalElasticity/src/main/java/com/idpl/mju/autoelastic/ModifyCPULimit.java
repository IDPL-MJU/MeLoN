package com.idpl.mju.autoelastic;

import java.io.BufferedWriter;
import java.io.FileWriter;

public class ModifyCPULimit extends Thread{
	private double cpuLimit;
	public ModifyCPULimit() {
		
	}
	public void run() {
		BufferedWriter fw;
		String text;
		String cpus;
		String[] temp;
		int index = 100;
		for(int i = 0 ; i < Resource.dockerCount ; i++) {
        	cpus = null;
			if(!Resource.dockerList[i].equals("cadvisor")) {
//				cpuLimit = (double)Resource.dockerCPULimit[i] / 100;
		        try{
		            // BufferedWriter 와 FileWriter를 조합하여 사용 (속도 향상)
		            fw = new BufferedWriter(new FileWriter("/home/hadoop/usage/adaptive/" + Resource.dockerList[i] + "_cpuUsage", true));
		            text = String.valueOf(Resource.dockerCPUUsage[i]);
		            // 파일안에 문자열 쓰기
		            fw.write(text + "\r\n");
		            fw.flush();
		            fw.close();
	
		            fw = new BufferedWriter(new FileWriter("/home/hadoop/usage/adaptive/" + Resource.dockerList[i] + "_cpuLimit", true));
		            text = String.valueOf(cpuLimit);
		            // 파일안에 문자열 쓰기
		            fw.write(text + "\r\n");
		            fw.flush();
		            
		            // 객체 닫기
		            fw.close();
		        }catch(Exception e){
		            e.printStackTrace();
		        }
		        if((Resource.dockerCPUUsage[i] / (Resource.pinnedCPUUsage[i] * 100)) * 100 < 60) {
		        	for(int j=0 ; j < Resource.hostCPUs ; j++) {
		        		if(Resource.pinnedCPUs[j] != null) {
			        		if(Resource.pinnedCPUs[j].equals(Resource.dockerList[i]) && Resource.pinnedCPUs[j] != "") {
				        		if(cpus != null) {
				        			cpus += ",";
					        		cpus += j;
				        		}
				        		if(cpus == null) {
				        			cpus = String.valueOf(j);
				        		}
			        		}
		        		}
		        	}
		        	temp = cpus.split(",");
		        	text = "";
		        	if(temp.length >= 1) {
			        	text = temp[temp.length-1];
			        	index = Integer.parseInt(text);
			        	for(int j=0; j < temp.length-2; j++) {
			        		if(text != "") {
				        		text += ",";
				        		text += temp[j];
			        		}
			        		if(text == "") {
				        		text += temp[j];
			        		}
			        	}
						UpdateResourceLimit.pinningCPU(i, text);
						System.out.println(Resource.dockerList[i] + " container CPU Pinning Update → " + text);
		        	}
				}
				if((Resource.dockerCPUUsage[i] / (Resource.pinnedCPUUsage[i] * 100)) * 100 > 65) {
		        	for(int j=0 ; j < Resource.hostCPUs ; j++) {
		        		if(Resource.pinnedCPUs[j] != null) {
		        			if(Resource.pinnedCPUs[j].equals(Resource.dockerList[i]) && Resource.pinnedCPUs[j] != "") {
			        			if(cpus != null) {
				        			cpus += ",";
					        		cpus += j;
				        		}
				        		if(cpus == null) {
				        			cpus = String.valueOf(j);
				        		}
			        		}
		        		}
		        		if(Resource.pinnedCPUs[j] == "" || Resource.pinnedCPUs[j] == null) {
		        			index = j;
		        		}
		        	}
		        	if(index < 100) {
		        		cpus += "," + index;
			        	Resource.pinnedCPUs[index] = Resource.dockerList[index];
						UpdateResourceLimit.pinningCPU(i, cpus);
						System.out.println(Resource.dockerList[i] + " container CPU Pinning Update → " + cpus);
			        	index = 100;
		        	}
				}
		        
		        
//				if(Resource.dockerCPUUsage[i] < 65 && cpuLimit != 1 && Resource.dockerCPUUsage[i] > 25) {
//					UpdateResourceLimit.scalingCPU(i, cpuLimit - 0.25);
//					System.out.println(Resource.dockerList[i] + " container CPU Limit Update → " + (cpuLimit - 0.25) + " CPUs");
//				} 
//				else if(Resource.dockerCPUUsage[i] < 25 && cpuLimit > 1.5) {
//					UpdateResourceLimit.scalingCPU(i, cpuLimit - 0.5);
//					System.out.println(Resource.dockerList[i] + " container CPU Limit Update → " + (cpuLimit - 0.5) + " CPUs");
//				}
//				if( sum(Resource.dockerCPULimit) < (Resource.hostCPUs + 1) * 100) {
//					if((Resource.dockerCPUUsage[i] / (double) Resource.dockerCPULimit[i]) * 100 > 90 && Resource.hostCPUs + 1 >= cpuLimit + 1) {
//						UpdateResourceLimit.scalingCPU(i, cpuLimit + 1);
//						System.out.println(Resource.dockerList[i] + " container CPU Limit Update → " + (cpuLimit + 1) + " CPUs");
//					}else if((Resource.dockerCPUUsage[i] / (double) Resource.dockerCPULimit[i]) * 100 > 80 && Resource.hostCPUs + 1 >= cpuLimit + 0.5) {
//						UpdateResourceLimit.scalingCPU(i, cpuLimit + 0.5);
//						System.out.println(Resource.dockerList[i] + " container CPU Limit Update → " + (cpuLimit + 0.5) + " CPUs");
//					}else if((Resource.dockerCPUUsage[i] / (double) Resource.dockerCPULimit[i]) * 100 > 70 && Resource.hostCPUs + 1 >= cpuLimit + 0.25) {
//						UpdateResourceLimit.scalingCPU(i, cpuLimit + 0.25);
//						System.out.println(Resource.dockerList[i] + " container CPU Limit Update → " + (cpuLimit + 0.25) + " CPUs");
//					}
//				}
			}
		}
//		System.out.println("Total Limit CPUs : " + (double)sum(Resource.dockerCPULimit)/100);
	}
	
	public long sum(long[] list) {
		long sum = 0;
		
		for(int i = 0; i < list.length; i++) {
			sum += list[i];
		}
		
		return sum;
	}
}
