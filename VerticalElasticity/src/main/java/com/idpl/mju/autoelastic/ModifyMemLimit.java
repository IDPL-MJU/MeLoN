package com.idpl.mju.autoelastic;

import java.io.BufferedWriter;
import java.io.FileWriter;

public class ModifyMemLimit extends Thread{
	private int memLimit;
	public ModifyMemLimit() {
		
	}
	public void run() {
		BufferedWriter fw;
		String text;
		for(int i = 0 ; i < Resource.dockerCount ; i++) {
			if(!Resource.dockerList[i].equals("cadvisor")) {
				memLimit = (int) (Resource.dockerMemoryLimit[i] / (1024 *1024));
		        try{
		            // BufferedWriter 와 FileWriter를 조합하여 사용 (속도 향상)
		            fw = new BufferedWriter(new FileWriter("/home/hadoop/usage/adaptive/" + Resource.dockerList[i] + "_memUsage", true));
		            text = String.valueOf(Resource.dockerMemoryUsage[i]);
		            // 파일안에 문자열 쓰기
		            fw.write(text + "\r\n");
		            fw.flush();
		 
		            // 객체 닫기
		            fw.close();
		            
		            fw = new BufferedWriter(new FileWriter("/home/hadoop/usage/adaptive/" + Resource.dockerList[i] + "_memLimit", true));
		            text = String.valueOf(memLimit);
		            // 파일안에 문자열 쓰기
		            fw.write(text + "\r\n");
		            fw.flush();
		 
		            // 객체 닫기
		            fw.close();
		        }catch(Exception e){
		            e.printStackTrace();
		        }
				if(Resource.dockerMemoryUsage[i] < 30 && memLimit > 640) {
					UpdateResourceLimit.scalingMem(i, memLimit - 512);
					System.out.println(Resource.dockerList[i] + " container Mem Limit Update → " + (memLimit - 512) + "M");
				}else if(Resource.dockerMemoryUsage[i] < 50 && memLimit > 384 && Resource.dockerMemoryUsage[i] > 30){
					UpdateResourceLimit.scalingMem(i, memLimit - 256);
					System.out.println(Resource.dockerList[i] + " container Mem Limit Update → " + (memLimit - 256) + "M");
				}else if(Resource.dockerMemoryUsage[i] < 80 && memLimit != 128) {
					UpdateResourceLimit.scalingMem(i, memLimit - 128);
					System.out.println(Resource.dockerList[i] + " container Mem Limit Update → " + (memLimit - 128) + "M");
				}
				if(Resource.remainMemory > 768) {
					if(Resource.dockerMemoryUsage[i] > 95) {
						UpdateResourceLimit.scalingMem(i, memLimit + 512);
						System.out.println(Resource.dockerList[i] + " container Mem Limit Update → " + (memLimit + 512) + "M");
					}else if(Resource.dockerMemoryUsage[i] > 90) {
						UpdateResourceLimit.scalingMem(i, memLimit + 256);
						System.out.println(Resource.dockerList[i] + " container Mem Limit Update → " + (memLimit + 256) + "M");
					}else if(Resource.dockerMemoryUsage[i] > 85) {
						UpdateResourceLimit.scalingMem(i, memLimit + 128);
						System.out.println(Resource.dockerList[i] + " container Mem Limit Update → " + (memLimit + 128) + "M");
					}
				}
			}
		}
		System.out.println("Total Limit Mem : " + sum(Resource.dockerMemoryLimit)/(1024*1024) + "m");
	}
	
	public long sum(long[] list) {
		long sum = 0;
		
		for(int i = 0; i < list.length; i++) {
			sum += list[i];
		}
		
		return sum;
	}
}
