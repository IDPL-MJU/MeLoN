package com.idpl.mju.autoelastic;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

public class GetDockerList extends Thread {
    private Process p;
	public GetDockerList() {
	}
	
	public void run(){
        String[] command = {
        		"/bin/sh",
        		"-c",
        		"curl -s localhost:8080/api/v1.3/docker | jq -r 'to_entries[].value.aliases[0]'"
        };
        try{
        	p = Runtime.getRuntime().exec(command);
//	        p.waitFor();
	        InputStream in = p.getInputStream();
	        InputStreamReader isr = new InputStreamReader(in);
	        BufferedReader br = new BufferedReader(isr);
	        int count = 0;
	        String line = null;
	        while((line=br.readLine())!= null) {
//	        	System.out.println(line);
	        	Resource.dockerList[count] = line;
	        	count++;
	        }
	        Resource.dockerCount = count;
	        p.waitFor();
	        p.destroy();
	        in.close(); 
        }
        catch(Exception e){
        	e.printStackTrace();
        } 
	}
}
