package com.idpl.mju.autoelastic;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;

public class ReadStats extends Thread{
	private String[] dockerStats;
	public ReadStats() {
		dockerStats = null;
	}
	public void run() {
		while(true) {
			int line = getLine();
			try {
				dockerStats = readStatsLine(line);
				for(int i = 0 ; i < line; i++) {
					System.out.println(dockerStats[i]);
				}
				Thread.sleep(4000);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public int getLine() {
    	String command = "docker container ps -q";
        int line = 0;
        Runtime rt = Runtime.getRuntime();
        Process p = null;
        StringBuffer sb = new StringBuffer();
        try{
            p=rt.exec(command);
            BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String cl = null;
            while((cl=in.readLine())!=null){
                sb.append(cl);
                line++;
            }
            in.close();
        }catch(IOException e){
            e.printStackTrace();
        }
        return line;
	}
	public static String[] readStatsLine(int lineNum) throws IOException {
		RandomAccessFile file = new RandomAccessFile("/home/cjy/docker_stats/monitor","r");
		long fileSize = file.length();
		long pos = fileSize - lineNum;
		String[] line = null;
		for(int i=0; i<lineNum; i++) {
			while(true) {
				file.seek(pos); //파일 포인터 이동
				if(file.readByte()=='\n') { //해당 위치의 바이트를 읽어 \n 문자와 같은지 검사
					break;  //같으면 멈춤
			    }
				pos--;  //포인터 위치값 감소 (앞으로)
			}
			file.seek(pos+1);  //\n 문자를 찾은 다음으로 파일 포인터 이동
			line[i] = file.readLine();
		}
		return line;
	}
}
