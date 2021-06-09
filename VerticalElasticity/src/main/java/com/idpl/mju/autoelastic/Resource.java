package com.idpl.mju.autoelastic;

public class Resource {
	static int dockerCount;
	static String[] dockerList = new String[100];
	static long[] dockerMemory = new long[100];
	static double[] dockerMemoryUsage = new double[100];
	static double[] dockerCPUUsage = new double[100];
	static long[] dockerMemoryLimit = new long[100];
	static long[] dockerCPULimit = new long[100];
	static String[] pinnedCPUs = new String[100];
	static int[] pinnedCPUUsage = new int[100];
	static int hostCPUs;
	static long hostMemory;
	static double remainCPUs;
	static long remainMemory;
}
