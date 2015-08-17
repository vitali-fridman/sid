package com.shn.dlp.sid.util;

public class MemoryInfo {
	
	private static final Runtime runtime = Runtime.getRuntime();
	
	public static long getAvailableMemory() {
		System.gc();
		System.gc();
		System.gc();
		return runtime.freeMemory() + runtime.maxMemory() - runtime.totalMemory();
	}

	public static long getAvailableMemoryNoGc() {
		return runtime.freeMemory() + runtime.maxMemory() - runtime.totalMemory();
	}
}
