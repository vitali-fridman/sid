package com.shn.dlp.sid.util;

import java.lang.invoke.MethodHandles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeriodicGarbageCollector extends Thread {

	private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());  
	private final int period;
	
	public PeriodicGarbageCollector(int period) {
		this.period = period;
	}
	
	@Override
	public void run() {
		while (true) {
            try {
                sleep(period);
                LOG.info("Available memory: " + String.format("%,d",MemoryInfo.getAvailableMemory()/(1024*1024)) + "MB");
            } catch (InterruptedException e) {}
        }
	}
	
}
