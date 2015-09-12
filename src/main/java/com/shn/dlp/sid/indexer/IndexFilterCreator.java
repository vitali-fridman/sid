package com.shn.dlp.sid.indexer;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.mapdb.HTreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shn.dlp.sid.entries.TermAndRow;
import com.shn.dlp.sid.security.CryptoException;
import com.shn.dlp.sid.util.SidConfiguration;

public class IndexFilterCreator {
	private final SidConfiguration config;
	private final HTreeMap<TermAndRow, Integer>[] commonTermsAndRowMaps;
	private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	
	public IndexFilterCreator(SidConfiguration config, HTreeMap<TermAndRow, Integer>[] commonTermsAndRowMaps) {
		this.config = config;
		this.commonTermsAndRowMaps = commonTermsAndRowMaps;
	}
	
	public void createTermAndRowFilters() throws CryptoException, IOException {
		int numShards = this.commonTermsAndRowMaps.length;
		
		ExecutorService executor= Executors.newFixedThreadPool(config.getIndexerNumThreads());
		List<Future<Boolean>> results = new ArrayList<>();
		boolean[] completed = new boolean[numShards];
		
		
		for (int i=0; i<numShards; i++) {
			Callable<Boolean> worker =  new FilterCreatorWorker(this.config, i, commonTermsAndRowMaps[i]);
			Future<Boolean> result = executor.submit(worker);
			results.add(result);
		}
		
		boolean fail = false;
		int remaining = numShards;
		
		while(!fail && remaining > 0) {
			for (int i=0; i<numShards; i++) {
				Future<Boolean> future = results.get(i);
				try {
					if (!completed[i] && future.isDone()) {
						if (!future.get()) {
							fail = true;
							LOG.error("Filter worker for shard# " + i + " failed");
							break;
						} else {
							completed[i] = true;
							remaining--;
						}
					}
				} catch (InterruptedException | ExecutionException e) {
					fail = true;
					LOG.error("Indexer workers for shard# " + i + " threw exception", e); 
					break;						
				}
				
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {};
			}
		}
		
		if (!fail) {
			executor.shutdown();
		} else {
			executor.shutdownNow();
		}
		
		try {
			executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			LOG.error("Indexer was interrupted before normal completion, exiting", e);
			LogManager.shutdown();
			System.exit(-1);
		}
		
		if (fail) {
			LOG.error("One of the indexer workers failed, Exiting");
			LogManager.shutdown();
			System.exit(-1);
		}
	}
}
