package com.shn.dlp.sid.indexer;

import static com.shn.dlp.sid.indexer.IndexComponents.TERM_AND_ROW_FILTER_FILE_NAME;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Set;
import java.util.concurrent.Callable;

import org.mapdb.HTreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.BloomFilter;
import com.shn.dlp.sid.entries.TermAndRow;
import com.shn.dlp.sid.entries.TermAndRowFunnel;
import com.shn.dlp.sid.util.SidConfiguration;

public class FilterCreatorWorker implements Callable<Boolean> {

	private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass()); 
	private SidConfiguration config;
	private final int shardNumber;
	private final HTreeMap<TermAndRow, Integer> commonTermsAndRowMap;
	
	
	
	public FilterCreatorWorker(SidConfiguration config, int shardNumber, HTreeMap<TermAndRow, Integer> termAndRowMap) {
		this.config = config;
		this.shardNumber = shardNumber;
		this.commonTermsAndRowMap = termAndRowMap;
	}



	@Override
	public Boolean call() {
		
		int numEntries = (int) this.commonTermsAndRowMap.mappingCount();
		Set<TermAndRow> entries = commonTermsAndRowMap.keySet();
		double filterFPP = this.config.getBloomFilterFPP();
		
		BloomFilter<TermAndRow> filter = BloomFilter.create(TermAndRowFunnel.INSTANCE,  numEntries, filterFPP);
		
		int count = 0;
		LOG.info("For shard# " + shardNumber + " adding " + 
				String.format("%,d", numEntries) + " entries to TermAndRow Filter.");
		for (TermAndRow entry : entries) {
			filter.put(entry);
			if (count%100000 == 0) {
				LOG.info("For shard# " + shardNumber + " Adding entry# " 
						+ String.format("%,d", count) + " to TermsAndRow Filter.");
			}
			count++;
		}
		String filterFileName = config.getIndexerTempDirectory() + File.separator + 
				TERM_AND_ROW_FILTER_FILE_NAME + "." + shardNumber;
		BufferedOutputStream out;
		try {
			out = new BufferedOutputStream(new FileOutputStream(filterFileName));
			filter.writeTo(out);
			out.close();
		} catch (IOException e) {
			LOG.error("Error writing Filter for shards# " + this.shardNumber, e);
			return false;
		}
		return true;
	}
}
