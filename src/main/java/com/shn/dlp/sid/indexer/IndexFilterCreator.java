package com.shn.dlp.sid.indexer;

import static com.shn.dlp.sid.indexer.IndexComponents.COMMON_TERMS_AND_ROW_DB_NAME;
import static com.shn.dlp.sid.indexer.IndexComponents.COMMON_TERMS_AND_ROW_MAP_NAME;
import static com.shn.dlp.sid.indexer.IndexComponents.COMMON_TERMS_FILTER_FILE_NAME;
import static com.shn.dlp.sid.indexer.IndexComponents.TERM_AND_ROW_FILTER_FILE_NAME;
import static com.shn.dlp.sid.indexer.IndexComponents.UNCOMMON_TERMS_FILTER_FILE_NAME;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.Set;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.BloomFilter;
import com.shn.dlp.sid.entries.RawTerm;
import com.shn.dlp.sid.entries.RawTermFunnel;
import com.shn.dlp.sid.entries.TermAndRow;
import com.shn.dlp.sid.entries.TermAndRowFunnel;
import com.shn.dlp.sid.entries.TermAndRowSerializer;
import com.shn.dlp.sid.security.CryptoException;
import com.shn.dlp.sid.util.SidConfiguration;
import com.shn.sid.search.SearchIndex;

public class IndexFilterCreator {
	private final SidConfiguration config;
	private final String indexName;
	private final String indexDirectory;
	private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	
	public IndexFilterCreator(SidConfiguration config, String indexName) {
		this.config = config;
		this.indexName = indexName;
		this.indexDirectory = config.getIndexerTempDirectory() + File.separator;
	}
	
	public void createFilters() throws CryptoException, IOException {
		createTermAndRowFilter();
		SearchIndex index = new SearchIndex(this.config, this.indexName, true);
		index.openIndex();	
		createTermsFilter(indexDirectory + UNCOMMON_TERMS_FILTER_FILE_NAME, 
				index.getUncommonEntriesCount(),
				index.getUncommonEntriesIterator());
		createTermsFilter(indexDirectory + COMMON_TERMS_FILTER_FILE_NAME, 
				index.getCommonEntriesCount(),
				index.getCommonEntriesIterator());	
		index.closeIndex();
	}
	
	private void createTermsFilter(String filterFileName, int numEntries, 
			Iterator<RawTerm> termsIterator) 
			throws IOException {
		
		double filterFPP = this.config.getBloomFilterFPP();
		
		LOG.info("For " + filterFileName + " number of entries is: " + String.format("%,d", numEntries));
		
		BloomFilter<RawTerm> filter = BloomFilter.create(RawTermFunnel.INSTANCE, numEntries, filterFPP);
		
		int count = 0;
		while (termsIterator.hasNext()) {
			filter.put(termsIterator.next());
			if (count%100000 == 0) {
				LOG.info("Adding entry# " + String.format("%,d", count) + " to " + filterFileName + " Filter.");
			}
			count++;
		}
		
		BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(filterFileName));
		filter.writeTo(out);
		
	}

	private void createTermAndRowFilter() throws CryptoException, IOException {
		File commonTermsAndRowDBfile = new File(indexDirectory + COMMON_TERMS_AND_ROW_DB_NAME);
		DB commonTermsAndRowDB = DBMaker.fileDB(commonTermsAndRowDBfile).readOnly().make();
		HTreeMap<TermAndRow, Integer> commonTermsAndRowMap = 
				commonTermsAndRowDB.hashMap(COMMON_TERMS_AND_ROW_MAP_NAME, 
				new TermAndRowSerializer(config), 
				Serializer.INTEGER,
				null);
		
		int numEntries = (int) commonTermsAndRowMap.mappingCount();
		Set<TermAndRow> entries = commonTermsAndRowMap.keySet();
		double filterFPP = this.config.getBloomFilterFPP();
		
		BloomFilter<TermAndRow> filter = BloomFilter.create(TermAndRowFunnel.INSTANCE,  numEntries, filterFPP);
		
		int count = 0;
		for (TermAndRow entry : entries) {
			filter.put(entry);
			if (count%100000 == 0) {
				LOG.info("Adding entry# " + String.format("%,d", count) + " to TermsAndRow Filter.");
			}
			count++;
		}
		String filterFileName = this.indexDirectory + TERM_AND_ROW_FILTER_FILE_NAME;
		BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(filterFileName));
		filter.writeTo(out);
		
		commonTermsAndRowDB.close();
	}
}
