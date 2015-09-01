package com.shn.sid.search;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shn.dlp.sid.entries.CellRowAndColMask;
import com.shn.dlp.sid.entries.RawTerm;
import com.shn.dlp.sid.entries.TermAndRow;
import com.shn.dlp.sid.entries.Token;
import com.shn.dlp.sid.indexer.CryptoFileIndexer;
import com.shn.dlp.sid.indexer.CryptoFileIndexerWorker;
import com.shn.dlp.sid.indexer.IndexDescriptor;
import com.shn.dlp.sid.util.SidConfiguration;

public class SearchIndex {
	
	private final SidConfiguration config;
	private final String indexName;
	private IndexDescriptor descriptor;
	private final String indexDirectory;
	private int numShards;
	private DB commonTermsDB;
	private HTreeMap<TermAndRow, Integer> commonTermsMap;
	private DB[] uncommonTermsDBs;
	private HTreeMap<RawTerm, ArrayList<CellRowAndColMask>>[]  unCommonTermsMaps;
	private DB[] allCmmonTermsDBs;
	private HTreeMap<RawTerm, Integer>[] allCommonTermsMaps;
	
	
	public SearchIndex(SidConfiguration config, String indexName) {
		this.config = config;
		this.indexName = indexName;
		this.indexDirectory = config.getIndexesDirectory() + File.separator + indexName;
	}

	public String getIndexName() {
		return indexName;
	}
	
	public void openIndex() throws JsonParseException, JsonMappingException, IOException {
		File descriptorFile = new File(indexDirectory + File.separator + CryptoFileIndexer.DESCRIPTOR_FILE_NAME);
		ObjectMapper mapper = new ObjectMapper();
		this.descriptor = mapper.readValue(descriptorFile, IndexDescriptor.class);
		this.numShards = descriptor.getNumShards();
		File commonTermsDBfile = new File(indexDirectory + File.separator+ CryptoFileIndexer.COMMON_TERMS_DB_NAME);
		this.commonTermsDB = DBMaker.fileDB(commonTermsDBfile).fileMmapEnable().readOnly().make();
		this.commonTermsMap = this.commonTermsDB.hashMap(CryptoFileIndexer.COMMON_TERMS_MAP_NAME);
		
		for (int i=0; i<this.numShards; i++) {
			File unCommonTermsDBfile = new File(indexDirectory + File.separator + 
					CryptoFileIndexerWorker.UNCOMMON_TERMS_DB_NAME + "." + i);
			this.uncommonTermsDBs[i] = DBMaker.fileDB(unCommonTermsDBfile).fileMmapEnable().readOnly().make();
		}
		
	}
	
	public Token.Presense lookupPresense(Token token) {
		
	}
	
	

}
