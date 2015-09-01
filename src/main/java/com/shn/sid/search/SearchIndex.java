package com.shn.sid.search;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shn.dlp.sid.entries.CellRowAndColMask;
import com.shn.dlp.sid.entries.CellRowAndColMaskListSerializer;
import com.shn.dlp.sid.entries.RawTerm;
import com.shn.dlp.sid.entries.RawTermSerializer;
import com.shn.dlp.sid.entries.TermAndRow;
import com.shn.dlp.sid.entries.TermAndRowSerializer;
import com.shn.dlp.sid.entries.Token;
import com.shn.dlp.sid.indexer.CryptoFileIndexer;
import com.shn.dlp.sid.indexer.CryptoFileIndexerWorker;
import com.shn.dlp.sid.indexer.IndexDescriptor;
import com.shn.dlp.sid.security.CryptoException;
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
	
	public void openIndex() throws JsonParseException, JsonMappingException, IOException, CryptoException {
		File descriptorFile = new File(indexDirectory + File.separator + CryptoFileIndexer.DESCRIPTOR_FILE_NAME);
		ObjectMapper mapper = new ObjectMapper();
		this.descriptor = mapper.readValue(descriptorFile, IndexDescriptor.class);
		this.numShards = descriptor.getNumShards();
		File commonTermsDBfile = new File(indexDirectory + File.separator+ CryptoFileIndexer.COMMON_TERMS_DB_NAME);
		this.commonTermsDB = DBMaker.fileDB(commonTermsDBfile).fileMmapEnable().readOnly().make();
		this.commonTermsMap = this.commonTermsDB.hashMap(CryptoFileIndexer.COMMON_TERMS_MAP_NAME, 
				new TermAndRowSerializer(config), 
				Serializer.INTEGER,
				null);
		
		for (int i=0; i<this.numShards; i++) {
			File unCommonTermsDBfile = new File(this.indexDirectory + File.separator + 
					CryptoFileIndexerWorker.UNCOMMON_TERMS_DB_NAME + "." + i);
			this.uncommonTermsDBs[i] = DBMaker.fileDB(unCommonTermsDBfile).fileMmapEnable().readOnly().make();
			this.unCommonTermsMaps[i] = this.uncommonTermsDBs[i].hashMap(CryptoFileIndexerWorker.UNCOMMON_TERMS_MAP_NAME,
					new RawTermSerializer(this.config),
					new CellRowAndColMaskListSerializer(),
					null);
			File allCommonTermsDBfile = new File(this.indexDirectory + File.separator +
					CryptoFileIndexerWorker.ALL_COMMON_TERMS_DB_NAME + "." + i);
			this.allCmmonTermsDBs[i] = DBMaker.fileDB(allCommonTermsDBfile).fileMmapEnable().readOnly().make();
			this.allCommonTermsMaps[i] = this.allCmmonTermsDBs[i].hashMap(CryptoFileIndexerWorker.ALL_COMMON_TERMS_MAP_NAME, 
					new RawTermSerializer(this.config), Serializer.INTEGER, null);
		}
		
	}
	
	public void closeIndex() {
		this.commonTermsDB.close();
		for (int i=0; i<this.numShards; i++) {
			this.uncommonTermsDBs[i].close();
			this.allCmmonTermsDBs[i].close();
		}
	}
	
	public Token.Presense lookupPresense(Token token) {
		int shard = calculateShard(token);
		this.allCommonTermsMaps[shard].containsKey(token.getTerm());
	}

	private int calculateShard(Token token) {
		RawTerm rt = token.getTerm();
		int hash = rt.hashCode();
		int positiveHash = (hash < 0 ? -hash : hash);
		int shardIndex = positiveHash / (Integer.MAX_VALUE / this.numShards);
		return shardIndex;
	}
	
	

}
