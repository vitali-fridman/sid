package com.shn.sid.search;

import static com.shn.dlp.sid.indexer.IndexComponents.COMMON_TERMS_AND_ROW_DB_NAME;
import static com.shn.dlp.sid.indexer.IndexComponents.COMMON_TERMS_AND_ROW_MAP_NAME;
import static com.shn.dlp.sid.indexer.IndexComponents.COMMON_TERMS_DB_NAME;
import static com.shn.dlp.sid.indexer.IndexComponents.COMMON_TERMS_MAP_NAME;
import static com.shn.dlp.sid.indexer.IndexComponents.DESCRIPTOR_FILE_NAME;
import static com.shn.dlp.sid.indexer.IndexComponents.UNCOMMON_TERMS_DB_NAME;
import static com.shn.dlp.sid.indexer.IndexComponents.UNCOMMON_TERMS_MAP_NAME;
import static com.shn.dlp.sid.indexer.IndexComponents.UNCOMMON_TERMS_FILTER_FILE_NAME;
import static com.shn.dlp.sid.indexer.IndexComponents.COMMON_TERMS_FILTER_FILE_NAME;
import static com.shn.dlp.sid.indexer.IndexComponents.TERM_AND_ROW_FILTER_FILE_NAME;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterators;
import com.google.common.hash.BloomFilter;
import com.shn.dlp.sid.entries.CellRowAndColMask;
import com.shn.dlp.sid.entries.CellRowAndColMaskListSerializer;
import com.shn.dlp.sid.entries.RawTerm;
import com.shn.dlp.sid.entries.RawTermFunnel;
import com.shn.dlp.sid.entries.RawTermSerializer;
import com.shn.dlp.sid.entries.TermAndRow;
import com.shn.dlp.sid.entries.TermAndRowFunnel;
import com.shn.dlp.sid.entries.TermAndRowSerializer;
import com.shn.dlp.sid.entries.Token;
import com.shn.dlp.sid.indexer.IndexDescriptor;
import com.shn.dlp.sid.security.CryptoException;
import com.shn.dlp.sid.util.SidConfiguration;

public class SearchIndex {

	private final SidConfiguration config;
	private final String indexName;
	private IndexDescriptor descriptor;
	private final String indexDirectory;
	private int numShards;
	private DB commonTermsAndRowDB;
	private HTreeMap<TermAndRow, Integer> commonTermsAndRowMap;
	private BloomFilter<TermAndRow> commonTermsAndRowFilter;
	private DB[] uncommonTermsDBs;
	private ArrayList<HTreeMap<RawTerm, ArrayList<CellRowAndColMask>>>  unCommonTermsMaps;
	private ArrayList<BloomFilter<RawTerm>> uncommonTermsFilters;
	private DB[] commonTermsDBs;
	private ArrayList<HTreeMap<RawTerm, Integer>> commonTermsMaps;
	private ArrayList<BloomFilter<RawTerm>> commonTermsFilters;
	public enum TokenPresense {COMMON, UNCOMMON};
	private volatile boolean isOpen;


	public SearchIndex(SidConfiguration config, String indexName, boolean inTemporaryIndexDirectory) {
		this.config = config;
		this.indexName = indexName;
		if (!inTemporaryIndexDirectory) {
			this.indexDirectory = config.getIndexesDirectory() + File.separator + indexName;
		} else {
			this.indexDirectory = config.getIndexerTempDirectory();
		}
		this.isOpen = false;
	}

	public String getIndexName() {
		return indexName;
	}

	public boolean isOpen() {
		return this.isOpen;
	}

	public synchronized void openIndex() throws JsonParseException, JsonMappingException, IOException, CryptoException {
		if (this.isOpen) {
			throw new IllegalStateException("Index " + this.indexName + " is already open");
		}

		BufferedInputStream filterIn = null;
		try {

			File descriptorFile = new File(indexDirectory + File.separator + DESCRIPTOR_FILE_NAME);
			ObjectMapper mapper = new ObjectMapper();
			this.descriptor = mapper.readValue(descriptorFile, IndexDescriptor.class);
			this.numShards = descriptor.getNumShards();

			File commonTermsAndRowDBfile = new File(indexDirectory + File.separator+ COMMON_TERMS_AND_ROW_DB_NAME);
			this.commonTermsAndRowDB = DBMaker.fileDB(commonTermsAndRowDBfile).readOnly().make();
			this.commonTermsAndRowMap = this.commonTermsAndRowDB.hashMap(COMMON_TERMS_AND_ROW_MAP_NAME, 
					new TermAndRowSerializer(config), 
					Serializer.INTEGER,
					null);

			String termsAndRowFilterFile = indexDirectory + File.separator + TERM_AND_ROW_FILTER_FILE_NAME;

			filterIn = new BufferedInputStream(new FileInputStream(termsAndRowFilterFile));
			this.commonTermsAndRowFilter = BloomFilter.readFrom(filterIn, TermAndRowFunnel.INSTANCE);
			filterIn.close();
			filterIn =  null;

			this.uncommonTermsDBs = new DB[this.numShards];
			this.commonTermsDBs = new DB[this.numShards];

			this.unCommonTermsMaps = new ArrayList<HTreeMap<RawTerm, ArrayList<CellRowAndColMask>>>();
			this.commonTermsMaps = new ArrayList<HTreeMap<RawTerm, Integer>>();

			this.uncommonTermsFilters = new ArrayList<BloomFilter<RawTerm>>();
			this.commonTermsFilters = new ArrayList<BloomFilter<RawTerm>>();


			for (int i=0; i<this.numShards; i++) {
				File unCommonTermsDBfile = new File(this.indexDirectory + File.separator + 
						UNCOMMON_TERMS_DB_NAME + "." + i);
				this.uncommonTermsDBs[i] = DBMaker.fileDB(unCommonTermsDBfile).readOnly().make();
				this.unCommonTermsMaps.add(this.uncommonTermsDBs[i].hashMap(UNCOMMON_TERMS_MAP_NAME,
						new RawTermSerializer(this.config),
						new CellRowAndColMaskListSerializer(),
						null));

				String uncommonTermsFilterFile = this.indexDirectory + File.separator +
						UNCOMMON_TERMS_FILTER_FILE_NAME + "." + i;
				filterIn = new BufferedInputStream(new FileInputStream(uncommonTermsFilterFile));
				this.uncommonTermsFilters.add(BloomFilter.readFrom(filterIn, RawTermFunnel.INSTANCE));
				filterIn.close();
				filterIn = null;

				File allCommonTermsDBfile = new File(this.indexDirectory + File.separator +
						COMMON_TERMS_DB_NAME + "." + i);
				this.commonTermsDBs[i] = DBMaker.fileDB(allCommonTermsDBfile).readOnly().make();
				this.commonTermsMaps.add(this.commonTermsDBs[i].hashMap(COMMON_TERMS_MAP_NAME, 
						new RawTermSerializer(this.config), Serializer.INTEGER, null));
				
				String commonTermsFilterFile = this.indexDirectory + File.separator +
						COMMON_TERMS_FILTER_FILE_NAME + "." + i;
				filterIn = new BufferedInputStream(new FileInputStream(commonTermsFilterFile));
				this.commonTermsFilters.add(BloomFilter.readFrom(filterIn, RawTermFunnel.INSTANCE));
				filterIn.close();
				filterIn = null;
			}
			
		} finally {
			if (filterIn != null) {
				filterIn.close();
			}
		}
		this.isOpen = true;
	}

	public synchronized void closeIndex() {
		if (!this.isOpen) {
			throw new IllegalStateException("Index " + this.indexName + " is already closed");
		}
		this.isOpen = false;
		this.commonTermsAndRowDB.close();
		this.commonTermsAndRowDB = null;
		for (int i=0; i<this.numShards; i++) {
			this.uncommonTermsDBs[i].close();
			this.uncommonTermsDBs[i] = null;
			this.commonTermsDBs[i].close();
			this.uncommonTermsDBs[i] = null;
		}
	}

	public FirstSearchLookupResult firstSearch(Token token) {
		if (!this.isOpen) {
			throw new IllegalStateException("Index " + this.indexName + " is not open");
		}

		FirstSearchLookupResult result = new FirstSearchLookupResult();
		int shardNumber = calculateShard(token);
		HTreeMap<RawTerm, Integer> allCommonTermsMap = this.commonTermsMaps.get(shardNumber);
		BloomFilter<RawTerm> commonTermsFilter = this.commonTermsFilters.get(shardNumber);
		Integer commonTermColMask= null;
		if (commonTermsFilter.mightContain(token.getTerm())) {
			commonTermColMask = allCommonTermsMap.get(token.getTerm());
		}
		result.token = token;
		if (commonTermColMask != null) {
			result.presense = TokenPresense.COMMON;
			result.commonTermColumnMask = commonTermColMask;
		} else {
			HTreeMap<RawTerm, ArrayList<CellRowAndColMask>> uncommonTermsMap = this.unCommonTermsMaps.get(shardNumber);
			BloomFilter<RawTerm> uncommonTermsFilter = this.uncommonTermsFilters.get(shardNumber);
			ArrayList <CellRowAndColMask> rowAndColMask = null;
			if (uncommonTermsFilter.mightContain(token.getTerm())) {
				rowAndColMask = uncommonTermsMap.get(token.getTerm());
			}
			if (rowAndColMask != null) {
				result.presense = TokenPresense.UNCOMMON;
				result.cellRowAndColMask = rowAndColMask;
			} else {
				result = null;
			}
		}
		return result;
	}

	public boolean secondSearch(int row, Token token) {
		if (!this.isOpen) {
			throw new IllegalStateException("Index " + this.indexName + " is not open");
		}
		
		Integer colMask = null;
		TermAndRow termAndRow = new TermAndRow(token.getTerm(), row);
		
		if (this.commonTermsAndRowFilter.mightContain(termAndRow)) {
			colMask = commonTermsAndRowMap.get(new TermAndRow(token.getTerm(), row));
		}
		
		if (colMask != null) {
			return true;
		} else {
			return false;
		}
	}

	private int calculateShard(Token token) {
		if (!this.isOpen) {
			throw new IllegalStateException("Index " + this.indexName + " is not open");
		}

		RawTerm rt = token.getTerm();
		int hash = rt.hashCode();
		int positiveHash = (hash < 0 ? -hash : hash);
		int shardIndex = positiveHash / (Integer.MAX_VALUE / this.numShards);
		return shardIndex;
	}

	public class FirstSearchLookupResult {
		public TokenPresense getPresense() {
			return presense;
		}
		public void setPresense(TokenPresense presense) {
			this.presense = presense;
		}
		public int getCommonTermColumnMask() {
			return commonTermColumnMask;
		}
		public void setCommonTermColumnMask(int commonTermColumnMask) {
			this.commonTermColumnMask = commonTermColumnMask;
		}
		public ArrayList<CellRowAndColMask> getCellRowAndColMask() {
			return cellRowAndColMask;
		}
		public void setCellRowAndColMask(ArrayList<CellRowAndColMask> cellRowAndColMask) {
			this.cellRowAndColMask = cellRowAndColMask;
		}

		public Token getToken() {
			return token;
		}

		private TokenPresense presense;
		private int commonTermColumnMask;
		private ArrayList<CellRowAndColMask> cellRowAndColMask;
		private Token token;
	}

	public int getUncommonEntriesCount() {
		if (!this.isOpen) {
			throw new IllegalStateException("Index " + this.indexName + " is not open");
		}

		long totalCount = 0;
		for (HTreeMap<?, ?> map : unCommonTermsMaps) {
			totalCount += map.mappingCount();
		}
		return (int) totalCount;
	}

	public int getCommonEntriesCount() {
		if (!this.isOpen) {
			throw new IllegalStateException("Index " + this.indexName + " is not open");
		}

		long totalCount = 0;
		for (HTreeMap<?, ?> map : commonTermsMaps) {
			totalCount += map.mappingCount();
		}
		return (int) totalCount;
	}

	public Iterator<RawTerm> getUncommonEntriesIterator() {	
		if (!this.isOpen) {
			throw new IllegalStateException("Index " + this.indexName + " is not open");
		}

		Iterator<RawTerm> allIterator = Collections.emptyIterator();
		for (HTreeMap<RawTerm, ?> map : this.unCommonTermsMaps) {
			allIterator = Iterators.concat(allIterator, map.keySet().iterator());
		}
		return allIterator;	
	}

	public Iterator<RawTerm> getCommonEntriesIterator() {
		if (!this.isOpen) {
			throw new IllegalStateException("Index " + this.indexName + " is not open");
		}

		Iterator<RawTerm> allIterator = Collections.emptyIterator();
		for (HTreeMap<RawTerm, ?> map : this.commonTermsMaps) {
			allIterator = Iterators.concat(allIterator, map.keySet().iterator());
		}
		return allIterator;
	}
}
