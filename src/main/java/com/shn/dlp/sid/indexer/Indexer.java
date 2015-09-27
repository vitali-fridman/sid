package com.shn.dlp.sid.indexer;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.OptionHandlerFilter;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shn.dlp.sid.entries.TermAndRow;
import com.shn.dlp.sid.entries.TermAndRowSerializer;
import com.shn.dlp.sid.security.Crypter;
import com.shn.dlp.sid.security.CryptoException;
import com.shn.dlp.sid.util.PeriodicGarbageCollector;
import com.shn.dlp.sid.util.SidConfiguration;
import static com.shn.dlp.sid.indexer.IndexComponents.COMMON_TERMS_AND_ROW_DB_NAME;
import static com.shn.dlp.sid.indexer.IndexComponents.COMMON_TERMS_AND_ROW_MAP_NAME;
import static com.shn.dlp.sid.indexer.IndexComponents.DESCRIPTOR_FILE_NAME;

public class Indexer {
	private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());  

	private final SidConfiguration config;
	private final String indexName;
	private final String fileToIndex;
	private DB[] commonTermsAndRowDBs;
	private HTreeMap<TermAndRow, Integer>[] commonTermsAndRowMaps;
	private int formatVersion;
	private String algorithm;
	private int fullTermLength;
	private int retainedTermLength;
	private int numRows;
	private int numColumns;
	private int numShards;


	public  Indexer(SidConfiguration config, String indexName, String fileToIndex) {
		this.config = config;
		this.indexName = indexName;
		this.fileToIndex = fileToIndex;
	}
		
	public boolean index() throws IOException, CryptoException {
		
		if (!Files.exists(Paths.get(this.config.getIndexesDirectory()))) {
			LOG.error("Indexes directory does not exist");
			return false;
		}

		String tempDirectory = this.config.getIndexerTempDirectory();
		FileUtils.deleteQuietly(new File(tempDirectory));
		FileUtils.forceMkdir(new File(tempDirectory));

		PeriodicGarbageCollector pgc = new PeriodicGarbageCollector(this.config.getIndexerGcPeriod());
		pgc.setDaemon(true);
		pgc.start();

		if (!readCryptoFileHeader()) {
			return false;
		}
		
		createCommonTermsDBandMaps();

		
		ExecutorService executor= Executors.newFixedThreadPool(config.getIndexerNumThreads());
		List<Future<Boolean>> results = new ArrayList<>();
		boolean[] completed = new boolean[numShards];

		for (int i = 0; i < this.numShards; i++) {
			Callable<Boolean> worker = new IndexerWorker(this.config, this.fileToIndex, 
					this.numRows, this.numShards, this.commonTermsAndRowMaps, 
					this.fullTermLength, this.retainedTermLength, i);
			Future<Boolean> result = executor.submit(worker);
			results.add(result);
		}

		boolean fail = false;
		int remaning  = this.numShards;
		while(!fail && remaning > 0) {
			for (int i=0; i<this.numShards; i++) {
				Future<Boolean> future = results.get(i);
				try {
					if (!completed[i] && future. isDone()) {
						if (!future.get()) {
							fail = true;
							LOG.error("Indexer workers for shard# " + i + " failed");
							break;
						} else {
							completed[i] = true;
							remaning--;
						}
					}
				} catch (InterruptedException | ExecutionException e) {
					fail = true;
					LOG.error("Indexer workers for shard# " + i + " threw exception", e); 
					break;
				}
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {};
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
			return false;
		}
		
		if (fail) {
			LOG.error("One of the indexer workers failed, Exiting");
			return false;
		}

		commitCommonTermsAndRowDBs();
		
		writeDescriptorFile(tempDirectory);
		
		IndexFilterCreator filterCreator = new IndexFilterCreator(config, commonTermsAndRowMaps);
		filterCreator.createTermAndRowFilters();
		
		closeCommonTermsAndRowDBs();
		
		String indexDirectory = this.config.getIndexesDirectory() + File.separator + this.indexName;
		FileUtils.deleteQuietly(new File(indexDirectory));
		FileUtils.moveDirectory(new File(tempDirectory), new File(indexDirectory));
		
		return true;
	}

	private void writeDescriptorFile(String indexDirectory) 
			throws JsonGenerationException, JsonMappingException, IOException {
		IndexDescriptor descriptor = new IndexDescriptor(this.formatVersion, this.algorithm, this.fullTermLength,
				this.retainedTermLength,  this.numRows, this.numColumns, this.numShards);
		ObjectMapper mapper = new ObjectMapper();
		mapper.writeValue(new File(indexDirectory + File.separator + DESCRIPTOR_FILE_NAME), descriptor);
	}

	private  void commitCommonTermsAndRowDBs() {
		for (int i=0; i< this.numShards; i++) {
			this.commonTermsAndRowDBs[i].commit();
		}
	}

	private void closeCommonTermsAndRowDBs() {
		for (int i=0; i< this.numShards; i++) {
			this.commonTermsAndRowDBs[i].close();
		}
	}
	
	private void createCommonTermsDBandMaps() throws CryptoException {

		this.commonTermsAndRowDBs = new DB[numShards];
		this.commonTermsAndRowMaps = new HTreeMap[numShards];
		
		for (int i=0; i<this.numShards; i++) {
			File commonTermsDBfile = new File(this.config.getIndexerTempDirectory() + "/" + COMMON_TERMS_AND_ROW_DB_NAME + "." + i);
			this.commonTermsAndRowDBs[i] = DBMaker.fileDB(commonTermsDBfile).
					transactionDisable().
					closeOnJvmShutdown().  
					fileMmapEnable().
					asyncWriteEnable().
					asyncWriteFlushDelay(this.config.getIndexerAsyncWriteFlushDelay()).
					asyncWriteQueueSize(this.config.getIndexerAsyncWriteQueueSize()).
					allocateStartSize(this.config.getIndexerMapDbSizeIncrement()).
					allocateIncrement(this.config.getIndexerMapDbSizeIncrement()).
					metricsEnable().
					make();
		
			this.commonTermsAndRowMaps[i] =
					this.commonTermsAndRowDBs[i].hashMapCreate(COMMON_TERMS_AND_ROW_MAP_NAME).
					valueSerializer(Serializer.INTEGER).
					keySerializer(new TermAndRowSerializer(this.retainedTermLength)).
					counterEnable().					
					make();
		}
	
	}


	private boolean readCryptoFileHeader() throws IOException {
		DataInputStream dis = null;
		try {
			dis = new DataInputStream(new BufferedInputStream(new FileInputStream(this.fileToIndex)));
			CryptoFileHeader header = CryptoFileHeader.read(dis, config.getCryptoFileHeaderAlgoritmNameLength());
			this.formatVersion = header.getFormatVersion();
			if (this.formatVersion != this.config.getCryptoFileFormatVersion()) {
				LOG.error("Wrong crypto file format version");
				return false;
			}
			this.algorithm = header.getAlrorithmName();
			this.fullTermLength = header.getCryptoTermLength();
			this.numRows = header.getNumRows();
			this.retainedTermLength = this.config.getTemSizeToRetain(this.numRows);
			this.numColumns = header.getNumColumns();
			if (this.numColumns < 1 || this.numColumns > 30) {
				LOG.error("Number of columns is " + this.numColumns + " but it must be between 1 and 30");
				return false;
			}
			long numCells = (long)numColumns * (long)numRows;
			if ( numCells > 2000000000) {
				LOG.error("Number of cells is " + numCells + " but maximum allowed is 2 Billion");
				return false;
			}
			int numCellsInt = this.numColumns*this.numRows;
			this.numShards = (int)Math.ceil(numCellsInt / (double) this.config.getIndexerOptimalCellsPerShard());
			if (this.config.getIndexerNumThreads() > this.numShards) {
				this.numShards = this.config.getIndexerNumThreads();
			}
			return true;

		} catch (FileNotFoundException e) {
			LOG.error("Crypto File: " + this.fileToIndex + " not found");
			return false;
		} catch (IOException e) {
			LOG.error("Error reading header of Crypto File: " + this.fileToIndex + e.getMessage());
			return false;
		} finally {
			if (dis != null ) dis.close();
		}
	}
}
