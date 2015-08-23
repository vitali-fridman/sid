package com.shn.dlp.sid.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.Optional;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SidConfiguration {

	private final Properties properties;

	private final static long GIG = 1024*1024*1024l;
	private final static long MEG = 1024*1024L;
	
	private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	private final static String PROPERTIES_FILE = "sid.properties";

	private final static String MOUNT_POINT_DEFAULT = "/mnt/sid/";
	private final static String MOUNT_POINT_PROP = "sid.DiskMountPont";

	private final static String DATA_FILES_DIR_DEFAULT = "datafiles";
	private final static String DATA_FILES_DIR_PROP = "sid.DataFilesDirectory";

	private final static String INDEXER_TEMP_DIR_DEFAULT = "indexerTemp";
	private final static String INDEXER_TEMP_DIR_PROP = "sid.indexer.tempDirectory";

	private final static String INDEXES_DIR_DEFAULT = "indexes";
	private final static String INDEXES_DIR_PROP = "sid.indexesDirectory";
	
	private final static int CRYPTO_FILE_FORMAT_VERSION_DEFAULT = 1;
	private final static String CRYPTO_FILE_FORMAT_VERSION_PROP = "sid.cryptoFileFormatVersion";
	
	private final static int OPTIMAL_INDEXER_CELLS_PER_SHARD_DEFAULT = 15000000;
	private final static String OPTIMAL_INDEXER_CELLS_PER_SHARD_PROP = "sid.indexer.optimalCellsPerShard";
	
	private final static int INDEXER_GC_PERIOD_DEFAULT = 10000;
	private final static String INDEXER_GC_PERIOD_PROP = "sid.indexer.gcPeriod";
	
	private static final int INDEXER_NUM_THREAD_DEFAULT = 12;
	private static final String INDEXER_NUM_THREADS_PROP = "sid.indexer.numThreads";
	
	private final static int INDEXER_INITIAL_CELL_LIST_SIZE_DEFAULT = 1000;
	private final static String INDEXER_INITIAL_CELL_LIST_SIZE_PROP = "sid.indexer.initialCellListSize";
	
	private final static long INDEXER_MAPDB_SIZE_INCREMENT_DEFAULT = 128 * MEG;
	private final static String INDEXER_MAPDB_SIZE_INCREMENT_PROP = "sid.indexer.mapDbSizeIncrement";
	
	private static final int INDEXER_ASYNC_WRRITE_QUEUE_SIZE_DEFAULT = 100000;
	private static final String INDEXER_ASYNC_WRRITE_QUEUE_SIZE_PROP = "sid.indexer.asyncWriteQueueSize";
	
	private final static int INDEXER_ASYNC_WRITE_FLUSH_DELAY_DEFAULT = 60000;
	private final static String INDEXER_ASYNC_WRITE_FLUSH_DELAY_PROP = "sid.indexer.asyncWriteFlushDelay";
	
	private final static int INDEXER_LOGGING_CELL_COUNT_DEFAULT = 1000000;
	private final static String INDEXER_LOGGING_CELL_COUNT_PROP = "sid.indexer.loggingCellCount";
	
	private final static String CRYPTO_ALGORITHM_NAME_DEFAULT = "HmacSHA1";
	private final static String CRYPTO_ALGORITHM_NAME_PROP = "sid.cryptoAlgorithm";
	
	private final static int CRYPTO_FILE_HEADER_LENGTH = 32;	
	private final static int CRYPTO_FILE_HEADER_ALGORITM_NAME_LENGTH = 21;
	
	private final static int TERM_COMMONALITY_THREASHOLD_DEFAULT = 100;
	private final static String TERM_COMMONALITY_THREASHOLD_PROP = "sid.termCommonalityThreashold";
	
	
	public SidConfiguration (String propertiesFileName) throws FileNotFoundException, IOException {
		this.properties = new Properties();
		this.properties.load(new FileInputStream(propertiesFileName));
	}

	public SidConfiguration () throws IOException {
		InputStream is = this.getClass().getClassLoader().getResourceAsStream(PROPERTIES_FILE);
		this.properties = new Properties();
		if (this.properties != null) {
			this.properties.load(is);  
		} else {
			throw new FileNotFoundException("Properties " + PROPERTIES_FILE + "are not present in resources.");
		}
		is.close();
	} 

	public String getDiskMountPoint() {
		return getStringProperty(MOUNT_POINT_PROP, MOUNT_POINT_DEFAULT);
	}

	public String getDataFilesDrictory() {
		return  getDiskMountPoint() + getStringProperty(DATA_FILES_DIR_PROP, DATA_FILES_DIR_DEFAULT);

	}

	public String getIndexerTempDirectory() {
		return  getDiskMountPoint() + getStringProperty(INDEXER_TEMP_DIR_PROP, INDEXER_TEMP_DIR_DEFAULT);

	}

	public String getIndexesDirectory() {
		return  getDiskMountPoint() + getStringProperty(INDEXES_DIR_PROP, INDEXES_DIR_DEFAULT);

	}
	
	public int getCryptoFileFormatVersion() {
		return getIntProperty(CRYPTO_FILE_FORMAT_VERSION_PROP, CRYPTO_FILE_FORMAT_VERSION_DEFAULT);
	}

	public int getIndexerOptimalCellsPerShard() {
		return getIntProperty(OPTIMAL_INDEXER_CELLS_PER_SHARD_PROP, OPTIMAL_INDEXER_CELLS_PER_SHARD_DEFAULT);
	}
	
	public int getIndexerGcPeriod() {
		return getIntProperty(INDEXER_GC_PERIOD_PROP, INDEXER_GC_PERIOD_DEFAULT);
	}
	
	public int getIndexerNumThreads() {
		return getIntProperty(INDEXER_NUM_THREADS_PROP, INDEXER_NUM_THREAD_DEFAULT);
	}
	
	public int getIndexerInitialCellListSize() {
		return getIntProperty(INDEXER_INITIAL_CELL_LIST_SIZE_PROP, INDEXER_INITIAL_CELL_LIST_SIZE_DEFAULT);
	}
	
	public long getIndexerMapDbSizeIncrement() {
		return getLongProperty(INDEXER_MAPDB_SIZE_INCREMENT_PROP, INDEXER_MAPDB_SIZE_INCREMENT_DEFAULT);
	}
	
	public int getIndexerAsyncWriteQueueSize() {
		return getIntProperty(INDEXER_ASYNC_WRRITE_QUEUE_SIZE_PROP, INDEXER_ASYNC_WRRITE_QUEUE_SIZE_DEFAULT);
	}
	
	public int getIndexerAsyncWriteFlushDelay() {
		return getIntProperty(INDEXER_ASYNC_WRITE_FLUSH_DELAY_PROP, INDEXER_ASYNC_WRITE_FLUSH_DELAY_DEFAULT);
	}
	
	public int getIndexerLoggingCellCount() {
		return getIntProperty(INDEXER_LOGGING_CELL_COUNT_PROP, INDEXER_LOGGING_CELL_COUNT_DEFAULT);
	}
	
	public String getCryptoAlgorithmName() {
		return getStringProperty(CRYPTO_ALGORITHM_NAME_PROP, CRYPTO_ALGORITHM_NAME_DEFAULT);  
	}
	
	public int getCryptoFileHeaderLength() {
		return CRYPTO_FILE_HEADER_LENGTH;
	}
	
	public int getCryptoFileHeaderAlgoritmNameLength() {
		return CRYPTO_FILE_HEADER_ALGORITM_NAME_LENGTH;
	}
	
	public int getCommonalityThreashold() {
		return getIntProperty(TERM_COMMONALITY_THREASHOLD_PROP, TERM_COMMONALITY_THREASHOLD_DEFAULT);
	}
	
	private String getStringProperty (String name, String defaultValue) {
		if (this.properties.containsKey(name)) {
			return this.properties.getProperty(name);
		} else {
			return defaultValue;
		}
	}

	private int getIntProperty (String name, int defaultValue) {
		if (this.properties.containsKey(name)) {
			try {
				return Integer.parseInt(this.properties.getProperty(name));
			} catch (NumberFormatException e) {
				LOG.warn("Property " + name + " in " + PROPERTIES_FILE + " is not valid integer, using default");
				return defaultValue;
			}
		} else {
			return defaultValue;
		}
	}

	private long getLongProperty (String name, long defaultValue) {
		if (this.properties.containsKey(name)) {
			try {
				return Long.parseLong(this.properties.getProperty(name));
			} catch (NumberFormatException e) {
				LOG.warn("Property " + name + " in " + PROPERTIES_FILE + " is not valid long, using default");
				return defaultValue;
			}
		} else {
			return defaultValue;
		}
	}

	private double getDoubleProperty (String name, double defaultValue) {
		if (this.properties.containsKey(name)) {
			try {
				return Double.parseDouble(this.properties.getProperty(name));
			} catch (NumberFormatException e) {
				LOG.warn("Property " + name + " in " + PROPERTIES_FILE + " is not valid double, using default");
				return defaultValue;
			}
		} else {
			return defaultValue;
		}
	}

	private boolean getBooleanProperty (String name, boolean defaultValue) {
		if (this.properties.containsKey(name)) {
			return Boolean.parseBoolean(this.properties.getProperty(name));
		} else {
			return defaultValue;
		}
	}
}
