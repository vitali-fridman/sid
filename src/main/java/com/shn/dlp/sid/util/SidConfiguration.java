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

	private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	private final static String PROPERTIES_FILE = "sid.properties";
	private final Properties properties;

	private final static String MOUNT_POINT_DEFAULT = "/mnt/sid/";
	private final static String MOUNT_POINT_PROP = "sid.DiskMountPont";

	private final static String DATA_FILES_DIR_DEFAULT = "datafiles";
	private final static String DATA_FILES_DIR_PROP = "sid.DataFilesDirectory";

	private final static String INDEXER_TEMP_DIR_DEFAULT = "indexerTemp";
	private final static String INDEXER_TEMP_DIR_PROP = "sid.indexerTempDirectory";

	private final static String INDEXES_DIR_DEFAULT = "indexes";
	private final static String INDEXES_DIR_PROPERTY = "sid.indexesDirectory";

	public SidConfiguration (String propertiesFileName) throws FileNotFoundException, IOException {
		this.properties = new Properties();
		this.properties.load(new FileInputStream(propertiesFileName));
	}

	public SidConfiguration () throws IOException {
		InputStream is = getClass().getResourceAsStream(PROPERTIES_FILE);
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
		return  getDiskMountPoint() + getStringProperty(INDEXES_DIR_PROPERTY, INDEXES_DIR_DEFAULT);

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
