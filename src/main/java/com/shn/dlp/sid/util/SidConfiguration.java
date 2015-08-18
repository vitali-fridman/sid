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
	private final String mountPoint;
	private final static String MOUNT_POINT_DEFAULT = "/mnt/sid/";
	
	
	public SidConfiguration (String propertiesFileName, String mountPoint) throws FileNotFoundException, IOException {
		this.mountPoint = (mountPoint == null ? MOUNT_POINT_DEFAULT : mountPoint + "/");
		this.properties = new Properties();
		this.properties.load(new FileInputStream(propertiesFileName));
	}
	
	public SidConfiguration (String mountPoint) throws IOException {
		this.mountPoint = (mountPoint == null ? MOUNT_POINT_DEFAULT : mountPoint + "/");
		InputStream is = getClass().getResourceAsStream(PROPERTIES_FILE);
		this.properties = new Properties();
		if (this.properties != null) {
			this.properties.load(is);
		} else {
			throw new FileNotFoundException("Properties " + PROPERTIES_FILE + "are not present in resources.");
		}
		is.close();
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
				LOG.warn("Property " + name + " in " + PROPERTIES_FILE + " is not valid integer, using default");
				return defaultValue;
			}
		} else {
			return defaultValue;
		}
	}
}
