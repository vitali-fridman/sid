package com.shn.dlp.sid.indexer;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.OptionHandlerFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Ints;
import com.shn.dlp.sid.security.Crypter;
import com.shn.dlp.sid.security.CryptoException;
import com.shn.dlp.sid.util.SidConfiguration;

public class CryptoFileCreator {

	@Option(name="-f",usage="File Name to read", required=true)
	private String fileName;
	@Option(name="-p", usage="Properties File", required=false)
	private String propertiesFile;

	private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());  
	
	public static void main(String[] args) throws FileNotFoundException, IOException, CryptoException {
		CryptoFileCreator cfr = new CryptoFileCreator();
		CmdLineParser parser = new CmdLineParser(cfr);
		try {
			parser.parseArgument(args);
		} catch (CmdLineException e) {
			LOG.error(e.getMessage());
			LOG.error(parser.printExample(OptionHandlerFilter.ALL));
			return;
		}
		
		long start=System.nanoTime();
		
		SidConfiguration config;
		if (cfr.propertiesFile != null) {
			config = new SidConfiguration(cfr.propertiesFile);
		} else {
			config = new SidConfiguration();
		}
		
		String fileToCrypto = config.getDataFilesDrictory() + File.separator + cfr.fileName;
		String cryptoFile = fileToCrypto + Crypter.CRYPRO_FILE_SUFFIX;
		
		if (!Files.exists(Paths.get(fileToCrypto))) {
			LOG.error("Data File does not exist");
			System.exit(-1);
		}
		
		Crypter crypter = new Crypter(config);
		
		BufferedReader clearReader = new BufferedReader(new FileReader(fileToCrypto));
		OutputStream cryptoWriter = new BufferedOutputStream(new FileOutputStream(new File(cryptoFile))); 
		
		int rows = 0;
		int cols = 0;
		for(String line; (line = clearReader.readLine()) != null; ) {
	        if (rows == 0) {
	        	String[] tokens = line.split("[,;\t]");
	        	cols = tokens.length;
	        }
	        rows++;
		}
		
		writeHeader(cryptoWriter, rows, cols, config, crypter);
		clearReader.close();
		clearReader = new BufferedReader(new FileReader(fileToCrypto));
		
		int row = 0;
		for(String line; (line = clearReader.readLine()) != null; ) {
	        String[] tokens = line.split("[,;\t]");
	        for (int col = 0; col < tokens.length; col++) {
	        	byte[] cryptedToken = crypter.computeDigest(tokens[col]);
	        	cryptoWriter.write(intToBytes(row));
	        	cryptoWriter.write(col);
	        	cryptoWriter.write(cryptedToken);
	        }
	        row++;
	        if (row%10000 == 0) {
				LOG.info("Row: " + row/1000 +"K");
			}
		}
		
		clearReader.close();
		cryptoWriter.close();
	}
	
	private static void writeHeader(OutputStream cryptoWriter, int rows, int cols, SidConfiguration config, Crypter crypter) throws IOException {
			cryptoWriter.write(config.getCryptoFileHeaderLength());
			cryptoWriter.write(config.getCryptoFileFormatVersion());
			cryptoWriter.write(StringUtils.rightPad(config.getCryptoAlgorithmName(), 
					config.getCryptoFileHeaderAlgoritmNameLength()).getBytes());
			cryptoWriter.write(crypter.getCryptoValueLength());
			cryptoWriter.write(intToBytes(rows));
			cryptoWriter.write(cols);
	}
		

	private static byte[] intToBytes(int value) {
		return Ints.toByteArray(value);
	}

}
