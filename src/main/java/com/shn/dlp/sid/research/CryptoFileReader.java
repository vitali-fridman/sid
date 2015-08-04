package com.shn.dlp.sid.research;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import com.shn.dlp.sid.entries.Cell;
import com.shn.dlp.sid.entries.CellLocation;
import com.shn.dlp.sid.entries.CellLocationListSerializer;
import com.shn.dlp.sid.entries.RawTerm;
import com.shn.dlp.sid.entries.RawTermSerializer;
import com.shn.dlp.sid.security.Sha256Hmac;
import com.shn.dlp.sid.tools.TestDataFileGenerator;

public class CryptoFileReader {

	public final static String MAP_NAME = "CellMap";
	public final static String DB_NAME = "SidDmap";
	
	@Option(name="-f",usage="File Name to read", required=true)
	private String fileName;
	@Option(name="-d",usage="Database directory name", required=true)
	private String dbDirectoryName;
	
	public static void main(String[] args) throws IOException {
		
		CryptoFileReader cfr = new CryptoFileReader();
		CmdLineParser parser = new CmdLineParser(cfr);
		try {
			parser.parseArgument(args);
		} catch (CmdLineException e) {
			System.err.println(e.getMessage());
			parser.printUsage(System.err);
			return;
		}
		
		File file = new File(cfr.fileName + TestDataFileGenerator.CRYPTO_SUFFIX);
		DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
		
		readHeader(dis);
		DB db = createDB(cfr.dbDirectoryName);
		HTreeMap<RawTerm,ArrayList<CellLocation>> dbmap = createMap(db);
		
		readData(dis, dbmap);
				
		dis.close();
		db.commit();
		// db.compact();
		// inspectDBmap(dbmap);
		dbmap.close();
	}

	private static DB createDB(String dbDirectoryName) throws IOException {
		FileUtils.deleteQuietly(new File(dbDirectoryName));
		FileUtils.forceMkdir(new File(dbDirectoryName));
		DB db = DBMaker.fileDB(new File(dbDirectoryName + "/" + DB_NAME)).
				// transactionDisable().
				// closeOnJvmShutdown().
				fileMmapEnable().
				asyncWriteEnable().
				asyncWriteQueueSize(10000).
				executorEnable().
				make();
		return db;
	}

	private static void inspectDBmap(HTreeMap<RawTerm, ArrayList<CellLocation>> dbmap) {
		Set<Entry<RawTerm, ArrayList<CellLocation>>> entrySet = dbmap.entrySet();
		for (Entry<RawTerm, ArrayList<CellLocation>> entry : entrySet) {
			System.out.println("Term: " + entry.getKey().toString());
			for (CellLocation cellLocation : entry.getValue()) {
				System.out.println("\t" + cellLocation.getRow() + ":" + cellLocation.getColumn());
			}
		}
	}

	private static HTreeMap<RawTerm, ArrayList<CellLocation>> createMap(DB db) {
		HTreeMap<RawTerm, ArrayList<CellLocation>> dbmap = db.hashMapCreate(MAP_NAME).
				valueSerializer(new CellLocationListSerializer()).
				keySerializer(new RawTermSerializer()).
				make();
		return dbmap;
	}

	private static void readData(DataInputStream dis, HTreeMap<RawTerm,ArrayList<CellLocation>> dbmap) {
		int i=0;
		while(true) {
			try {
				Cell cell = Cell.read(dis, Sha256Hmac.MAC_LENGTH);
				// System.out.println("Cell " + cell.getRow() + ":" + cell.getColumn());
				if (i%10000 == 0) {
					System.out.println("Cell: " + i);
				}
				CellLocation cellLocation = new CellLocation(cell.getRow(), cell.getColumn());
				
				//insert into dbmap
				RawTerm rt = new RawTerm(cell.getTerm());
				ArrayList<CellLocation> list = dbmap.get(rt);
				if (list == null) {
					ArrayList<CellLocation> newList = new ArrayList<CellLocation>();
					newList.add(cellLocation);
					newList.trimToSize();
					dbmap.put(rt, newList);
				} else {
					list.add(cellLocation);
					list.trimToSize();
					dbmap.put(rt, list);
				}
				
			} catch (IOException e) {
				// end of file reached
				return;
			}
			i++;
		}
		
	}

	private static void readHeader(DataInputStream dis) throws IOException {
		dis.skip(6);
	}

}
