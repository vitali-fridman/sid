package com.shn.dlp.sid.entries;

import java.io.DataInputStream;
import java.io.IOException;

public interface IndexEntryFactory {
	
	int estimateMemory(int termSize, int spineLength, int entryCount);
    
    IndexEntry read(DataInputStream is, int length) throws IOException;
}
