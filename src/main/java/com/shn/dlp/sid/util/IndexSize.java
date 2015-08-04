package com.shn.dlp.sid.util;

public class IndexSize {
	public static final int SIZE_OF_COLUMN_MASK = 4;
    public static final int MAX_COLUMN_COUNT = 31;
    public static final int MAX_COLUMN_INDEX = 30;
    public static final int SIZE_OF_END_MARKER = 1;
    public static final int SIZE_OF_COL = 1;
    public static final int SIZE_OF_ROW = 4;
    
    public static int getSizeOfAllCommonTermContent(final int termToRetain, final int entryCount, final int spineLength, final int emptyBucketCount) {
        return (4 + termToRetain) * entryCount + (spineLength - emptyBucketCount) * 1;
    }
    
    public static long getSizeOfRowTermContent(final int termSize, final int entryCount, final int spineLength, final int emptyBucketCount) {
        return (5 + termSize) * entryCount + (spineLength - emptyBucketCount) * 1L;
    }
}
