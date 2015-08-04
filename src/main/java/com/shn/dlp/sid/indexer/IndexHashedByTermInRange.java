package com.shn.dlp.sid.indexer;

import com.shn.dlp.sid.entries.IndexEntry;

class IndexHashedByTermInRange extends IndexHashedByTerm
{
    private final int rangeBottom;
    private final int rangeTop;
    
    public IndexHashedByTermInRange(final int targetEntryCount, final double targetColisionListLength, final int termLengthToRetain, final int rangeBottom, final int rangeTop) {
        super(targetEntryCount, targetColisionListLength, termLengthToRetain);
        this.rangeBottom = rangeBottom;
        this.rangeTop = rangeTop;
    }
    
    final boolean tryToAddEntry(final IndexEntry entry) {
        final int bucket = this.calculateBucket(entry);
        if (bucket >= this.rangeBottom && bucket < this.rangeTop) {
            this.addEntry(entry, bucket);
            return true;
        }
        return false;
    }
}
