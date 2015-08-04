package com.shn.dlp.sid.indexer;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.functors.NotNullPredicate;
import org.apache.commons.collections.iterators.FilterIterator;

import com.shn.dlp.sid.entries.IndexEntry;
import com.shn.dlp.sid.util.PrimeFinder;
import com.shn.dlp.sid.util.TermHash;

abstract class Index
{
    final List<IndexEntry>[] _spine;
    private final int _arrayListInitiallCapacity;
    int _entryCount;
    int _emptyBucketCount;
    static final int COUNT_NOT_SET = -1;
    private final int _termLengthToRetain;
    
    @SuppressWarnings("unchecked")
	protected Index(final int targetEntryCount, final double targetColisionListLength, final int termLengthToRetain) {
        this._arrayListInitiallCapacity = ((targetColisionListLength > 1.0) ? ((int)targetColisionListLength * 2) : 2);
        final int targetSpineLength = calculateSpineLength(targetEntryCount, targetColisionListLength);
        this._spine = new List[targetSpineLength];
        this._entryCount = -1;
        this._emptyBucketCount = targetSpineLength;
        Arrays.fill(this._spine, null);
        this._termLengthToRetain = termLengthToRetain;
    }
    
    static int calculateSpineLength(final int targetEntryCount, final double targetColisionListLength) {
        return PrimeFinder.nextPrime((int)(targetEntryCount / targetColisionListLength + 0.5));
    }
    
    static int[] calculateSpineRanges(final int targetEntryCount, final double targetCollisionListLength, final int totalCells, final int maxCellsInPath) {
        final int spineLength = calculateSpineLength(targetEntryCount, targetCollisionListLength);
        int rangeSpan;
        int numRanges;
        if (maxCellsInPath < totalCells) {
            rangeSpan = spineLength * (maxCellsInPath / totalCells);
            numRanges = spineLength / rangeSpan + 1;
        }
        else {
            rangeSpan = spineLength;
            numRanges = 1;
        }
        final int[] ranges = new int[numRanges];
        for (int i = 0; i < numRanges; ++i) {
            ranges[i] = rangeSpan * (i + 1);
            if (ranges[i] >= spineLength) {
                ranges[i] = spineLength;
            }
        }
        return ranges;
    }
    
    void addEntry(final IndexEntry entry) {
        final int bucket = this.calculateBucket(entry);
        this.addEntry(entry, bucket);
    }
    
    final void addEntry(final IndexEntry entry, final int bucket) {
        if (this._spine[bucket] == null) {
            this._spine[bucket] = new ArrayList<IndexEntry>(this._arrayListInitiallCapacity);
            --this._emptyBucketCount;
        }
        this._spine[bucket].add(entry);
    }
    
    final int getSpineLength() {
        if (this._spine != null) {
            return this._spine.length;
        }
        return -1;
    }
    
    final List<?> getBucket(final int spineIndex) {
        return this._spine[spineIndex];
    }
    
    final int getEntryCount() {
        if (this._entryCount == -1) {
            throw new IllegalStateException("Entry count has not been set.");
        }
        return this._entryCount;
    }
    
    final void setEntryCount(final int entryCount) {
        this._entryCount = entryCount;
    }
    
    final void setEmptyBucketCount(final int emptyBucketCount) {
        this._emptyBucketCount = emptyBucketCount;
    }
    
    static int getFourBytesHash(final IndexEntry entry) {
        return TermHash.calculateHashForSpine(entry.getTerm());
    }
    
    abstract int calculateBucket(final IndexEntry p0);
    
    void writeToFile(final DataOutputStream indexFile) throws IOException {
        indexFile.writeInt(this._termLengthToRetain);
        indexFile.writeInt(this._spine.length);
        indexFile.writeInt(this._emptyBucketCount);
        if (this._entryCount == -1) {
            throw new IllegalStateException("Entry count for index is not set");
        }
        indexFile.writeInt(this._entryCount);
        for (int i = 0; i < this._spine.length; ++i) {
            final List<?> bucket = this._spine[i];
            if (bucket != null) {
                int bucketSize = 0;
                Iterator<?> it = new FilterIterator(bucket.iterator(), NotNullPredicate.INSTANCE);
                while (it.hasNext()) {
                    it.next();
                    ++bucketSize;
                }
                indexFile.writeInt(bucketSize);
                it = new FilterIterator(bucket.iterator(), NotNullPredicate.INSTANCE);
                while (it.hasNext()) {
                    ((IndexEntry) it.next()).write(indexFile, this._termLengthToRetain);
                }
            }
            else {
                indexFile.writeInt(0);
            }
        }
    }
    
}

