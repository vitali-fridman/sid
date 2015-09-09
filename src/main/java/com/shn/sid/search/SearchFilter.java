package com.shn.sid.search;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Set;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import com.shn.dlp.sid.entries.RawTerm;
import com.shn.dlp.sid.entries.TermAndRow;
import com.shn.dlp.sid.util.SidConfiguration;

public class SearchFilter {
	
	private final SidConfiguration config;
	private final String indexName;
	private final String indexDirectory;
	private final static String ALL_COMMON_TERMS_FILTER_FILE = "AllCommonTerms.filer";
	private final static String UNCOMMON_TERMS_FILTER_FILE = "UncommonTerms.filter";
	private final static String COMMON_TERMS_FILTER_FILE = "CommonTerms.filter";
	private BloomFilter<RawTerm> allCommonTermsFilter;
	private BloomFilter<RawTerm> uncommonTermsFilter;
	private BloomFilter<TermAndRow> commonTermsFilter;
	

	public SearchFilter(SidConfiguration config, String indexName) {
		this.config = config;
		this.indexName = indexName;
		this.indexDirectory = config.getIndexesDirectory() + File.separator + indexName + File.separator;
	}
	
	public void loadFilters() throws FileNotFoundException {
		BufferedInputStream allCommonTermsFilterIn = new BufferedInputStream(new FileInputStream(this.indexDirectory + ALL_COMMON_TERMS_FILTER_FILE));
		BufferedInputStream unCommonTermsFilterIn = new BufferedInputStream(new FileInputStream(this.indexDirectory + UNCOMMON_TERMS_FILTER_FILE));
		BufferedInputStream commonTermsFilterIn = new BufferedInputStream(new FileInputStream(this.indexDirectory + COMMON_TERMS_FILTER_FILE));		
	}
	
	public void unloadFilters() {
		
	}
	
	private enum RawTermFunnel implements Funnel<RawTerm>{
		INSTANCE;

		@Override
		public void funnel(RawTerm from, PrimitiveSink into) {
			// TODO Auto-generated method stub
			
		}
		
	}
}
