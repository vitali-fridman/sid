package com.shn.sid.search;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.shn.dlp.sid.entries.CellRowAndColMask;
import com.shn.dlp.sid.entries.Token;
import com.shn.dlp.sid.security.CryptoException;
import com.shn.dlp.sid.util.SidConfiguration;
import com.shn.sid.search.SearchIndex.FirstSearchLookupResult;
import com.shn.sid.search.SearchIndex.TokenPresense;

public class ViolationsDetector {

	private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());  
	private final SidConfiguration config;
	private final SearchIndex index;

	public ViolationsDetector(SidConfiguration config, String indexName) {
		this.config = config;
		this.index = new SearchIndex(config, indexName);
	}
	
	public void loadIndex() throws JsonParseException, JsonMappingException, IOException, CryptoException {
		this.index.openIndex();
	}
	
	public void unloadIndex() {
		this.index.closeIndex();
	}

	public List<Violation> findViolations(List<Token> tokens, int colThreashold, int violationsThreashold) {

		List<Violation> violations = new LinkedList<Violation>();
		List<Violation> candidateViolations = new LinkedList<Violation>();

		List <SearchIndex.FirstSearchLookupResult> uncommonFirstSearchResults =  new LinkedList<SearchIndex.FirstSearchLookupResult>();
		List <SearchIndex.FirstSearchLookupResult> commonFirstSearchResults =  new LinkedList<SearchIndex.FirstSearchLookupResult>();
		int numCommon = 0;
		int numUncommon = 0;
		for (int i=0; i<tokens.size(); i++)  {
			SearchIndex.FirstSearchLookupResult firstSearchResult = index.firstSearch(tokens.get(i));
			if (firstSearchResult == null) {
				//nothing
			} else if (firstSearchResult.getPresense() == TokenPresense.UNCOMMON) {
				numUncommon++;
				uncommonFirstSearchResults.add(firstSearchResult);
			} else {
				numCommon++;
				commonFirstSearchResults.add(firstSearchResult);
			}
		}

		if (numUncommon == 0) {
			LOG.info("There are 0 uncommon terms in SID search, baling out");
			return null;
		}
		if (numUncommon + numCommon < colThreashold) {
			LOG.info("Numbre of common and uncommone terms in SID search is less than column threashold, baling out");
			return null;
		}

		Map<Integer, List<Token>> perRowMap = separateUncommonTermsPerRow(uncommonFirstSearchResults);

		for (int row : perRowMap.keySet()) {
			List<Token> tokensOnRow = perRowMap.get(row);	
			Violation violation = new Violation(row);
			for (Token token : tokensOnRow) {
				violation.addToken(token);
			}
			if (violation.numTokens() >= colThreashold) {
				violations.add(violation);
			} else {
				candidateViolations.add(violation);
			}
		}

		if ((violations.size() > violationsThreashold )) {
			LOG.info("Found enough violation just in uncommon terms, returning this list");
			return violations;
		}

		for (Violation candidateViolation : candidateViolations) {
			int row = candidateViolation.getRow();
			for (SearchIndex.FirstSearchLookupResult commonFirstSearcResult : commonFirstSearchResults) {
				Token commonToken = commonFirstSearcResult.getToken();
				if (this.index.secondSearch(row, commonToken)) {
					candidateViolation.addToken(commonToken);
					if (candidateViolation.numTokens() > colThreashold) {
						violations.add(candidateViolation);
						if (violations.size() >= violationsThreashold) {
							LOG.info("Found engougth violations while processing candidate rows, returning this list");
							return violations;
						}
					}
				}
			}
		}
		
		// LOG.info("Did not find enough violations, returning null");
		// return null;
		LOG.info("returing " + violations.size() + " violations");
		return violations;
	}

	private Map<Integer, List<Token>> separateUncommonTermsPerRow(List<FirstSearchLookupResult> firstSearchResults) {
		Map<Integer, List<Token>> perRowMap = new HashMap<Integer, List<Token>>();
		for (SearchIndex.FirstSearchLookupResult result : firstSearchResults) {
			ArrayList<CellRowAndColMask> rows = result.getCellRowAndColMask();
			for (CellRowAndColMask entry : rows) {
				List<Token> listOfTokens = perRowMap.get(entry.getRow());
				if (listOfTokens == null) {
					listOfTokens = new LinkedList<Token>();
				}
				listOfTokens.add(result.getToken());
				perRowMap.put(entry.getRow(), listOfTokens);
			}
		}
		return perRowMap;
	}

}
