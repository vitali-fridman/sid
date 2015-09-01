package com.shn.dlp.sid.lexer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

import com.shn.dlp.sid.entries.RawTerm;
import com.shn.dlp.sid.entries.Token;
import com.shn.dlp.sid.security.Crypter;
import com.shn.dlp.sid.security.CryptoException;
import com.shn.dlp.sid.util.SidConfiguration;

public class TestLexer {

	private final SidConfiguration config;
	
	public TestLexer(SidConfiguration config) {
		this.config = config;
	}


	public List<Token> scan(String fileName) throws IOException, CryptoException {
		byte[] content = Files.readAllBytes(Paths.get(fileName));
		String contentAsString = new String(content);
		String[] clearTerms = contentAsString.split("[ \t\r\n]+");
		Crypter crypter = new Crypter(this.config);
		List<Token> rawTerms = new LinkedList<Token>();
		for (String clearTerm : clearTerms) {
			clearTerm = clearTerm.trim();
			rawTerms.add(new Token(clearTerm, new RawTerm(crypter.computeDigest(clearTerm))));
		}
		return rawTerms;
	}

}
