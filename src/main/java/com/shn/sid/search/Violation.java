package com.shn.sid.search;

import java.util.LinkedList;
import java.util.List;

import com.shn.dlp.sid.entries.Token;

public class Violation {

	private List<Token> tokens;
	private final int row;
	
	public List<Token> getTokens() {
		return tokens;
	}

	public int getRow() {
		return row;
	}

	public Violation(int row) {
		this.tokens = new LinkedList<Token>();
		this.row = row;
	}
	
	public void addToken(Token token) {
		this.tokens.add(token);
	}

	public int numTokens() {
		return this.numTokens();
	}

	@Override
	public String toString() {
		StringBuilder sb= new StringBuilder();
		sb.append("Violation ");
		for (Token token : this.tokens) {
			sb.append(":").append(token.toString());
		}
		return sb.toString();
	}
}
