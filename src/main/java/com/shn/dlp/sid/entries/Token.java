package com.shn.dlp.sid.entries;

public class Token {

	private final String clear;
	private final RawTerm term;
	public enum Presense {NOT_PRESENT, COMMON, UNCOMMON};
	private Presense presense;
	
	public Token(String clear, RawTerm term) {
		this.clear = clear;
		this.term = term;
	}

	public String getClear() {
		return clear;
	}

	public RawTerm getTerm() {
		return term;
	}

	public Presense getPresense() {
		return presense;
	}

	public void setPresense(Presense presense) {
		this.presense = presense;
	}

	@Override
	public String toString() {
		return "Token:" + this.presense + ":" + this.clear;
	}
	
	
}
