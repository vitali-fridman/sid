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
		return "Token: " + this.clear;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((clear == null) ? 0 : clear.hashCode());
		result = prime * result + ((presense == null) ? 0 : presense.hashCode());
		result = prime * result + ((term == null) ? 0 : term.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Token other = (Token) obj;
		if (clear == null) {
			if (other.clear != null)
				return false;
		} else if (!clear.equals(other.clear))
			return false;
		return true;
	}
	
	
}
