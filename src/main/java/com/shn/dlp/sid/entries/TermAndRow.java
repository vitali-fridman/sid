package com.shn.dlp.sid.entries;

public class TermAndRow implements Comparable {
	
	private final RawTerm term;
	private final int row;
	
	public TermAndRow (RawTerm term, int row) {
		this.term = term;
		this.row = row;
	}
	
	public RawTerm getTerm() {
		return this.term;
	}
	
	public int getRow() {
		return this.row;
	}
	
	@Override
	public int hashCode() {
		return this.term.hashCode() + row;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TermAndRow other = (TermAndRow) obj;
		if (row != other.row)
			return false;
		if (term == null) {
			if (other.term != null)
				return false;
		} else if (!term.equals(other.term))
			return false;
		return true;
	}

	@Override
	public int compareTo(Object o) {
		TermAndRow other = (TermAndRow) o;
		if (this.row < other.row) {
			return -1;
		} else if (this.row > other.row) {
			return +1;
		} else {
			return this.term.compareTo(other);
		}
	}
}
