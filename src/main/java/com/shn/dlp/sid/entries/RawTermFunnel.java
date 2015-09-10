package com.shn.dlp.sid.entries;

import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

public enum RawTermFunnel implements Funnel<RawTerm>{
	INSTANCE;

	@Override
	public void funnel(RawTerm term, PrimitiveSink into) {
		into.putBytes(term.getValue());
	}
	
}
