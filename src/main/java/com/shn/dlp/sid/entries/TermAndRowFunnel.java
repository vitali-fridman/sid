package com.shn.dlp.sid.entries;

import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

public enum TermAndRowFunnel implements Funnel<TermAndRow>{
	INSTANCE;
	@Override
	public void funnel(TermAndRow termAndRow, PrimitiveSink into) {
		into.putBytes(termAndRow.getTerm().getValue()).putInt(-1).putInt(termAndRow.getRow());
	}

}
