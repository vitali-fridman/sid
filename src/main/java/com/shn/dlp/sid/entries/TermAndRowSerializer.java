package com.shn.dlp.sid.entries;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.mapdb.Serializer;

import com.shn.dlp.sid.security.Crypter;
import com.shn.dlp.sid.security.CryptoException;
import com.shn.dlp.sid.util.SidConfiguration;

public class TermAndRowSerializer extends Serializer<TermAndRow> {
	
	private final int termLength;

	public TermAndRowSerializer(int termLength) {
		this.termLength = termLength;
	}
	
	@Override
	public void serialize(DataOutput out, TermAndRow entry) throws IOException {
		out.writeInt(entry.getRow());
		out.write(entry.getTerm().getValue());
	}

	@Override
	public TermAndRow deserialize(DataInput in, int available) throws IOException {
		int row = in.readInt();
		byte[] termValue = new byte[this.termLength];
		in.readFully(termValue);
		return new TermAndRow(new RawTerm(termValue), row);
	}
	
	@Override
	public int fixedSize() {
		return this.termLength + 4;
	}

}
