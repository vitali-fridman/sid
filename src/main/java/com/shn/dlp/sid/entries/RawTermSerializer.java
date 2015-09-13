package com.shn.dlp.sid.entries;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.mapdb.Serializer;

import com.shn.dlp.sid.security.Crypter;
import com.shn.dlp.sid.security.CryptoException;
import com.shn.dlp.sid.util.SidConfiguration;

public class RawTermSerializer extends Serializer<RawTerm> {

	private final int termLength;
	
	public RawTermSerializer(int termLength) {
		this.termLength = termLength;
	}

	@Override
	public void serialize(DataOutput out, RawTerm term) throws IOException {
		out.write(term.getValue());
	}

	@Override
	public RawTerm deserialize(DataInput in, int available) throws IOException {
		byte[] value = new byte[this.termLength];
		in.readFully(value);
		return new RawTerm(value);
	}
	
	@Override
	public int fixedSize() {
		return this.termLength;
	}

}
