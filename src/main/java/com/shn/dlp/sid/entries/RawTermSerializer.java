package com.shn.dlp.sid.entries;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.mapdb.Serializer;

import com.shn.dlp.sid.security.Crypter;
import com.shn.dlp.sid.security.CryptoException;
import com.shn.dlp.sid.util.SidConfiguration;

public class RawTermSerializer extends Serializer<RawTerm> {

	private final int cryptoDataLength;
	
	public RawTermSerializer(SidConfiguration config) throws CryptoException {
		this.cryptoDataLength = new Crypter(config).getCryptoValueLength();
	}

	@Override
	public void serialize(DataOutput out, RawTerm term) throws IOException {
		out.write(term.getValue());
	}

	@Override
	public RawTerm deserialize(DataInput in, int available) throws IOException {
		byte[] value = new byte[this.cryptoDataLength];
		in.readFully(value);
		return new RawTerm(value);
	}
	
	@Override
	public int fixedSize() {
		return this.cryptoDataLength;
	}

	@Override
	public int hashCode(RawTerm a) {
		byte[] value = a.getValue();
		return (((value[3]       ) << 24) |
	            ((value[2] & 0xff) << 16) |
	            ((value[1] & 0xff) <<  8) |
	            ((value[0] & 0xff)      ));
	}
	
	@Override
	public boolean equals(RawTerm term1, RawTerm term2) {
		return term1.equals(term2);
	}
}
