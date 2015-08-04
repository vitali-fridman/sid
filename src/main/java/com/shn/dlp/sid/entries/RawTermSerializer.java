package com.shn.dlp.sid.entries;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.mapdb.Serializer;

import com.shn.dlp.sid.security.Sha256Hmac;

public class RawTermSerializer extends Serializer<RawTerm> {

	@Override
	public void serialize(DataOutput out, RawTerm term) throws IOException {
		out.write(term.getValue());
	}

	@Override
	public RawTerm deserialize(DataInput in, int available) throws IOException {
		byte[] value = new byte[Sha256Hmac.MAC_LENGTH];
		in.readFully(value);
		return new RawTerm(value);
	}
	
	@Override
	public int fixedSize() {
		return Sha256Hmac.MAC_LENGTH;
	}

}
