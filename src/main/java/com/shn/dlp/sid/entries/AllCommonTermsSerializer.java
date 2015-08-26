package com.shn.dlp.sid.entries;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.mapdb.Serializer;

import com.shn.dlp.sid.security.Crypter;
import com.shn.dlp.sid.security.CryptoException;
import com.shn.dlp.sid.util.SidConfiguration;

public class AllCommonTermsSerializer extends Serializer<AllCommonTermsEntry >{

	private final int termLength;
	
	public AllCommonTermsSerializer(SidConfiguration config) throws CryptoException {
		this.termLength = new Crypter(config).getCryptoValueLength();
	}
	
	@Override
	public void serialize(DataOutput out, AllCommonTermsEntry entry) throws IOException {
		out.writeInt(entry.getColMask());
		out.write(entry.getTerm());
	}

	@Override
	public AllCommonTermsEntry deserialize(DataInput in, int available) throws IOException {
		int colMask = in.readInt();
		byte[] term = new byte[this.termLength];
		in.readFully(term);
		return new AllCommonTermsEntry(colMask, term);
	}

	@Override
	public int fixedSize() {
		return this.termLength + 4;
	}
}
