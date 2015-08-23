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
		this.termLength = new Crypter(config).getCryptoValueLength();;
	}
	
	@Override
	public void serialize(DataOutput out, AllCommonTermsEntry entry) throws IOException {
		out.writeInt(entry.getColMask());
		out.write(entry.getTerm());
	}

	@Override
	public AllCommonTermsEntry deserialize(DataInput in, int available) throws IOException {
		int colMask = in.readInt();
		byte[] term = new byte[termLength];
		in.readFully(term);
		return new AllCommonTermsEntry(colMask, term);
	}

	@Override
	public int fixedSize() {
		return this.termLength;
	}

	@Override
	public int hashCode(AllCommonTermsEntry entry) {
		byte[] value = entry.getTerm();
		return (((value[3]       ) << 24) |
	            ((value[2] & 0xff) << 16) |
	            ((value[1] & 0xff) <<  8) |
	            ((value[0] & 0xff)      )) + entry.getColMask();
	}
	
	@Override
	public boolean equals(AllCommonTermsEntry entry1, AllCommonTermsEntry entry2) {
		return (entry1.getTerm().equals(entry2.getTerm())) && (entry1.getColMask() == entry2.getColMask());
	}
}
