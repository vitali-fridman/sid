package com.shn.dlp.sid.entries;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Arrays;

import org.apache.commons.codec.binary.Hex;

import com.shn.dlp.sid.util.TermHash;


public final class RawTerm implements Serializable, Comparable{

	private static final long serialVersionUID = -5672420139594083733L;

	private final byte[] value;
    
    public RawTerm(final byte[] value) {
        this.value = value;
    }
    
    public byte[] getValue() {
        return this.value;
    }
    
    @Override
    public boolean equals(final Object obj) {
        return obj instanceof RawTerm && Arrays.equals(((RawTerm)obj).value, this.value);
    }
    
    @Override
    public int hashCode() {
        return TermHash.calculateHashForSearch(this.value);
    }
    
    @Override
    public String toString() {
    	return Hex.encodeHexString(this.value);
    }

	@Override
	public int compareTo(Object o) {
		RawTerm rto = (RawTerm) o;
		BigInteger me =new BigInteger(this.value);
		BigInteger them = new BigInteger(rto.getValue());
		return me.compareTo(them);
	}
}
