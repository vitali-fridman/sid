package com.shn.dlp.sid.util;

public final class TermHash {
	
	public static int calculateHashForSpine(final byte[] term) {
        int theHash = 0;
        final int numBytes = (term.length < 4) ? term.length : 4;
        for (int i = term.length - numBytes; i < term.length; ++i) {
            theHash |= (term[i] & 0xFF) << 8 * i;
        }
        if (theHash < 0) {
            return -theHash;
        }
        return theHash;
    }
    
    public static int calculateHashForSearch(final byte[] term) {
        int theHash = 0;
        for (int numBytes = (term.length < 4) ? term.length : 4, i = 0; i < numBytes; ++i) {
            theHash |= (term[i] & 0xFF) << 8 * i;
        }
        return theHash;
    }
}
