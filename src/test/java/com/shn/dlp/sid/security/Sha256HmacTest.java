package com.shn.dlp.sid.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

public class Sha256HmacTest {

	@Test
	public void sha1HmacLenghtTest() {
		Sha256Hmac hm = new Sha256Hmac();
		try {
			byte[] digest = hm.computeDigest("does not matter");
			assertEquals(Sha256Hmac.MAC_LENGTH, digest.length);
		} catch (CryptoException e) {
			fail("CryptoException" + e.getMessage());
		}
	}
	
	@Test
	public void sha1HmacLengthTestWithKey() {
		Sha256Hmac hm = new Sha256Hmac("abcdef".getBytes());
		try {
			byte[] digest = hm.computeDigest("does not matter");
			assertEquals(Sha256Hmac.MAC_LENGTH, digest.length);
		} catch (CryptoException e) {
			fail("CryptoException" + e.getMessage());
		}
	}
}

