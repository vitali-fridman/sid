package com.shn.dlp.sid.security;

import static org.junit.Assert.fail;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Mac;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.BDDMockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;


@RunWith(PowerMockRunner.class)
@PrepareForTest({ Mac.class })
public class Sha256ExceptionsTest {
	
	@SuppressWarnings("unchecked")
	@Test
	public void cryptoExceptionTest() throws NoSuchAlgorithmException {
		Sha256Hmac hm = new Sha256Hmac();
		PowerMockito.mockStatic(Mac.class);
		BDDMockito.given(Mac.getInstance("HmacSHA256")).willThrow(InvalidKeyException.class);
		
		try {
			hm.computeDigest("abc");
		} catch (CryptoException e) {
			return;
		}
		fail("Did not throw CryptoException");
	}
}

