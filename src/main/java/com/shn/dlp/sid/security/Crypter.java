package com.shn.dlp.sid.security;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import javax.crypto.Mac;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.SecretKeySpec;

import com.shn.dlp.sid.util.SidConfiguration;

public final class Crypter {
	public static final String CRYPRO_FILE_SUFFIX = ".crypto";
	// private static final String MAC_ALGORITHM_NAME = "HmacSHA256";
	private static final byte[] BUILTIN_KEY = "56778990sdsfnbvnf87JHGSDFmjk)(%ks".getBytes(); 
	// public static final int MAC_LENGTH = 32;
	private final ThreadLocal<Mac> _digester = new ThreadLocal<Mac>();
	private final SecretKeySpec _digestInitKey;
	private final SidConfiguration config;
	private final int cryptoValueLength;
	  
	  public Crypter(SidConfiguration config, byte[] keyBytes) throws CryptoException //NOSONAR. As SecureKeySpec immediately makes copy of the array it make no sense to do it here
	  {
		this.config = config;
		try {
			this.cryptoValueLength = Mac.getInstance(config.getCryptoAlgorithmName()).getMacLength();
		} catch (NoSuchAlgorithmException e) {
			throw new CryptoException(e);
		}
		if (keyBytes == null) {
			keyBytes = BUILTIN_KEY;
		}
	    this._digestInitKey = new SecretKeySpec(keyBytes, config.getCryptoAlgorithmName());
	  }
	  
	  public Crypter(SidConfiguration config) throws CryptoException {
		  this(config, null);
	  }
	  
	  private Mac initializeDigester()
	    throws CryptoException
	  {
	    try 
	    {
	      Mac hmacDigester = Mac.getInstance(config.getCryptoAlgorithmName());
	      hmacDigester.init(this._digestInitKey); 
	      return hmacDigester;
	    }
	    catch (InvalidKeyException e)
	    {
	      throw new CryptoException(e);
	    }
	    catch (NoSuchAlgorithmException e)
	    {
	      throw new CryptoException(e);
	    }
	  }
	  
	  public int getCryptoValueLength() {
		  return this.cryptoValueLength;
	  }
	  
	  public byte[] computeDigest(String toBeSigned)
	    throws CryptoException
	  {
	    try
	    {
	      Mac hmacDigester = (Mac)this._digester.get();
	      if (hmacDigester == null)
	      {
	        hmacDigester = initializeDigester();
	        this._digester.set(hmacDigester);
	      }
	      
	      byte[] plainText;
	      try
	      {
	        plainText = toBeSigned.getBytes("UTF-8");
	      }
	      catch (UnsupportedEncodingException e) //NOSONAR This exception may normally occur and should not be logged or dealt with in any other way
	      {
	        plainText = toBeSigned.getBytes();
	      }
	      
	      hmacDigester.update(plainText, 0, plainText.length);
	      
	      int size = hmacDigester.getMacLength();
	      byte[] digest = new byte[size];
	      
	      hmacDigester.doFinal(digest, 0);
	      
	      return digest;
	    }
	    catch (ShortBufferException e)
	    {
	      throw new CryptoException(e);
	    }
	  }
}

