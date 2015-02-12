/*
 * Seldon -- open source prediction engine
 * =======================================
 *
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 * ********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ********************************************************************************************
 */

/*
 * Created on 23-Mar-2004
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package io.seldon.memcache;

/**
 * @author Administrator
 *
 * To change the template for this generated type comment go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Date;
import java.util.Random;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.log4j.Logger;

public class SecurityHashPeer {
	private static Logger logger = Logger.getLogger( SecurityHashPeer.class.getName() );

	private static SecureRandom prng = null;
    private static MessageDigest sha = null;

	private static long seqNumber = new Date().getTime();

	public static synchronized String getNextSeqNumber()
	{
		return Long.toString( seqNumber++ );
	}

	public static synchronized void initialise()
	{
		if (prng == null)
		{
			try
			{
				prng = SecureRandom.getInstance("SHA1PRNG");
				//get its digest
                sha = MessageDigest.getInstance("SHA-1");
                logger.info("Initialised random generator and digest");
			}
			catch ( NoSuchAlgorithmException ex ) {
				logger.error("Can't get crypotographic algorithm", ex);
	  		}
		}
	}

    public static String md5(String value)
    {
    	try
    	{
    		MessageDigest md5 = MessageDigest.getInstance("MD5");
    		byte[] result =  md5.digest( value.getBytes() );
    		return hexEncode(result);
    	} catch (NoSuchAlgorithmException e) {
			logger.error("Failed to get MD5 digest ",e);
			return value;
		}
    }

    /**
     * Uses {@link DigestUtils}.
     * @param input - string to hash
     * @return a hex encoded string
     */
    public static String md5digest(String input) {
        return DigestUtils.md5Hex(input);
    }
    
	public static synchronized String digest(String value)
	{
	  	if (sha != null)
	 	{
          byte[] result =  sha.digest( value.getBytes() );
			
          return hexEncode(result);
	  	}
	  	else
	  	{
              final String message = "Can't do operation as no sha digest available";
              logger.error(message, new Exception(message));
	  		return "";
	  	}
		
	}
	

  public static synchronized String getNewId() 
  {
  	if ((prng != null) && (sha != null))
 	{
  		//generate a random number
		String randomNum = new Integer( prng.nextInt() ).toString();
		byte[] result =  sha.digest( randomNum.getBytes() );

		return hexEncode(result);
  	}
  	else
  	{
  		logger.warn("Returning non-secure possibly non-unqiue random identifier");
		Random generator = new Random();
		String randomKey = "" + generator.nextInt(99999999);
		return hexEncode(randomKey.getBytes());
  	}
  }

  /**
  * The byte[] returned by MessageDigest does not have a nice
  * textual representation, so some form of encoding is usually performed.
  *
  * This implementation follows the example of David Flanagan's book
  * "Java In A Nutshell", and converts a byte array into a String
  * of hex characters.
  *
  * Another popular alternative is to use a "Base64" encoding.
  */
  static public String hexEncode( byte[] aInput){
	StringBuffer result = new StringBuffer();
	final char[] digits = {'0', '1', '2', '3', '4','5','6','7','8','9','a','b','c','d','e','f'};
	for ( int idx = 0; idx < aInput.length; ++idx) {
	  byte b = aInput[idx];
	  result.append( digits[ (b&0xf0) >> 4 ] );
	  result.append( digits[ b&0x0f] );
	}
	return result.toString();
  }
  
  
  public static byte[] toByteArray(long foo)
  {
    return toByteArray(foo, new byte[8]);
  }
  
  private static byte[] toByteArray(long foo, byte[] array)
  {
    for (int iInd = 0; iInd < array.length; ++iInd)
    {
      array[iInd] = (byte) ((foo >> (iInd*8)) % 0xFF);
    }
 
    return array;
  }
  
} 


