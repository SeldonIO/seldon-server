/*
 * Seldon -- open source prediction engine
 * =======================================
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 **********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at       
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ********************************************************************************************** 
*/
package io.seldon.vw;

import org.apache.commons.lang.StringUtils;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class VwFeatureHash {

	final static int VW_CONSTANT_HASH = 11650396;
	final int mask;
	final int stride;
	
	public VwFeatureHash(int bits,int oaa)
	{
		mask = Math.round((float)Math.pow(2, bits) - 1);
		stride = Math.round((float)Math.pow(2,Math.ceil(log2(oaa,2))));
		System.out.println("Stide is "+stride);
	}
	
	private double log2(int val,int base)
	{
		return Math.log(val) / Math.log(base);
	}
	
	private boolean isInteger(String s) {
	    return isInteger(s,10);
	}

	private boolean isInteger(String s, int radix) {
	    if(s.isEmpty()) return false;
	    for(int i = 0; i < s.length(); i++) {
	        if(i == 0 && s.charAt(i) == '-') {
	            if(s.length() == 1) return false;
	            else continue;
	        }
	        if(Character.digit(s.charAt(i),radix) < 0) return false;
	    }
	    return true;
	}
	
	
	public Integer getFeatureHash(int label,String namespace,String feature)
	{
		int nsHash = 0;
		if (!StringUtils.isEmpty(namespace))
		{
			HashFunction h = Hashing.murmur3_32(0);
			nsHash = h.hashBytes(namespace.getBytes()).asInt();
		}
		int hcl = 0;
		if (isInteger(feature))
			hcl = Integer.parseInt(feature) + nsHash;
		else
		{
			HashFunction h = Hashing.murmur3_32(nsHash);
			hcl = (h.hashBytes(feature.getBytes()).asInt());
		}
		int f = ((hcl * stride) + label - 1) & mask;
		return f;
	}
	
	public Integer getConstantHash(int label)
	{
		int hash_oaa = VW_CONSTANT_HASH * stride;
		int f = (hash_oaa + label - 1) & mask;
		return f;
	}
	
	public static void main(String[] args)
	{
		VwFeatureHash hasher = new VwFeatureHash(18,2);
		int label = 1;
		Integer hcl = hasher.getFeatureHash(label,"f","101");
		System.out.println("code="+hcl);
		Integer hcon = hasher.getConstantHash(label);
		System.out.println("Constant="+hcon);
	}
	
}
