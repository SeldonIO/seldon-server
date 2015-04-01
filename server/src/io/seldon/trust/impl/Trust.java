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

package io.seldon.trust.impl;

import java.io.Serializable;

/**
 * See the deatils of the algorithm at http://wiki.playtxt.net/TrustAlgorithm
 * 
 * This is the core Trust class that holds the belief,disbelief,uncertainty, and base rates for 2 members references by their (db) indexes
 * @author clive
 * 
 *
 */
public class Trust implements Serializable {
	
	// bidirectional - b,d,u are reciprocal
	// As trust is based on shared ratings of content if I distruct you, you will distrust me (modulo the base rate)
	public static final int TYPE_GENERAL = 0;
	public int type;
	public long m1; // work with jdokeys of members
	public long m2;
	// b + d + u = 1
	public double b; // belief (0,1)
	public double d; // disbelief (0,1)
	public double u; // uncertainty (0,1)
	public double aM1; // base rate trust of m1 (0,1)
	public double aM2; // base rate trust of m2 (0,1)	
	public boolean friends;
	
	
	public Trust(int type,long m1,long m2,double b,double d,double u,double aM1,double aM2,boolean friends)
	{
		this.type = type;
		this.m1 = m1;
		this.m2 = m2;
		this.b = b;
		this.d = d;
		this.u = u;
		this.aM1 = aM1;
		this.aM2 = aM2;
		this.friends = friends;
	}
	
	
	/**
	 *  Created from indirect shared opinion (not persisted)
	 */
	public Trust(int type,long m1,long m2,double b,double d,double u,double a)
	{
		this.type = type;
		this.m1 = m1;
		this.m2 = m2;
		this.b = b;
		this.d = d;
		this.u = u;
		this.aM1 = a;
		this.friends = false;
	}
	
	/**
	 * Created from new shared opinion
	 */ 
	public Trust(int type,long m1,long m2,double r,double s,double baseRate)
	{
		this.type = type;
		this.m1 = m1 <= m2 ? m1 : m2;
		this.m2 = m1 <= m2 ? m2 : m1;
		updateBelief(r,s);
		// neutral base rate?
		this.aM1 = baseRate;
		this.aM2 = baseRate;
		this.friends = false;
	}
	
	/**
	 * Created from mutual friendship
	 */ 
	public Trust(int type,long m1,double baseRateM1,long m2,double baseRateM2)
	{
		this.type = type;
		this.m1 = m1 <= m2 ? m1 : m2;
		this.m2 = m1 <= m2 ? m2 : m1;
		this.b = 0;
		this.d = 0;
		this.u = 1;
		this.aM1 = m1 <= m2 ? baseRateM1 : baseRateM2;
		this.aM2 = m1 <= m2 ? baseRateM2 : baseRateM1;
		this.friends = true;
	}
	

	/**
	 * Base rates derived from initial trust for members
	 */
	public void updateBaseTrust(long m,double baseRateM)
	{
		if (m1 == m)
			aM1 = baseRateM;
		else
			aM2 = baseRateM;
	}
	
	/**
	 * 
	 * @param r - agreement
	 * @param s - disagreement
	 */
	public void updateBelief(double r,double s)
	{
		double den = r+s+2;
		this.b = r/den;
		this.d = s/den;
		this.u = 2/den;
	}
	
	public String toString()
	{
		return ""+m1+"->"+m2+" b:" + b + " d:"+d+" u:"+u + " aM1:"+aM1+" aM2:"+aM2;
	}
	
	public double getExpectationTrustValueSrc(Long src)
	{
		// Get correct direction
		if (src == m1)
			return b + (u * aM1);
		else
			return b + (u * aM2);
	}
	public double getExpectationTrustValueDst(Long src)
	{
		// Get correct direction
		if (src == m2)
			return b + (u * aM1);
		else
			return b + (u * aM2);
	}
	
	public double getExpectationTrustValueIndirect()
	{
		return b + (u * aM1);
	}
	
	public long getM1() { return m1; }
	public long getM2() { return m2; }

	public boolean isFriends() {
		return friends;
	}

	public void setFriends(boolean friends) {
		this.friends = friends;
	}
	
	private static int generateHashCode(long m1,long m2,int type)
	{
		StringBuffer st = new StringBuffer();
		if (m1 <= m2)
			st.append(m1);
		else
			st.append(m2);
		st.append(":");
		if (m1 <= m2)
			st.append(m2);
		else
			st.append(m1);
		st.append(":");
		st.append(type);
		return st.toString().hashCode();
	}
	
	public static int getHashCode(long m1,int type)
	{
		return generateHashCode(m1,-1,type);
	}
	
	public static int getHashCode(long m1,long m2,int type)
	{
		return generateHashCode(m1,m2,type);
	}
	
	public int hashCode()
	{
		return Trust.generateHashCode(m1, m2, type);
	}
	
	public boolean equals(Object other)
	{
		if (!(other instanceof Trust))
			return false;
		else
		{
			Trust to = (Trust) other;
			if (to.m1 == m1 && to.m2 == m2 && to.type == type)
				return true;
			else
				return false;
		}
	}
	
	public int compareTo(Object other) {
		Trust to = (Trust) other;
		int skey = ((Long) m1).compareTo(to.m1);
		if (skey != 0)
			return skey;
		else
		{
			skey = ((Long) m2).compareTo(to.m2);
			if (skey != 0)
				return skey;
			else
				return ((Integer) type).compareTo(to.type);
				
		}
		
	}
	
}
