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
 * The derived trust for a member
 * @author clive
 *
 */
public class TrustNetworkMember implements Serializable,Comparable<TrustNetworkMember> {

	/**
	 * 
	 */

	long m;
	Trust t; // transient
	double e; // expected trust probability value 0->1
	int sixd;
	
	public TrustNetworkMember(long m,Trust t,double e,int sixd)
	{
		this.m = m;
		this.t = t;
		this.e = e;
		this.sixd = sixd;
	}
	
	public long getMember() { return m; }
	public Trust getTrust() { return t; }
	public double getE() { return e; }
	public int getSixd() { return sixd; }
	
	public boolean equals(Object obj)
	{
		if (!(obj instanceof TrustNetworkMember))
			return false;
		TrustNetworkMember other = (TrustNetworkMember) obj;
		return (m == other.m);
	}

	@Override
	public int compareTo(TrustNetworkMember o) {
		if (this.e == o.e)
			return 0;
		else if (this.e > o.e)
			return -1;
		else 
			return 1;
	}
	
}
