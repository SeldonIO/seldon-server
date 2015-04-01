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

package io.seldon.trust.impl.jdo;

import java.io.Serializable;
import java.util.StringTokenizer;

public class MemberTrustStateKey implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2069769234439881906L;
	public long srcUser;
	public int type;
	public long dstUser;
	
	public MemberTrustStateKey()
	{
		
	}
	
	public MemberTrustStateKey(String value) 
	{
		StringTokenizer token = new StringTokenizer (value, "::");
        token.nextToken();               // className
        this.srcUser = Long.parseLong(token.nextToken()); 
        this.type = Integer.parseInt(token.nextToken());
        this.dstUser = Long.parseLong(token.nextToken()); 
	}
	
	 public boolean equals(Object obj)
	 {
		 if (obj == this)
		 {
			 return true;
		 }
		 if (!(obj instanceof MemberTrustStateKey))
		 {
			 return false;
		 }
		 MemberTrustStateKey mts = (MemberTrustStateKey)obj;

		 return srcUser == mts.srcUser && type == mts.type && dstUser == mts.dstUser;
	 }

	 public int hashCode ()
	 {
		 return ((Long)srcUser).hashCode() ^ ((Integer)type).hashCode() ^ ((Long)dstUser).hashCode();
	 }

	 public String toString ()
	 {
		 return this.getClass().getName() + "::"  + this.srcUser + "::" + this.type + "::" + this.dstUser;
	 }
	
}
