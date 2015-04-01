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

public class MemberContentTrustStateKey implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2069769234439881906L;
	public long srcContent;
	public int type;
	public long dstContent;
	
	public MemberContentTrustStateKey()
	{
		
	}
	
	public MemberContentTrustStateKey(String value) 
	{
		StringTokenizer token = new StringTokenizer (value, "::");
        token.nextToken();               // className
        this.srcContent = Long.parseLong(token.nextToken()); 
        this.type = Integer.parseInt(token.nextToken());
        this.dstContent = Long.parseLong(token.nextToken()); 
	}
	
	 public boolean equals(Object obj)
	 {
		 if (obj == this)
		 {
			 return true;
		 }
		 if (!(obj instanceof MemberContentTrustStateKey))
		 {
			 return false;
		 }
		 MemberContentTrustStateKey mts = (MemberContentTrustStateKey)obj;

		 return srcContent == mts.srcContent && type == mts.type && dstContent == mts.dstContent;
	 }

	 public int hashCode ()
	 {
		 return ((Long)srcContent).hashCode() ^ ((Integer)type).hashCode() ^ ((Long)dstContent).hashCode();
	 }

	 public String toString ()
	 {
		 return this.getClass().getName() + "::"  + this.srcContent + "::" + this.type + "::" + this.dstContent;
	 }

}
