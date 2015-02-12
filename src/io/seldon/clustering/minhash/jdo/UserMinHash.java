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

package io.seldon.clustering.minhash.jdo;

import java.io.Serializable;
import java.util.StringTokenizer;

public class UserMinHash {

	String hash;
	long user;
	public UserMinHash(String hash, long user) {
		this.hash = hash;
		this.user = user;
	}
	public String getHash() {
		return hash;
	}
	public long getUser() {
		return user;
	}

	/**
     * Inner class representing Primary Key
     */
    public static class PK implements Serializable
    {
        public String hash;
        public long user;

        public PK()
        {
        }

        public PK(String s)
        {
        	StringTokenizer token = new StringTokenizer (s, "::");
            token.nextToken();               // className
            this.hash = token.nextToken();
            this.user = Long.parseLong(token.nextToken());
        }

        public String toString()
        {
        	return this.getClass().getName() + "::" + this.hash + "::" + this.user;
        }

        public int hashCode()
        {
            return (this.hash.hashCode() ^ ((Long)this.user).hashCode());
        }

        public boolean equals(Object other)
        {
        	if (other == this)
	        {
	            return true;
	        }
            if (other != null && (other instanceof PK))
            {
                PK otherPK = (PK)other;
                return otherPK.hash.equals(hash) && otherPK.user == user;
            }
            return false;
        }
    }
}
