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
/**
 * Object to store an ordered set of hash seeds
 * @author rummble
 *
 */
public class MinHashSeed {
	int position;
	int seed;
	public MinHashSeed(int position, int seed) {
		this.position = position;
		this.seed = seed;
	}
	public int getPosition() {
		return position;
	}
	public int getSeed() {
		return seed;
	}
	
	/**
     * Inner class representing Primary Key
     */
    public static class PK implements Serializable
    {
        public int position;
        public int seed;

        public PK()
        {
        }

        public PK(String s)
        {
        	StringTokenizer token = new StringTokenizer (s, "::");
            token.nextToken();               // className
            this.position = Integer.parseInt(token.nextToken());
            this.seed = Integer.parseInt(token.nextToken());
        }

        public String toString()
        {
        	return this.getClass().getName() + "::" + this.position + "::" + this.seed;
        }

        public int hashCode()
        {
            return (((Integer)this.position).hashCode() ^ ((Integer)this.seed).hashCode());
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
                return otherPK.position == position && otherPK.seed == seed;
            }
            return false;
        }
    }
}
