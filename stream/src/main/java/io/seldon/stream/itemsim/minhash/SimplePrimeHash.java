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
package io.seldon.stream.itemsim.minhash;

import java.util.Random;
import java.util.Set;

public class SimplePrimeHash implements Hasher {

	static long PRIME = 4294967311L;
	static final int maxID = Integer.MAX_VALUE - 1;
	
	long coefA;
	long coefB;
	
	
	
	public SimplePrimeHash(long coefA, long coefB) {
		super();
		this.coefA = coefA;
		this.coefB = coefB;
	}


	@Override
	public long hash(long in) {
		return (coefA * in + coefB) % PRIME;
	}


	public static Hasher create(Set<Hasher> existing) {
		Random r = new Random();
		Hasher h = null;
		do
		{
			h = new SimplePrimeHash(r.nextInt(maxID),r.nextInt(maxID));
		} while (existing.contains(h));
		return h;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (coefA ^ (coefA >>> 32));
		result = prime * result + (int) (coefB ^ (coefB >>> 32));
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SimplePrimeHash other = (SimplePrimeHash) obj;
		if (coefA != other.coefA)
			return false;
		if (coefB != other.coefB)
			return false;
		return true;
	}

	
	
}
