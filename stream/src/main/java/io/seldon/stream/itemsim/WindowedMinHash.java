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
package io.seldon.stream.itemsim;

import io.seldon.stream.itemsim.minhash.Hasher;

public class WindowedMinHash implements MinHasher {
	
	Hasher hasher;
	long windowStart;
	long windowEnd;
	Long minHash = null;
	int additions = 0;
	
	public WindowedMinHash(Hasher h,long windowStart,long windowEnd)
	{
		this.hasher = h;
		this.windowStart = windowStart;
		this.windowEnd = windowEnd;
	}

	@Override
	public void add(long id, long time) 
	{
		if (time >= windowStart && time < windowEnd)
		{
			long hash = hasher.hash(id);
			if (minHash == null || hash < minHash)
				minHash = hash;
			additions++;
		}
	}

	@Override
	public Long getMinHash(long time) {
		if (time >= windowStart && time < windowEnd)
			return minHash;
		else
			return null;
	}

	@Override
	public int getCount(long time) {
		return additions;
	}
	
	
}
