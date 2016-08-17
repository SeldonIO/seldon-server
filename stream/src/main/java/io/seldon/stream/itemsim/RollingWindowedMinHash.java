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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class RollingWindowedMinHash implements MinHasher {
	
	List<MinHashEntry> hashes = new ArrayList<>();
	Hasher hasher;
	long window;
	
	public RollingWindowedMinHash(Hasher h,long window)
	{
		this.hasher = h;
		this.window = window;
	}
		
	@Override
	public synchronized void add(long id,long time)
	{
		long hash = hasher.hash(id);
		int idx = hashes.size() - 1;
		int cSum = 0;
		while(idx >= 0 && hash < hashes.get(idx).minHash) 
		{
			cSum += hashes.get(idx).count;
			hashes.remove(idx);
			idx--;
		}
		if (idx > -1)
		{
			int lastIdx = hashes.size() - 1;
			if (hash == hashes.get(lastIdx).minHash)
			{
				hashes.get(lastIdx).time = time;
				hashes.get(lastIdx).count = hashes.get(lastIdx).count + cSum + 1;
			}
			else
				hashes.add(new MinHashEntry(hash,time,cSum+1));
		}
		else
		{
			hashes = new ArrayList<>();
			hashes.add(new MinHashEntry(hash,time,cSum + 1));
		}
		this.removeOldEntries(time);
		//System.out.println(hashes.size());
	}
	
	private void removeOldEntries(long time)
	{
		final long start_t = time - window;
		hashes.removeIf(new Predicate<MinHashEntry>() {
			@Override
			public boolean test(MinHashEntry t) {
				return (t.time <= start_t);
			}
		});
	}
	
	public static class MinHashEntry 
	{
		long minHash;
		long time;
		int count;
		
		public MinHashEntry(long minHash, long time, int count) {
			super();
			this.minHash = minHash;
			this.time = time;
			this.count = count;
		}

		@Override
		public String toString() {
			return "MinHashEntry [minHash=" + minHash + ", time=" + time
					+ ", count=" + count + "]";
		}
		
		
	}

	@Override
	public synchronized Long getMinHash(long time) {
		this.removeOldEntries(time);
		if (hashes.size() > 0)
			return hashes.get(0).minHash;
		else
			return null;
	}

	@Override
	public synchronized int getCount(long time) {
		this.removeOldEntries(time);
		int sum = 0;
		for(MinHashEntry e : hashes)
		{
			sum += e.count;
		}
		return sum;
	}

	@Override
	public String toString() {
		return "RollingWindowedMinHash [hashes=" + hashes + ", window="
				+ window + "]";
	}
	
	
	
}
