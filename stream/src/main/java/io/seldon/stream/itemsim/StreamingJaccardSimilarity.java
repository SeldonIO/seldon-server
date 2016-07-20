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

import io.seldon.stream.itemsim.MinHashCollections.State;
import io.seldon.stream.itemsim.minhash.Hasher;
import io.seldon.stream.itemsim.minhash.SimplePrimeHash;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class StreamingJaccardSimilarity {

	MinHashCollections mhcs;
	
	public StreamingJaccardSimilarity(int windowSizeSecs,int numHashes,int minActivity)
	{
		Set<Hasher> existing = new HashSet<>();
		for(int i=0;i<numHashes;i++)
		{
			Hasher h = SimplePrimeHash.create(existing);
			existing.add(h);
		}
		List<Hasher> hashes = new ArrayList<Hasher>(existing);
		MinHasherFactory f = new RollingWindowedMinHashFactory(hashes);
		this.mhcs = new MinHashCollections(f, windowSizeSecs, minActivity);
	}
	
	public void add(long itemId,long userId,long timeSecs)
	{
		mhcs.add(itemId, userId, timeSecs);
	}
	
	public List<JaccardSimilarity> getSimilarity(long timeSecs)
	{
		List<JaccardSimilarity> res = new ArrayList<>();
		List<State> states = mhcs.getAllMinHashes(timeSecs);
		for(State s1 : states)
			for(State s2 : states)
			{
				if (s1.id < s2.id)
				{
					float jaccard = s1.jaccardEstimate(s2);
					if (jaccard > 0)
						res.add(new JaccardSimilarity(s1.id, s2.id, jaccard));
				}
			}
		return res;
	}
	
}
