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

import java.util.ArrayList;
import java.util.List;

public class MinHashCollection {

	List<MinHasher> minHashes = new ArrayList<>();
	
	public MinHashCollection(List<MinHasher> minHashes) {
		this.minHashes = minHashes;
	}

	public void add(long id,long time)
	{
		for(MinHasher mh : minHashes)
			mh.add(id, time);
	}
	
	public List<Long> getMinHashes(long time)
	{
		List<Long> res = new ArrayList<Long>();
		for(MinHasher mh : minHashes)
		{
			Long minHash = mh.getMinHash(time);
			if (minHash == null)
				return new ArrayList<Long>();
			res.add(minHash);
		}
		return res;
	}
	
	public int getCount(long time)
	{
		return minHashes.get(0).getCount(time);
	}
	
	


	
}
