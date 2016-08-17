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
import java.util.concurrent.ConcurrentHashMap;

public class MinHashCollections {

	ConcurrentHashMap<Long,MinHashCollection> mhcs = new ConcurrentHashMap<>();
	MinHasherFactory mhFactory;
	long window;
	int minActivity;
	
	public MinHashCollections(MinHasherFactory mhFactory,long window,int minActivity)
	{
		this.mhFactory = mhFactory;
		this.window = window;
		this.minActivity = minActivity;
	}
	
	public void add(long item,long user,long time)
	{
		if (!mhcs.containsKey(item))
			mhcs.putIfAbsent(item, new MinHashCollection(mhFactory.create(window)));
		MinHashCollection mhc = mhcs.get(item);
		mhc.add(user, time);
	}
	
	public List<State> getAllMinHashes(long time)
	{
		List<State> states = new ArrayList<>();
		for(Long id : mhcs.keySet())
		{
			if (mhcs.get(id).getCount(time) >= minActivity)
			{
				List<Long> mh = mhcs.get(id).getMinHashes(time);
				if (mh != null && mh.size() > 0)
					states.add(new State(id,mh));
			}
		}
		System.out.println("Raw number of minHashes "+mhcs.size()+" but will return "+states.size()+" with minActiviy filter at "+minActivity);
		return states;
	}
	
	public static class State
	{
		long id;
		List<Long> minHashes;
		public State(long id, List<Long> minHashes) 
		{
			super();
			this.id = id;
			this.minHashes = minHashes;
		}
		public float jaccardEstimate(State other)
		{
			int overlap = 0;
			for(int i=0;i<minHashes.size();i++)
			{
				if (minHashes.get(i).equals(other.minHashes.get(i)))
					overlap++;
			}
			return overlap/(float)minHashes.size();
		}
	}
	
}
