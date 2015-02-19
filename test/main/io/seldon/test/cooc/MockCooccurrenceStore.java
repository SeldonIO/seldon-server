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

package io.seldon.test.cooc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.seldon.cooc.CooccurrenceCount;
import io.seldon.cooc.CooccurrencePeer;
import io.seldon.cooc.ICooccurrenceStore;

public class MockCooccurrenceStore implements ICooccurrenceStore {

	Map<String,CooccurrenceCount> map;
	
	
	
	public MockCooccurrenceStore(Map<String, CooccurrenceCount> map) {
		super();
		this.map = map;
	}
	
	public void updateMap(Map<String, CooccurrenceCount> map)
	{
		this.map = map;
	}

	@Override
	public Map<String, CooccurrenceCount> getCounts(List<Long> item1,
			List<Long> item2) {
		Map<String,CooccurrenceCount> r = new HashMap<>();
		for(Long i1 : item1)
			for(Long i2 : item2)
			{
				String key = CooccurrencePeer.getKey(i1, i2);
				if (map.containsKey(key))
					r.put(key, map.get(key));
			}
		return r;
	}

	@Override
	public Map<String, CooccurrenceCount> getCounts(List<Long> item1) {
		Map<String,CooccurrenceCount> r = new HashMap<>();
		for(Long i1 : item1)
		{
			String key = CooccurrencePeer.getKey(i1, i1);
			if (map.containsKey(key))
				r.put(key, map.get(key));
		}
		return r;
	}

}
