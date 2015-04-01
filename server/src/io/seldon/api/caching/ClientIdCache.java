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

package io.seldon.api.caching;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

public class ClientIdCache {

	ConcurrentLinkedHashMap<String,Long> externalToInternal;
	ConcurrentLinkedHashMap<Long,String> internalToExternal;
	
	public ClientIdCache(String client,int cacheSize)
	{
		internalToExternal = new ConcurrentLinkedHashMap.Builder<Long, String>()
	    .maximumWeightedCapacity(cacheSize)
	    .build();
		
		externalToInternal = new ConcurrentLinkedHashMap.Builder<String, Long>()
	    .maximumWeightedCapacity(cacheSize)
	    .build();
	}
	
	public void add(String external,long internal)
	{
		externalToInternal.put(external, internal);
		internalToExternal.put(internal, external);
	}
	
	public Long getInternal(String external)
	{
		return externalToInternal.get(external);
	}
	
	public String getExternal(Long internal)
	{
		return internalToExternal.get(internal);
	}
}
