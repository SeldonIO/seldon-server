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

package io.seldon.clustering.recommender.jdo;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.db.jdo.ClientPersistable;
import org.apache.log4j.Logger;

public class FBToUserDimStore  extends ClientPersistable {

	private static Logger logger = Logger.getLogger(FBToUserDimStore.class.getName());

	double alpha = 1;
	
	public FBToUserDimStore(String client)
	{
		super(client);
	}
	
	public Map<String,Set<Integer>> getMappings()
	{
		final PersistenceManager pm = getPM();
		Map<String,Set<Integer>> map = new ConcurrentHashMap<String,Set<Integer>>();
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select category,dim_id from fbcat_map" );
		Collection<Object[]> results = (Collection<Object[]>) query.execute();
		for(Object[] res : results)
		{
			String category = (String) res[0];
			Integer dim = (Integer) res[1];
			Set<Integer> dims;
			if (!map.containsKey(category))
				dims = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
			else
				dims = map.get(category);
			dims.add(dim);
			map.put(category, dims);			
		}
		return map;
	}
	
}
