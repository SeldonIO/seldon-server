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

package io.seldon.clustering.tag.jdo;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.clustering.tag.ITagClusterCountStore;
import io.seldon.db.jdo.ClientPersistable;

public class JdoTagClusterCountStore extends ClientPersistable implements ITagClusterCountStore {

	public JdoTagClusterCountStore(String client)
	{
		super(client);
	}
	
	@Override
	public Map<Long, Double> getTopCounts(String tag) {
		final PersistenceManager pm = getPM();
		Map<Long,Double> map = new HashMap<Long,Double>();
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select item_id,count from tag_cluster_counts where tag=?" );
		Collection<Object[]> res = (Collection<Object[]>)  query.execute(tag);
		for(Object[] r : res)
		{
			Long itemId = (Long) r[0];
			Double count = (Double) r[1];
			map.put(itemId, count);
		}
		return map;
	}

	@Override
	public Map<Long, Double> getTopCountsForDimension(String tag,
			int dimension, int maxCounts,double decay) {
		final PersistenceManager pm = getPM();
		Map<Long,Double> map = new HashMap<Long,Double>();
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select t.item_id,exp(-(greatest(unix_timestamp()-t,0)/?))*count as decayedCount from tag_cluster_counts t join item_map_enum ime on (t.item_id=ime.item_id) natural join dimension d where tag=? and d.dim_id=? order by decayedCount desc limit "+maxCounts );
		Collection<Object[]> res = (Collection<Object[]>)  query.execute(decay,tag,dimension);
		for(Object[] r : res)
		{
			Long itemId = (Long) r[0];
			Double count = (Double) r[1];
			map.put(itemId, count);
		}
		return map;
	}

	

}
