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

package io.seldon.similarity.tagcount.jdo;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.jdo.Query;

import org.apache.log4j.Logger;

import io.seldon.api.TestingUtils;
import io.seldon.db.jdo.ClientPersistable;
import io.seldon.similarity.tagcount.ITagCountPeer;

public class SqlTagCountPeer  extends ClientPersistable implements ITagCountPeer{
	private static Logger logger = Logger.getLogger( SqlTagCountPeer.class.getName() );
	public SqlTagCountPeer(String clientName) {
		super(clientName);
	}

	private Map<String,Set<Long>> getTopItemsFromClusters(int maxItems,int attrId,int dimension) 
	{
		String sql = "select c.item_id,imv.value from cluster_counts c join item_map_varchar imv on (c.item_id=imv.item_id and imv.attr_id="+attrId+") join item_map_enum ime on (c.item_id=ime.item_id) join dimension d on (ime.attr_id=d.attr_id and ime.value_id=d.value_id and d.dim_id="+dimension+") group by item_id order by sum(exp(-(greatest(?-t,0)/?))*count) desc limit "+maxItems;
		Query query = getPM().newQuery( "javax.jdo.query.SQL", sql);
		Collection<Object[]> results = (Collection<Object[]>) query.execute(TestingUtils.getTime(),43200);
		Map<String,Set<Long>> res = createResult(results);
		query.closeAll();
		return res;
	}
	
	private Map<String,Set<Long>> getRecentItems(int maxItems,int attrId,int dimension) 
	{
		String sql;
		if (TestingUtils.get().getTesting())
		{
			sql = "select i.item_id,imv.value from items i join item_map_varchar imv on (i.item_id=imv.item_id and imv.attr_id="+attrId+") join item_map_enum ime on (i.item_id=ime.item_id) join dimension d on (d.dim_id="+dimension+" and d.attr_id=ime.attr_id and d.value_id=ime.value_id) where unix_timestamp(i.first_op)<"+TestingUtils.getTime()+" order by i.item_id desc limit "+maxItems;				
		}
		else
		{
			sql = "select i.item_id,imv.value from items i join item_map_varchar imv on (i.item_id=imv.item_id and imv.attr_id="+attrId+") join item_map_enum ime on (i.item_id=ime.item_id) join dimension d on (d.dim_id="+dimension+" and d.attr_id=ime.attr_id and d.value_id=ime.value_id) order by i.item_id desc limit "+maxItems;	
		}
		
		logger.info("About to run "+sql);
		Query query = getPM().newQuery( "javax.jdo.query.SQL", sql);
		Collection<Object[]> results = (Collection<Object[]>) query.execute();
		Map<String,Set<Long>> res = createResult(results);
		query.closeAll();
		return res;
	}
	
	private Map<String,Set<Long>> createResult(Collection<Object[]> results)
	{
		HashMap<String,Set<Long>> map = new HashMap<String,Set<Long>>();
		for(Object[] res : results)
		{
			Long itemId = (Long) res[0];
			String tags = (String) res[1];
			String[] parts = tags.split(",");
			for(int i=0;i<parts.length;i++)
			{
				String tag = parts[i];
				Set<Long> items = map.get(tag);
				if (items == null)
				{
					items = new HashSet<Long>();
					map.put(tag, items);
				}
				items.add(itemId);
			}
		}
		return map;
	}
	
	@Override
	public Map<String, Set<Long>> getRecentItemTags(int maxItems,int attrId,int dimension) {
		Map<String,Set<Long>> map = getTopItemsFromClusters(maxItems, attrId, dimension);
		if (map.size() > 0)
		{
			logger.debug("Returning item tags based on clusters");
			return map;
		}
		else
		{
			logger.debug("Returning item tags based on recency");
			return getRecentItems(maxItems, attrId, dimension);
		}
	}

	@Override
	public Map<String, Integer> getUserTags(long userId) {
		String sql = "select tag,count from user_tag where user_id=?";
		Query query = getPM().newQuery( "javax.jdo.query.SQL", sql);
		Collection<Object[]> results = (Collection<Object[]>) query.execute(userId);
		Map<String,Integer> map = new HashMap<String,Integer>();
		for(Object[] res : results)
		{
			String tag = (String) res[0];
			Integer count = (Integer) res[1];
			map.put(tag, count);
		}
		query.closeAll();
		return map;
	}

	

}
