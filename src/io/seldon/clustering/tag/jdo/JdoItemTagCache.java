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

import io.seldon.clustering.tag.IItemTagCache;
import io.seldon.db.jdo.ClientPersistable;
import io.seldon.memcache.DogpileHandler;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import io.seldon.memcache.UpdateRetriever;
import org.apache.log4j.Logger;

import javax.jdo.Query;
import java.util.HashSet;
import java.util.Set;

public class JdoItemTagCache  extends ClientPersistable implements IItemTagCache {


	private static final int CACHE_SECS = 60 * 60;
	private static Logger logger = Logger.getLogger(JdoItemTagCache.class.getName());
	public JdoItemTagCache(String client)
	{
		super(client);
	}
	
	private static Set<String> getTags(String tagStr)
	{
		String parts[] = tagStr.split(",");
		Set<String> res = new HashSet<String>();
		for(int i=0;i<parts.length;i++)
		{
			res.add(parts[i].trim());
		}
		return res;
	}
	
	ItemTags getTagsForItem(long itemId,int attrId) {
		String sql = "select value from item_map_varchar where item_id=? and attr_id=?";
		Query query = getPM().newQuery( "javax.jdo.query.SQL", sql);
		query.setUnique(true);
		query.setResultClass(String.class);
		String res = (String) query.execute(itemId,attrId);
		query.closeAll();
		if (res != null)
		{
			Set<String> tags = getTags(res);
			return new ItemTags(itemId, tags, System.currentTimeMillis());
		}
		else
			return new ItemTags(itemId, null, System.currentTimeMillis());
	}
	
	@Override
	public Set<String> getTags(final long itemId, final int attrId) {
		String memcacheKey = MemCacheKeys.getItemTags(this.clientName, itemId, attrId);
		ItemTags itemTags = (ItemTags) MemCachePeer.get(memcacheKey);

		ItemTags newerItemTags = null;
		try {
			newerItemTags = DogpileHandler.get().retrieveUpdateIfRequired(memcacheKey, itemTags, new UpdateRetriever<ItemTags>() {
                @Override
                public ItemTags retrieve() throws Exception {
                    return getTagsForItem(itemId, attrId);
                }
            }, CACHE_SECS);
		} catch (Exception e) {
			logger.warn("Couldn't get tags using dogpile handler : ", e);
		}
		if(newerItemTags!=null) {
			MemCachePeer.put(memcacheKey, newerItemTags, CACHE_SECS);
			return newerItemTags.getTags();
		}
		if (itemTags.getTags() != null)
			return itemTags.getTags();
		else
			return new HashSet<String>();
	}

	

}
