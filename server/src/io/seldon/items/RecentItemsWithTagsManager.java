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

package io.seldon.items;

import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.service.PersistenceProvider;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.general.ItemPeer;
import io.seldon.memcache.DogpileHandler;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

@Component
public class RecentItemsWithTagsManager {
	private static Logger logger = Logger.getLogger(RecentItemsWithTagsManager.class.getName());
	public static final int CACHE_TIME_SECS = 1800;
	
	private final PersistenceProvider persister;
	private final DogpileHandler dogpileHandler;
	
	private final ConcurrentMap<String, ItemTags> clientStores = new ConcurrentHashMap<>();

	private final ConcurrentMap<String, Boolean> loading = new ConcurrentHashMap<>();
	 private BlockingQueue<Runnable> queue = new LinkedBlockingDeque<>();
	 private ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 3, 10, TimeUnit.MINUTES, queue) {
        protected void afterExecute(java.lang.Runnable runnable, java.lang.Throwable throwable) 
        {
        	        	JDOFactory.cleanupPM();
        }
    };
    
    
    @Autowired
    public RecentItemsWithTagsManager(PersistenceProvider persister,DogpileHandler dogpileHandler)
    {
    	this.persister = persister;
    	this.dogpileHandler = dogpileHandler;
    }
    
    private void getRecentItems(final String client,final Set<Long> ids,final int attrId,final String table)
    {
    	 executor.execute(new Runnable() {
             @Override
             public void run() 
             {
            	 final String key = getKey(client,attrId,table);
            	 try
            	 {
            		 ItemPeer iPeer = persister.getItemPersister(client);
            		 ConsumerBean c = new ConsumerBean();
            		 c.setShort_name(client);
            		 logger.info("Loading recent item tags for "+key);
            		 long t1 = System.currentTimeMillis();
            		 Map<Long,List<String>> res = iPeer.getRecentItemTags(ids,attrId,table);
            		 long t2 = System.currentTimeMillis();
            		 logger.info("Loaded recent item tags of size "+res.size()+" for "+key+" in "+(t2-t1)+" msec");
            		 clientStores.put(key, new ItemTags(res,ids));
            	 }
            	 catch(Exception ex)
            	 {
            		 logger.error("Failed to load recent items with key "+key,ex);
            	 }
            	 finally
            	 {
            		 loading.remove(key);
            		 dogpileHandler.updated(key, CACHE_TIME_SECS);
            	 }
             }
    	 });
    }
    
    private String getKey(String client,int attrId,String table)
    {
    	return client + ":" +attrId+":"+table;
    }
    
    public Map<Long,List<String>> retrieveRecentItems(String client, Set<Long> ids,int attrId,String table)
    {
    	final String key = getKey(client, attrId,table);
    	ItemTags items = clientStores.get(key);
		if(items!=null && !ids.equals(items.ids)){
			logger.info("Tag ids mismatch so setting items to null for "+client+" attrId:"+attrId+" table "+table);
			items = null;
		}

    	if (items == null)
    	{
    		if ((loading.putIfAbsent(key, true) == null))
    		{
    			getRecentItems(client, ids, attrId, table);
    		}
    		return new HashMap<>();
    	}
    	else
    	{
			if (dogpileHandler.updateIsRequired(key, items, CACHE_TIME_SECS)) {
    			getRecentItems(client, ids, attrId, table);
    		}

    		return items.itemTags;
    	}
    }
    
    public ItemPeer getRecentItemPeer(String client){
     return null;
    } 
    
	private static class ItemTags {
		Map<Long,List<String>> itemTags;
		Set<Long> ids;

		public ItemTags(Map<Long, List<String>> itemTags,Set<Long> ids) {
			super();
			this.itemTags = itemTags;
			this.ids = ids;
		}
		
	}
}
