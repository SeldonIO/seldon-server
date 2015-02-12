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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import io.seldon.api.APIException;
import io.seldon.api.Util;
import io.seldon.db.jdo.ClientPersistable;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import net.spy.memcached.CASMutation;

import org.apache.log4j.Logger;

public class ActionHistoryCache extends ClientPersistable {

	private static Logger logger = Logger.getLogger(ActionHistoryCache.class.getName());
	public static int CACHE_TIME = 1800;
	
	private static ConcurrentHashMap<String,Boolean> clients = new ConcurrentHashMap<String,Boolean>();
	private static ConcurrentHashMap<String,Boolean> clientsUseDb = new ConcurrentHashMap<String,Boolean>();	
	
	public static void initalise(Properties props)
	{
		String timeoutStr = props.getProperty("io.seldon.actioncache.timeout");
		if (timeoutStr != null && !"".equals(timeoutStr))
			CACHE_TIME = Integer.parseInt(timeoutStr);
		logger.info("Action History cache timeout set to "+CACHE_TIME);
		
		String clientStr = props.getProperty("io.seldon.actioncache.clients");
		if (clientStr != null)
		{
			String clientsActive[] = clientStr.split(",");
			for(int i=0;i<clientsActive.length;i++)
			{
				logger.info("Activating Action History Cache for client "+clientsActive[i]);
				clients.put(clientsActive[i], true);
				String useDb = props.getProperty("io.seldon.actioncache."+clientsActive[i]+".usedb");
				if (useDb != null && "true".equals(useDb))
				{
					logger.info("Adding client "+clientsActive[i]+" to list of clients to use db to get action history");
					clientsUseDb.put(clientsActive[i], true);
				}
			}
		}
	}
	
	public static boolean isActive(String client)
	{
		Boolean val = clients.get(client);
		if (val != null)
			return val;
		else
			return false;
	}
	
	public static void setClient(String client,boolean val)
	{
		clients.put(client, val);
	}
	
	
	public ActionHistoryCache(String client)
	{
		super(client);
	}
	
	public void removeRecentActions(long userId)
	{
		String mkey = MemCacheKeys.getActionHistory(this.clientName, userId);
		MemCachePeer.delete(mkey);
	}
		
	public List<Long> getRecentActions(long userId,int numActions)
	{
		String mkey = MemCacheKeys.getActionHistory(this.clientName, userId);
		List<Long> res = (List<Long>) MemCachePeer.get(mkey);
		if (res == null)
		{
			if (clientsUseDb.containsKey(clientName))
			{
				List<Long> transaction = Util.getActionPeer(getPM()).getRecentUserActions(userId);
				res = new ArrayList<Long>(transaction);
				MemCachePeer.put(mkey, res, CACHE_TIME);
				logger.debug("Stored action history for user "+userId+" in memcache");
			}
			else
			{
				logger.debug("creating empty action history for user "+userId+" for client "+clientName);
				res = new ArrayList<Long>();
			}
		}
		else 
		{
			if (res.size() > numActions)
				res = res.subList(0, numActions);
			logger.debug("Got action history for user "+userId+" from memcache");
		}
		return res;
	}
	
	public void addAction(long userId,final long itemId) throws APIException
    {
		logger.debug("Adding action to cache for "+userId+" item "+itemId);
        CASMutation<List<Long>> mutation = new CASMutation<List<Long>>() {

            // This is only invoked when a value actually exists.
            public List<Long> getNewValue(List<Long> current) {
            	if(!current.contains(itemId)) {
	                current.add(0, itemId);
            	}
	            return current;
            }
        };
        List<Long> actions = new ArrayList<Long>();
        actions.add(itemId);
        String mkey = MemCacheKeys.getActionHistory(this.clientName, userId);
        List<Long> res = MemCachePeer.cas(mkey, mutation, actions,CACHE_TIME);

        // if there is just 1 action added..check there are not older actions in DB and if so add them
        // Can cause issues if load is high
        if(res != null && res.size() == 1 && clientsUseDb.containsKey(clientName)) {
        	logger.debug("Updating actions as maybe new or expired");
            List<Long> transaction = null;
            transaction = Util.getActionPeer(getPM()).getRecentUserActions(userId);
            if(transaction != null && !transaction.isEmpty())
            {
            	ArrayList<Long> transNew = new ArrayList<Long>(transaction);
            	if(!transaction.contains(res.get(0))) {
            		 transNew.add(0,res.get(0));
            	}
                MemCachePeer.put(mkey, transNew,CACHE_TIME);
                logger.debug("Updated action history for user "+userId+" in memcache after db refresh");
            }
        }
       
    }
	
	
}
