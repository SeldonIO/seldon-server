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

import io.seldon.api.state.NewClientListener;
import io.seldon.api.state.options.DefaultOptions;
import io.seldon.api.state.zk.ZkClientConfigHandler;

import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PostConstruct;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ClientIdCacheStore implements NewClientListener {
	private static Logger logger = Logger.getLogger(ClientIdCacheStore.class.getName());
	
	private ConcurrentHashMap<String,ClientIdCache> userCaches = new ConcurrentHashMap<>();
	private ConcurrentHashMap<String,ClientIdCache> itemCaches = new ConcurrentHashMap<>();
	
	private static final String PROP_PREFIX = "io.seldon.idstore";
	private static final int DEF_USER_CACHE_SIZE = 10000;
	private static final int DEF_ITEM_CACHE_SIZE = 500;
	
	DefaultOptions options;
	ZkClientConfigHandler clientConfigHandler;
	
	@Autowired
	public ClientIdCacheStore(DefaultOptions options,ZkClientConfigHandler clientConfigHandler)
	{
		this.options = options;
		this.clientConfigHandler = clientConfigHandler;
		clientConfigHandler.addNewClientListener(this, true);
	}
	
	@PostConstruct
	public void initialise()
	{
		String clientProp = options.getOption(PROP_PREFIX+".clients");
		if (clientProp != null)
		{
			String[] clients = clientProp.split(",");
			for(int i=0;i<clients.length;i++)
			{
				String client = clients[i];
				
				addClient(client);;
			}
		}
	}
	
	private void addClient(String client)
	{
		int itemSize = DEF_ITEM_CACHE_SIZE;
		String val = options.getOption(PROP_PREFIX+"."+client+".maxitems");
		if (val != null)
			itemSize = Integer.parseInt(val);
		addItemCache(client, itemSize);
		
		int userSize = DEF_USER_CACHE_SIZE;
		val = options.getOption(PROP_PREFIX+"."+client+".maxusers");
		if (val != null)
			userSize = Integer.parseInt(val);
		addUserCache(client, userSize);
	}
	
	@Override
	public void clientAdded(String client) {
		logger.info("Adding client: "+client);
		addClient(client);
	}

	@Override
	public void clientDeleted(String client) {
		logger.info("Removing client: "+client);
		itemCaches.remove(client);
		userCaches.remove(client);
	}
	
	private void addItemCache(String client,int size)
	{
		logger.info("Adding item id cache of size "+size+" for "+client);
		itemCaches.put(client, new ClientIdCache(client,size));
	}
	
	private void addUserCache(String client,int size)
	{
		logger.info("Adding user id cache of size "+size+" for "+client);
		userCaches.put(client, new ClientIdCache(client,size));
	}
	
	public void putUserId(String client,String external,Long internal)
	{
		if (userCaches.containsKey(client))
			userCaches.get(client).add(external, internal);
	}
	
	public void putItemId(String client,String external,Long internal)
	{
		if (itemCaches.containsKey(client))
			itemCaches.get(client).add(external, internal);
	}
	
	public Long getInternalUserId(String client,String clientId)
	{
		if (userCaches.containsKey(client))
			return userCaches.get(client).getInternal(clientId);
		else
			return null;
	}
	
	public String getExternalUserId(String client,Long userId)
	{
		if (userCaches.containsKey(client))
			return userCaches.get(client).getExternal(userId);
		else
			return null;
	}
	
	public Long getInternalItemId(String client,String clientId)
	{
		if (itemCaches.containsKey(client))
			return itemCaches.get(client).getInternal(clientId);
		else
			return null;
	}
	
	public String getExternalItemId(String client,Long itemId)
	{
		if (itemCaches.containsKey(client))
			return itemCaches.get(client).getExternal(itemId);
		else
			return null;
	}
	
	
	
}
