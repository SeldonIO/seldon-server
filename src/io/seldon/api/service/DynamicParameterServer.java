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

package io.seldon.api.service;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import io.seldon.api.resource.ListBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import org.apache.log4j.Logger;
import org.datanucleus.util.StringUtils;

import io.seldon.api.resource.DynamicParameterBean;

public class DynamicParameterServer {

	private static Logger logger = Logger.getLogger( DynamicParameterServer.class.getName() );
	private static Map<String,Map<String, String>> dynamicParameters = new ConcurrentHashMap<String,Map<String, String>>();
	private static final long REFRESH_TIME = 600;
	private static final int RELOAD_TIME = 120;
	private static Map<String,Long> lastUpdate = new ConcurrentHashMap<String,Long>();
	
	public enum Parameter { AB_TESTING }
	
	//GET PARAM FROM MEMORY
	//REFRESH PARAMETER FROM CACHE PERIODICALLY
	public static String getParameter(String client, String name) {
		Long lastUpdateClient = lastUpdate.get(client);
		if(lastUpdateClient == null || (System.currentTimeMillis() - lastUpdateClient)/1000 > REFRESH_TIME) {
			refreshParametersFromCache(client);
		}
		return getParameters(client).get(name);
	}
	
	
	public static synchronized void setParameter(String client, String name, String value) {
		Map<String,String> parameters = (Map<String,String>) MemCachePeer.get(MemCacheKeys.getDynamicParameters(client));
		if(parameters == null) {
			parameters = new HashMap<String,String>();
		}
		parameters.put(name, value);
		for(Map.Entry<String, String> p : parameters.entrySet()) 
		{
			logger.info("Updating dynamic parameter for "+client+" key:"+p.getKey()+" value:"+p.getValue());
			getParameters(client).put(p.getKey(), p.getValue());
		}
		MemCachePeer.put(MemCacheKeys.getDynamicParameters(client), parameters);
	}
	
	//GET FROM CACHE
	public static ListBean getParametersBean(String client) {
		ListBean list = new ListBean();
		Map<String,String> parameters = (Map<String,String>)MemCachePeer.get(MemCacheKeys.getDynamicParameters(client));
		if(parameters!=null) {
			for(Map.Entry<String, String> p  : parameters.entrySet()) {
				list.addBean(new DynamicParameterBean(p.getKey(),p.getValue()));
			}
		}
		return list;
	}
	
	//ADD TO CACHE
	public static void setParametersBean(String client, ListBean list) {
		for(ResourceBean res : list.getList()) {
			DynamicParameterBean par = (DynamicParameterBean) res;
			setParameter(client,par.getName(),par.getValue());
		}
	}
	

	//ADD TO MEMORY
	public static void setParameterBean(String client, DynamicParameterBean bean) {
			setParameter(client,bean.getName(),bean.getValue());
	}
	
	//PARAM INIT PER CLIENT
	public static Map<String, String> getParameters(String client) {
		Map<String, String> res = dynamicParameters.get(client);
		if(res == null) {
			dynamicParameters.put(client, new ConcurrentHashMap<String,String>());
			res = dynamicParameters.get(client);
		}
		return res;
	}
	
	private static synchronized void refreshParametersFromCacheInternal(String client) 
	{
		Map<String,String> parameters = (Map<String,String>)MemCachePeer.get(MemCacheKeys.getDynamicParameters(client));
		if(parameters != null) 
		{
			for(Map.Entry<String, String> p : parameters.entrySet()) 
			{
				logger.info("Updating dynamic parameter for "+client+" key:"+p.getKey()+" value:"+p.getValue());
				getParameters(client).put(p.getKey(), p.getValue());
			}
		}
		lastUpdate.put(client,System.currentTimeMillis());
	}
	//CACHE -> MEMORY
	public static synchronized void refreshParametersFromCache(String client) {
		Long lastUpdateClient = lastUpdate.get(client);
		if(lastUpdateClient == null || (System.currentTimeMillis() - lastUpdateClient)/1000 > REFRESH_TIME) 
		{
			refreshParametersFromCacheInternal(client);
		}
	}

	//AB TESTING PARAM
	public static boolean isABTesting(String client,String recTag) {
		Boolean res = false;
		String value;
		if (StringUtils.isEmpty(recTag))
			value = getParameter(client,Parameter.AB_TESTING.name());
		else
			value = getParameter(client,Parameter.AB_TESTING.name()+":"+recTag);
		if(value != null) { res = Boolean.parseBoolean(value); }
		return res;
	}
	
	
	static Timer reloadTimer;
	
	public static void startReloadTimer()
	{
		reloadTimer = new Timer(true);
		int period = 1000 * RELOAD_TIME;
		int delay = 1000;

		
		reloadTimer.scheduleAtFixedRate(new TimerTask() {
			   public void run()  
			   {
				   try
				   {
					   if (dynamicParameters != null)
					   {
						   for(String client : dynamicParameters.keySet())
						   {
							   logger.info("Refreshing dynamic parameters for client "+client);
							   refreshParametersFromCacheInternal(client);
						   }
					   }
				   }
				   catch (Exception e)
				   {
					   logger.error("Caught exception trying to refresh parameters ",e);
				   }
			   }
		   }, delay, period);
	}
	
	public static void stopReloadTimer()
	{
		if (reloadTimer != null)
			reloadTimer.cancel();
	}
}
