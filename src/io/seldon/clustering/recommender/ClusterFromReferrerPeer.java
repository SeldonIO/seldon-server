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

package io.seldon.clustering.recommender;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;

import io.seldon.clustering.recommender.jdo.JdoClusterFromReferrer;

public class ClusterFromReferrerPeer {

	public static final String PROP = "io.seldon.clusters.referrer.clients";
	
	private static ClusterFromReferrerPeer peer;
	
	public static void initialise(Properties props)
	{
		String clientsProp = props.getProperty(PROP);
		if (StringUtils.isNotBlank(clientsProp))
		{
			String[] clients = clientsProp.split(",");
			peer = new ClusterFromReferrerPeer(clients);
		}
	}

	public static void shutdown()
	{
		if (peer != null)
		{
			peer.shutdownReferrers();
		}
	}
	
	public static ClusterFromReferrerPeer get()
	{
		return peer;
	}
	
	Map<String,IClusterFromReferrer> referrerHandlerMap;
	

	private ClusterFromReferrerPeer (String[] clients)
	{
		referrerHandlerMap = new ConcurrentHashMap<String,IClusterFromReferrer>();
		for(int i=0;i<clients.length;i++)
			referrerHandlerMap.put(clients[i], new JdoClusterFromReferrer(clients[i]));
	}

	public void shutdownReferrers()
	{
		for(Map.Entry<String, IClusterFromReferrer> e : referrerHandlerMap.entrySet())
			e.getValue().shutdown();
	}
	
	public Set<Integer> getClustersFromReferrer(String client,String referrer)
	{
		IClusterFromReferrer c = referrerHandlerMap.get(client);
		if (c == null)
			return null;
		else
			return c.getClusters(referrer);
	}

}
