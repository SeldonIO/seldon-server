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

package io.seldon.storm;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

public class DRPCSettingsFactory {
	private static Logger logger = Logger.getLogger(DRPCSettingsFactory.class.getName());
	public static final int DEF_DRPC_PORT = 3772;
	public static final int DEF_DRPC_TIMEOUT = 60000; //millisecs
	public static final String DEF_REC_TOPOLOGY = "trust-recommendations";
	public static final String PROP_PREFIX = "io.seldon.storm.drpc.";
	public static final String DRPC_HOST_SUFFIX = ".host";
	public static final String DRPC_PORT_SUFFIX = ".port";
	public static final String DRPC_TIMEOUT_SUFFIX = ".timeout";
	public static final String DRPC_REC_SUFFIX = ".rec.topology";
	
	private static ConcurrentHashMap<String,DRPCSettings> peers = new ConcurrentHashMap<>();
	
	public static void initialise(Properties props)
	{
		String clients = props.getProperty(PROP_PREFIX+"clients");
		if (clients != null && clients.length() > 0)
		{
			String[] parts = clients.split(",");
			for(int i=0;i<parts.length;i++)
			{
				String client = parts[i];
				
				// DRPC host
				String host = null;
				String val = props.getProperty(PROP_PREFIX + client + DRPC_HOST_SUFFIX);
				if (val != null)
					host = val;
				else
				{
					logger.error("Failed to find host property for client "+client+" will ignore these settings");
					continue;
				}
				
				// port
				int port = DEF_DRPC_PORT;
				val = props.getProperty(PROP_PREFIX + client + DRPC_PORT_SUFFIX);
				if (val != null)
					port = Integer.parseInt(val);
				
				// port
				int timeout = DEF_DRPC_TIMEOUT;
				val = props.getProperty(PROP_PREFIX + client + DRPC_TIMEOUT_SUFFIX);
				if (val != null)
					timeout = Integer.parseInt(val);
				
				// recommendations topology
				String recToplology = DEF_REC_TOPOLOGY;
				val = props.getProperty(PROP_PREFIX + client + DRPC_REC_SUFFIX);
				if (val != null)
					recToplology = val;
				
				logger.info("Creating DRPCSettings for client "+client+" with host: "+host+" port: "+port+" timeout: "+timeout+" recTopology name: "+recToplology);
				
				peers.put(client, new DRPCSettings(host, port, timeout,recToplology));
			}
		}
	}
	
	public static DRPCSettings get(String client)
	{
		if (peers.containsKey(client))
			return peers.get(client);
		else
		{
			logger.warn("No DRPC settings for client "+client);
			return null;
		}
	}
	
}
