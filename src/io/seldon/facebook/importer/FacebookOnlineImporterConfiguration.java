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

package io.seldon.facebook.importer;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;
import org.datanucleus.util.StringUtils;

public class FacebookOnlineImporterConfiguration {
	/**
	 * This class needs Springifying 
	 */
	private static Logger logger = Logger.getLogger( FacebookOnlineImporterConfiguration.class.getName() );
	private static final String PROPS_KEY = "io.seldon.facebook.online.clients";
	static ConcurrentMap<String,Boolean> activeClients = new ConcurrentHashMap<>();
	
	public static void initialise(Properties props)
	{
		String val = props.getProperty(PROPS_KEY);
		if (val != null)
		{
			String[] clients = val.split(",");
			if (clients != null && clients.length>0)
			{
				for (String client : clients)
				{
					if (!StringUtils.isEmpty(client))
					{
						logger.info("Adding online facebook import for "+client);
						activeClients.put(client, true);
					}
				}
			}
		}
	}
	
	public static boolean isOnlineImport(String client)
	{
		Boolean b = activeClients.get(client);
		if (b != null)
			return b;
		else
			return false;
	}
			
}
