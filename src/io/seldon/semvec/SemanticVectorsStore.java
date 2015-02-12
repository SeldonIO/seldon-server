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

package io.seldon.semvec;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class SemanticVectorsStore {

	private static Logger logger = Logger.getLogger(SemanticVectorsStore.class.getName());
	
	public static final String PREFIX_FIND_SIMILAR = "text";
	public static final String PREFIX_KEYWORD_SEARCH = "text_all";
	public static final String PREFIX_RATINGS = "user_ratings";
	public static final String PREFIX_USER_ITEMS = "user_items";
	public static final String CLUSTER = "cluster";
	
	static ConcurrentHashMap<String,SemVectorsPeer> store = new ConcurrentHashMap<String,SemVectorsPeer>();
	static String baseDirectory = "";
	private static int reloadSec = 60 * 5;
	
	public static void setBaseDirectory(String baseDir)
	{
		SemanticVectorsStore.baseDirectory = baseDir;
	}
	
	public static void initialise(Properties props)
	{
		SemanticVectorsStore.baseDirectory = props.getProperty("io.seldon.labs.semvec.basedir");
		if (SemanticVectorsStore.baseDirectory == null)
		{
			logger.warn("No base directory for semantic vectors specified");
		}
		else
		{
			String clientsToPreload = props.getProperty("io.seldon.labs.semvec.preload");
			if (!StringUtils.isEmpty(clientsToPreload))
			{
				String client[] = clientsToPreload.split(",");
				for(int i=0;i<client.length;i++)
				{
					String prefixStr = props.getProperty("io.seldon.labs.semvec.preload.prefix."+client[i]);
					if (prefixStr == null)
						prefixStr="text";
					if (prefixStr != null)
					{
						String prefix[] = prefixStr.split(",");
						for(int j=0;j<prefix.length;j++)
						{
							logger.info("Preloading semantic vectors store for client "+client[i]+" with prefix "+prefix[j]+"...");
							SemVectorsPeer svPeer = get(client[i],prefix[j]);
							if (svPeer == null) {
//								logger.error("Failed to preload store for client "+client[i]+" with prefix "+prefix[j]);
                                final String message = "Failed to preload store for client " + client[i] + " with prefix " + prefix[j];
                                logger.error(message, new Exception(message));
                            }
							else
								logger.info("Preloaded semantic vectors store for client "+client[i]+" with prefix "+prefix[j]);
						}
					}
				}
			}
		}
	}
	
	
	
	public static String getBaseDirectory() {
		return baseDirectory;
	}



	public static SemVectorsPeer get(String client,String prefix)
	{
		return get(client,prefix,null);
	}
	
	public static SemVectorsPeer get(String client,String prefixType,Integer type,int reloadSecDelay,int reloadSecInterval)
	{
		String key;
		key = client+":"+prefixType;
		if (type != null)
			key = key+":"+type;
		String prefix = prefixType;
		if (type != null)
			prefix = prefix+"_"+type;
		SemVectorsPeer peer = store.get(key);
		if (peer == null)
		{
			String baseDir = baseDirectory+"/" + client;
			peer = new SemVectorsPeer(baseDir,prefix,prefix+"_termvectors.bin",prefix+"_docvectors.bin",true);
			if (store.putIfAbsent(key, peer) == null)
				peer.startReloadTimer(prefix+"_termvectors2.bin", prefix+"_docvectors2.bin", reloadSecDelay, reloadSecInterval);
			return get(client,prefixType,type);
		}
		else
			return peer;
	}

	
	public static SemVectorsPeer get(String client,String prefixType,Integer type)
	{
		return get(client,prefixType,type,reloadSec,reloadSec);
	}
}
