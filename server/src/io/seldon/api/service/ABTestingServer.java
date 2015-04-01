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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import io.seldon.trust.impl.CFAlgorithm;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class ABTestingServer {

	private static Logger logger = Logger.getLogger(ABTestingServer.class.getName());
	
	private static Map<String,ABTest> tests = new ConcurrentHashMap<>();
	//max period for the a/b testing
	private final static int CACHING_TIME = 86400;
	
	private static String getABTestKey(String consumer,String recTag)
	{
		return recTag == null ? consumer : consumer+":"+recTag;
	}

	//GET ALGORITHM FOR A USER
	public static CFAlgorithm getUserTest(String consumer, String recTag,String clientUserId) {
		CFAlgorithm res = null;
		final String abTestKey = getABTestKey(consumer, recTag);
		ABTest abTest = tests.get(abTestKey);
		if (abTest != null)
		{
			String mKey = MemCacheKeys.getABTestingUser(abTestKey, clientUserId, abTest.getAlgorithm().getAbTestingKey());
			Boolean inTest = (Boolean) MemCachePeer.get(mKey);
			if (inTest == null)
			{
				// decide whether this user should be in test and store in memcache
				if (Math.random() <= abTest.getPercentage())
				{
					MemCachePeer.put(mKey, new Boolean(true),CACHING_TIME);
					res = abTest.getAlgorithm();
				}
				else
				{
					//Store the fact this user is not in the test
					MemCachePeer.put(mKey, new Boolean(false),CACHING_TIME);
				}
			}
			else if (inTest)
			{
				res = abTest.getAlgorithm();
			}
				
		}
		return res;
	}
	
	public static void setABTest(String consumer, ABTest abTest)
	{
		if (abTest == null)
		{
			logger.error("Attempt to store null abTest for consumer "+consumer);
		}
		else if (abTest.getAlgorithm() == null)
		{
			logger.error("Attempt to store null algorithm for consumer "+consumer);
		}
		else if (StringUtils.isEmpty(abTest.getAlgorithm().getAbTestingKey()))
		{
			logger.error("Empty AB Test key for consumer "+consumer);
		}
		else
		{
			final String abTestKey = getABTestKey(consumer, abTest.getAlgorithm().getRecTag());
			logger.info("Adding new AB Test for consumer "+abTestKey+" with key "+abTest.getAlgorithm().getAbTestingKey()+" Algoritm:"+abTest.getAlgorithm().toString());
			tests.put(abTestKey, abTest);
		}
	}
	
	public static ABTest getABTest(String consumer,String recTag)
	{
		final String abTestKey = getABTestKey(consumer, recTag);
		return tests.get(abTestKey);
	}
}





	
