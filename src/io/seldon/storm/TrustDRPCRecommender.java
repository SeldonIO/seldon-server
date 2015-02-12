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

import java.util.ArrayList;
import java.util.List;

import io.seldon.util.CollectionTools;
import org.apache.log4j.Logger;
import org.apache.thrift7.TException;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;

public class TrustDRPCRecommender {

	private static Logger logger = Logger.getLogger(TrustDRPCRecommender.class.getName());
	
	DRPCSettings settings;
	DRPCClient client;
	
	
	public TrustDRPCRecommender(DRPCSettings settings,DRPCClient client) {
		super();
		this.client = client;
		this.settings = settings;
	}

	public static String getSortMessage(long userId,int dimension,List<Long> items)
	{
		return new StringBuffer().append(userId).append(",").append(dimension).append(",").append(CollectionTools.join(items, ":")).toString();
	}

	public List<Long> sort(long userId,int dimension,List<Long> items)
	{
		try 
		{
			String result = client.execute(settings.getRecommendationTopologyName(), getSortMessage(userId,dimension,items));
			List<Long> sorted = new ArrayList<Long>();
			if (result != null && !result.equals(""))
			{
				String[] itemIds = result.split(",");
				for(int i=0;i < itemIds.length;i++)
				{
					Long itemId = Long.parseLong(itemIds[i]);
					sorted.add(itemId);
				}
			}
			return sorted;
		} catch (TException e) 
		{
			logger.error("Failed to sort ",e);
			return null;
		} catch (DRPCExecutionException e) 
		{
			logger.error("Failed to sort ",e);
			return null;
		}
		
	}
	
}
