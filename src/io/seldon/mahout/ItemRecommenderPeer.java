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

package io.seldon.mahout;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.mahout.cf.taste.common.TasteException;

import io.seldon.db.jdo.JDOFactory;

public class ItemRecommenderPeer {

	static ConcurrentHashMap<String,ItemRecommender> store = new ConcurrentHashMap<String,ItemRecommender>();
	private static final String MAHOUT_JNDI_PREFIX = "java:comp/env/";
	public static ItemRecommender get(String client)
	{
		ItemRecommender r =  store.get(client);
		if (r== null)
		{
    		if (ItemRecommenderPeer.addRecommender(client))
    			return get(client);
    		else
    			return null;
		}
		else
			return r;
	}
	

	public static boolean addRecommender(String client)
	{
		try {
			String jndi = JDOFactory.getJNDIForClient(client);
			if (jndi != null)
			{
				if (jndi.startsWith(MAHOUT_JNDI_PREFIX))
				{
					jndi = jndi.substring(MAHOUT_JNDI_PREFIX.length());
					store.putIfAbsent(client, new ItemRecommender(jndi));
					return true;
				}
				else
				{
				//FIXME add error
				}
			}
			else
			{
				
			}
			
			return false;
		} catch (TasteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
}
