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

import java.util.HashMap;
import java.util.Map;

public class ALSRecommenderStore {

	static Map<String,ALSRecommender> recommenders = new HashMap<String,ALSRecommender>();
	
	public static void load(String clientStr,String baseDir)
	{
		String[] clients = clientStr.split(",");
		for(int i=0;i<clients.length;i++)
		{
			String dir = baseDir + "/" + clients[i];
			recommenders.put(clients[i], new ALSRecommender(dir+"/U",dir+"/M"));
		}
	}
	
	public static ALSRecommender get(String client)
	{
		return recommenders.get(client);
	}
}
