/*
 * Seldon -- open source prediction engine
 * =======================================
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 **********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at       
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ********************************************************************************************** 
*/
package io.seldon.recommendation.baseline;

import io.seldon.recommendation.model.ModelManager;
import io.seldon.resources.external.ExternalResourceStreamer;
import io.seldon.resources.external.NewResourceNotifier;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class MostPopularInSessionFeaturesManager extends ModelManager<MostPopularInSessionFeaturesManager.DimPopularityStore> {

	private static Logger logger = Logger.getLogger(MostPopularInSessionFeaturesManager.class.getName());
	private final ExternalResourceStreamer featuresFileHandler;
	public static final String LOC_PATTERN = "mostpopulardim";

	@Autowired
	public MostPopularInSessionFeaturesManager(ExternalResourceStreamer featuresFileHandler,
								NewResourceNotifier notifier){
		super(notifier, Collections.singleton(LOC_PATTERN));
		this.featuresFileHandler = featuresFileHandler;
	}

	
	
	public static class ItemCount {
		long item;
		float count;
		public ItemCount(long item, float count) {
			super();
			this.item = item;
			this.count = count;
		}
		
	}
	
	
	public static class DimPopularityStore
	{
		Map<Integer,List<ItemCount>> dimToPopularItems;

		public DimPopularityStore(
				Map<Integer, List<ItemCount>> dimToPopularItems) {
			super();
			this.dimToPopularItems = dimToPopularItems;
		}
		
		List<ItemCount> getTopItemsForDimension(int dim)
		{
			return dimToPopularItems.get(dim);
		}
		
	}
	
	protected DimPopularityStore createStore(BufferedReader reader,String client) throws JsonParseException, JsonMappingException, IOException
	{
		ObjectMapper mapper = new ObjectMapper();
		String line;
		Map<Integer,List<ItemCount>> map = new HashMap<>();
		while((line = reader.readLine()) !=null){
			DimItemCount v = mapper.readValue(line.getBytes(), DimItemCount.class);
			List<ItemCount> l = null;
			l = map.get(v.dim);
			if (l == null)
				l = new ArrayList<ItemCount>();
			l.add(new ItemCount(v.item,v.count));
			map.put(v.dim, l);
		}


		logger.info("finished load of most popular items by dimension for client "+client);
		return new DimPopularityStore(map);
	}

	@Override
	protected DimPopularityStore loadModel(String location, String client) {
		logger.info("Reloading most popular items by dimension for client: "+ client);
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					featuresFileHandler.getResourceStream(location + "/items.json")
			));

			DimPopularityStore store = createStore(reader, client);
			
			reader.close();
			
			return store;
		} catch (FileNotFoundException e) {
			logger.error("Couldn't reloadFeatures for client "+ client, e);
		} catch (IOException e) {
			logger.error("Couldn't reloadFeatures for client "+ client, e);
		}
		return null;
	}
}
