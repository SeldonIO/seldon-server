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
package io.seldon.mf;

import io.seldon.recommendation.Recommendation;
import io.seldon.recommendation.model.ModelManager;
import io.seldon.resources.external.ExternalResourceStreamer;
import io.seldon.resources.external.NewResourceNotifier;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MfUserClustersModelManager  extends ModelManager<MfUserClustersModelManager.MfUserModel> {

	private static Logger logger = Logger.getLogger(MfUserClustersModelManager.class.getName());
	private final ExternalResourceStreamer featuresFileHandler;
	public static final String LOC_PATTERN = "mfcluster";

	@Autowired
	public MfUserClustersModelManager(ExternalResourceStreamer featuresFileHandler,
								NewResourceNotifier notifier){
		super(notifier, Collections.singleton(LOC_PATTERN));
		this.featuresFileHandler = featuresFileHandler;
	}
	
	public static class MfUserModel
	{
		public final Map<Long,Integer> userClusters;
		public final Map<Integer,List<Recommendation>> recommendations;
		
		public MfUserModel(Map<Long, Integer> userClusters,
				Map<Integer, List<Recommendation>> recommendations) {
			super();
			this.userClusters = userClusters;
			this.recommendations = recommendations;
		}
		
		
	}
	
	protected Map<Long,Integer> createUserClusters(BufferedReader reader) throws IOException
	{
		String line;
		Map<Long,Integer> map = new ConcurrentHashMap<>();
		while((line = reader.readLine()) !=null){
			String[] parts = line.split(",");
			long userId = Long.parseLong(parts[0]);
			int clusterIdx = Integer.parseInt(parts[1]); 
			map.put(userId,clusterIdx); 
		}
		return map;
	}
	
	protected Map<Integer,List<Recommendation>> createRecommendations(BufferedReader reader) throws NumberFormatException, IOException
	{
		String line;
		Map<Integer,List<Recommendation>> map = new ConcurrentHashMap<>();
		while((line = reader.readLine()) !=null){
			String[] parts = line.split(",");
			int clusterId = Integer.parseInt(parts[0]);
			long itemId = Long.parseLong(parts[1]);
			double score = Double.parseDouble(parts[2]);
			Recommendation r = new Recommendation(itemId, 1, score);
			List<Recommendation> recs = map.get(clusterId);
			if (recs == null)
				recs = new ArrayList<Recommendation>();
			recs.add(r);
			map.put(clusterId, recs);
		}
		for(List<Recommendation> recs : map.values())
			Collections.sort(recs);
		
		return map;
	}

	@Override
	protected MfUserModel loadModel(String location, String client) {
		logger.info("Reloading most popular items by dimension for client: "+ client);
		try {
			BufferedReader readerClusters = new BufferedReader(new InputStreamReader(
					featuresFileHandler.getResourceStream(location + "/userclusters.csv")
			));
			BufferedReader readerRecommendations = new BufferedReader(new InputStreamReader(
					featuresFileHandler.getResourceStream(location + "/userclusters.csv")
			));

			MfUserModel m = new MfUserModel(createUserClusters(readerClusters), createRecommendations(readerRecommendations));
			
			readerClusters.close();
			readerRecommendations.close();
			
			return m;
		} catch (FileNotFoundException e) {
			logger.error("Couldn't reloadFeatures for client "+ client, e);
		} catch (IOException e) {
			logger.error("Couldn't reloadFeatures for client "+ client, e);
		}
		return null;
	}
}
