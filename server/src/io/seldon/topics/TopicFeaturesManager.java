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

package io.seldon.topics;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import io.seldon.mf.PerClientExternalLocationListener;
import io.seldon.resources.external.ExternalResourceStreamer;
import io.seldon.resources.external.NewResourceNotifier;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TopicFeaturesManager implements PerClientExternalLocationListener {
	 private static Logger logger = Logger.getLogger(TopicFeaturesManager.class.getName());
	 private final ConcurrentMap<String,TopicFeaturesStore> clientStores = new ConcurrentHashMap<>();
	 private Set<NewResourceNotifier> notifiers = new HashSet<>();
	 private final ExternalResourceStreamer featuresFileHandler;
	 public static final String TOPIC_NEW_LOC_PATTERN = "topics";

	 private final Executor executor = Executors.newFixedThreadPool(5);

	 @Autowired
	 public TopicFeaturesManager(ExternalResourceStreamer featuresFileHandler,
	                             NewResourceNotifier notifier){
	        this.featuresFileHandler = featuresFileHandler;
	        notifiers.add(notifier);
	        notifier.addListener(TOPIC_NEW_LOC_PATTERN, this);
	 }
	 

	  public void reloadFeatures(final String location, final String client){
	        executor.execute(new Runnable() {
	            @Override
	            public void run() {
	                logger.info("Reloading topic features for client: "+ client);

	                try {
	                    BufferedReader userFeaturesReader = new BufferedReader(new InputStreamReader(
	                            featuresFileHandler.getResourceStream(location + "/users.csv")
	                    ));
	                    Map<Long,Map<Integer,Float>> userFeatures = readUserFeatures(userFeaturesReader);
	                    logger.info("Loaded user features for client "+client+" with map of size "+userFeatures.size());
	                    BufferedReader topicFeaturesReader = new BufferedReader(new InputStreamReader(
	                            featuresFileHandler.getResourceStream(location + "/topics.csv")
	                    ));
	                    Map<String,Map<Integer,Float>> topicFeatures = readTopicFeatures(topicFeaturesReader);
	                    clientStores.put(client, new TopicFeaturesStore(userFeatures, topicFeatures));
	                    logger.info("Loaded topic features for client "+client+" with map of size "+topicFeatures.size());
	                    
	                    userFeaturesReader.close();
	                    topicFeaturesReader.close();
	                    logger.info("finished load of topic features for client "+client);
	                } catch (FileNotFoundException e) {
	                    logger.error("Couldn't reloadFeatures for client "+ client, e);
	                } catch (IOException e) {
	                    logger.error("Couldn't reloadFeatures for client "+ client, e);
	                }
	            }
	        });

	    }

	    public TopicFeaturesStore getClientStore(String client){
	        return clientStores.get(client);
	    }

	    private Map<String,Map<Integer,Float>> readTopicFeatures(BufferedReader reader) throws IOException {
	    	Map<String,Map<Integer,Float>> toReturn = new HashMap<>();
	        String line;
	        while((line = reader.readLine()) !=null){
	        	String[] parts = line.split(",");
	            Integer topic = Integer.parseInt(parts[0]);
	        	String keyword = parts[1];
	        	Float weight = Float.parseFloat(parts[2]);
	        	
	        	Map<Integer,Float> topicToWeight = toReturn.get(keyword);
	        	if (topicToWeight == null)
	        		topicToWeight = new HashMap<>();
	        	topicToWeight.put(topic, weight);
	            toReturn.put(keyword, topicToWeight);
	        }
	        return toReturn;
	    }

	    private Map<Long,Map<Integer,Float>> readUserFeatures(BufferedReader reader) throws IOException {
	    	Map<Long,Map<Integer,Float>> toReturn = new HashMap<>();
	        String line;
	        while((line = reader.readLine()) !=null){
		        Map<Integer, Float> topicMap = new HashMap<>();
	        	String[] userAndTopics = line.split(",");
	            Long user = Long.parseLong(userAndTopics[0]);

	            for (int i = 1; i < userAndTopics.length; i++){
	                String[] topicAndWeight = userAndTopics[i].split(":");
	                topicMap.put(Integer.parseInt(topicAndWeight[0]), Float.parseFloat(topicAndWeight[1]));
	            }
	            toReturn.put(user, topicMap);
	        }
	        return toReturn;
	    }

	    @Override
	    public void newClientLocation(String client, String location,String nodePattern) {
	        reloadFeatures(location,client);
	    }

	    @Override
	    public void clientLocationDeleted(String client,String nodePattern) {
	        clientStores.remove(client);
	    }

	    public static class TopicWeights
	    {
	    	float[] weights;
	    	long created;
			public TopicWeights(float[] weights, long created) {
				super();
				this.weights = weights;
				this.created = created;
			}
	    	
	    	
	    }

	    
	    public static class TopicFeaturesStore
		{
	    	int numTopics;
			Map<Long,Map<Integer,Float>> userTopicWeights;
			Map<String,Map<Integer,Float>> tagTopicWeights;
			Map<Long,TopicWeights> itemTopicWeights = new ConcurrentHashMap<>();

			public TopicFeaturesStore(
					Map<Long, Map<Integer, Float>> userTopicWeights,
					Map<String, Map<Integer, Float>> tagTopicWeights) 
			{
				super();
				this.userTopicWeights = userTopicWeights;
				this.tagTopicWeights = tagTopicWeights;
				int maxTopic = 0;
				for(Map<Integer,Float> v : tagTopicWeights.values())
					for (Integer topic : v.keySet())
						if (topic > maxTopic)
							maxTopic = topic;
				for(Map<Integer,Float> v : userTopicWeights.values())
					for (Integer topic : v.keySet())
						if (topic > maxTopic)
							maxTopic = topic;
				this.numTopics = maxTopic + 1;
			}	
			
			public float[] getUserWeightVector(Long userId)
			{
				Map<Integer,Float> topicWeights = userTopicWeights.get(userId);
				if (topicWeights != null)
				{
					float[] v = new float[numTopics];
					for(Map.Entry<Integer, Float> e : topicWeights.entrySet())
						v[e.getKey()] = e.getValue();
					return v;
				}
				else 
					return null;
			}
			
			
			public int getNumTopics() {
				return numTopics;
			}



			public float[] getTopicWeights(Long key,List<String> tags)
			{
				TopicWeights tw = itemTopicWeights.get(key);
				if (tw != null)
					return tw.weights;
				else
				{
					float[] weights = predictTopicWeights(tags);
					tw = new TopicWeights(weights, System.currentTimeMillis());
					itemTopicWeights.put(key, tw);
					return weights;
				}
			}
			
			//Simplistic average of topic weights in tags
			// Need to change to proper online LDA approx
			private float[] predictTopicWeights(List<String> tags)
			{
				float[] score = new float[numTopics];
				float sum = 0.0f;
				for(String tag : tags)
				{
					Map<Integer,Float> topicWeightMap = tagTopicWeights.get(tag);
					if (topicWeightMap != null) // if we have a topic weight for this tag
					{
						for(Map.Entry<Integer, Float> topicWeight : topicWeightMap.entrySet())
						{
							score[topicWeight.getKey()] += topicWeight.getValue();
							sum = sum + topicWeight.getValue();
						}
					}
				}
				if (sum > 0)
					for(int i=0;i<score.length;i++)
						score[i] = score[i]/sum;
				return score;
			}
		}
	    

}
