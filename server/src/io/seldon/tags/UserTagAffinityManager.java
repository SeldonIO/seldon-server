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
package io.seldon.tags;

import io.seldon.mf.PerClientExternalLocationListener;
import io.seldon.resources.external.ExternalResourceStreamer;
import io.seldon.resources.external.NewResourceNotifier;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class UserTagAffinityManager implements PerClientExternalLocationListener {

	private static Logger logger = Logger.getLogger(UserTagAffinityManager.class.getName());
	private final ConcurrentMap<String,UserTagStore> clientStores = new ConcurrentHashMap<>();
	private Set<NewResourceNotifier> notifiers = new HashSet<>();
	private final ExternalResourceStreamer featuresFileHandler;
	public static final String TAG_NEW_LOC_PATTERN = "tags";

	private final Executor executor = Executors.newFixedThreadPool(5);

	@Autowired
	public UserTagAffinityManager(ExternalResourceStreamer featuresFileHandler,NewResourceNotifier notifier){
		this.featuresFileHandler = featuresFileHandler;
		notifiers.add(notifier);
		notifier.addListener(TAG_NEW_LOC_PATTERN, this);
	}

	public void reloadFeatures(final String location, final String client){
        executor.execute(new Runnable() {
            @Override
            public void run() {
                logger.info("Reloading user tag affinities for client: "+ client);

                try {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(
                            featuresFileHandler.getResourceStream(location + "/part-00000")
                    ));

                    UserTagStore userTags = loadTagAffinities(client, reader);
                    clientStores.put(client, userTags);
                    reader.close();
                    
                    logger.info("finished load of user tag affinities for client "+client);
                } catch (FileNotFoundException e) {
                    logger.error("Couldn't reloadFeatures for client "+ client, e);
                } catch (IOException e) {
                    logger.error("Couldn't reloadFeatures for client "+ client, e);
                }
            }
        });

    }

 protected UserTagStore loadTagAffinities(String client,BufferedReader reader) throws IOException
 {
	 String line;
	 ObjectMapper mapper = new ObjectMapper();
	 int numTags = 0;
	 Map<Long,Map<String,Float>> userTagAffinities = new ConcurrentHashMap<Long,Map<String,Float>>();
	 while((line = reader.readLine()) !=null)
	 {
		 UserTagAffinity data = mapper.readValue(line.getBytes(), UserTagAffinity.class);
		 numTags++;
		 Map<String,Float> tagMap = userTagAffinities.get(data.user);
		 if (tagMap == null)
		 {
			 tagMap = new ConcurrentHashMap<String,Float>();
			 userTagAffinities.put(data.user, tagMap);
		 }
		 tagMap.put(data.tag, data.weight);
	 }
	 logger.info("Loaded "+userTagAffinities.keySet().size()+" users with "+numTags+" tags for "+client);
	 return new UserTagStore(userTagAffinities);
 }
 
 
 	public UserTagStore getStore(String client)
 	{
 		return clientStores.get(client);
 	}
	 
	@Override
	public void newClientLocation(String client, String location,
			String nodePattern) {
		reloadFeatures(location, client);
	}

	@Override
	public void clientLocationDeleted(String client, String nodePattern) {
		clientStores.remove(client);
	}
	
	
	
	public static class UserTagStore {
		
		public final Map<Long,Map<String,Float>> userTagAffinities;

		public UserTagStore(Map<Long, Map<String, Float>> userTagAffinities) {
			super();
			this.userTagAffinities = userTagAffinities;
		}
		
		
	}

	
}
