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

package io.seldon.sv;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
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

import pitt.search.semanticvectors.FlagConfig;
import pitt.search.semanticvectors.ObjectVector;
import pitt.search.semanticvectors.VectorStoreRAM;

@Component
public class SemanticVectorsManager implements PerClientExternalLocationListener {

	 private static Logger logger = Logger.getLogger(SemanticVectorsManager.class.getName());
	 private final ConcurrentMap<String,SemanticVectorsStore> clientStores = new ConcurrentHashMap<>();
	 private Set<NewResourceNotifier> notifiers = new HashSet<>();
	 private final ExternalResourceStreamer featuresFileHandler;
	 public static final String SV_TEXT_NEW_LOC_PATTERN = "svtext";
	 public static final String SV_CLUSTER_NEW_LOC_PATTERN = "svcluster";
	 public static final String SV_WORD2VEC_NEW_LOC_PATTERN = "word2vec";	 

	 private static SemanticVectorsManager theManager; // hack until rest of code Springified
	 
	 private final Executor executor = Executors.newFixedThreadPool(5);

	 public static SemanticVectorsManager getManager()
	 {
		 return theManager;
	 }
	 
	 @Autowired
	 public SemanticVectorsManager(ExternalResourceStreamer featuresFileHandler,
	                             NewResourceNotifier notifier){
	        this.featuresFileHandler = featuresFileHandler;
	        notifiers.add(notifier);
	        notifier.addListener(SV_TEXT_NEW_LOC_PATTERN, this);
	        notifier.addListener(SV_CLUSTER_NEW_LOC_PATTERN, this);
	        notifier.addListener(SV_WORD2VEC_NEW_LOC_PATTERN, this);
	        theManager = this;
	 }
	 
	 private String getKey(String client,String key)
	 {
		 return client + ":" + key;
	 }
	 
	  public void reloadFeatures(final String location, final String client, final String nodePattern){
	        executor.execute(new Runnable() {
	            @Override
	            public void run() {
	                logger.info("Reloading semantic vector features for client: "+ client +" with pattern "+nodePattern+" from location "+location);

	                try {
	                    BufferedReader termVectorsReader = new BufferedReader(new InputStreamReader(
	                            featuresFileHandler.getResourceStream(location + "/termvectors.txt")
	                    ));
	                    VectorStoreRAM termStore = createSVPeer(termVectorsReader);
	                    termVectorsReader.close();
	                    logger.info("Loaded "+client+"/"+nodePattern+" termstore with "+termStore.getNumVectors()+" vectors");
	                    
	                    BufferedReader docVectorsReader = new BufferedReader(new InputStreamReader(
	                            featuresFileHandler.getResourceStream(location + "/docvectors.txt")
	                    ));
	                    VectorStoreRAM docStore = createSVPeer(docVectorsReader);
	                    docVectorsReader.close();
	                    logger.info("Loaded "+client+"/"+nodePattern+" docstore with "+docStore.getNumVectors()+" vectors");
	                    
	                    SemanticVectorsStore svPeer = new SemanticVectorsStore(termStore, docStore);
	                    clientStores.put(getKey(client,nodePattern), svPeer);
	                    

	                    logger.info("finished load of semantic vector features for client "+client);
	                } catch (FileNotFoundException e) {
	                    logger.error("Couldn't reloadFeatures for client "+ client, e);
	                } catch (IOException e) {
	                    logger.error("Couldn't reloadFeatures for client "+ client, e);
	                }
	            }
	        });

	    }
	  
	  
	  private VectorStoreRAM createSVPeer(BufferedReader reader) throws IOException
	  {
		  FlagConfig flagConfig = FlagConfig.getFlagConfig(null);
		  String firstLine = reader.readLine();
	      FlagConfig.mergeWriteableFlagsFromString(firstLine, flagConfig);
		  VectorStoreRAM svstore = new VectorStoreRAM(flagConfig);
		  VectorEnumerationText vectorEnumeration = new VectorEnumerationText(reader,flagConfig);
		  while (vectorEnumeration.hasMoreElements()) {
		      ObjectVector objectVector = vectorEnumeration.nextElement();
		      svstore.putVector(objectVector.getObject(), objectVector.getVector());
		    }
		  return svstore;
	  }

	@Override
	public void newClientLocation(String client, String location,String nodePattern) {
		logger.info("New location alter for "+client+" at location "+location+" with node pattern "+nodePattern);
		reloadFeatures(location, client, nodePattern);
		
	}

	@Override
	public void clientLocationDeleted(String client,String nodePattern) {
		logger.info("Remove client "+client+" with pattern "+nodePattern);
		clientStores.remove(getKey(client,nodePattern));
	}
	 
	public SemanticVectorsStore getStore(String client,String type)
	{
		return clientStores.get(getKey(client, type));
	}
	
}
