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

package io.seldon.mf;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import io.seldon.resources.external.ExternalResourceStreamer;
import io.seldon.resources.external.NewResourceNotifier;
import org.apache.commons.math.linear.Array2DRowRealMatrix;
import org.apache.commons.math.linear.InvalidMatrixException;
import org.apache.commons.math.linear.LUDecompositionImpl;
import org.apache.commons.math.linear.RealMatrix;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 *
 * Manages matrix factorization models for recommendations. It loads new
 * features files when sent notifications.
 *
 * @author firemanphil
 *         Date: 29/09/2014
 *         Time: 15:35
 */
@Component
public class MfFeaturesManager implements PerClientExternalLocationListener {


    private static Logger logger = Logger.getLogger(MfFeaturesManager.class.getName());
    private final ConcurrentMap<String, ClientMfFeaturesStore> clientStores
            = new ConcurrentHashMap<>();
    private NewResourceNotifier notifier;
    private final ExternalResourceStreamer featuresFileHandler;
    private static final String MF_NEW_LOC_PATTERN = "mf";

    private final Executor executor = Executors.newFixedThreadPool(5);

    @Autowired
    public MfFeaturesManager(ExternalResourceStreamer featuresFileHandler,
                             NewResourceNotifier notifier){
        this.featuresFileHandler = featuresFileHandler;
        this.notifier = notifier;
        notifier.addListener(MF_NEW_LOC_PATTERN, this);
    }

    @PostConstruct
    public void init(){
    }

    public void reloadFeatures(final String location, final String client){
        executor.execute(new Runnable() {
            @Override
            public void run() {
                logger.info("Reloading matrix factorization features for client: "+ client);

                try {
                    BufferedReader userFeaturesReader = new BufferedReader(new InputStreamReader(
                            featuresFileHandler.getResourceStream(location + "/userFeatures.txt.gz")
                    ));
                    Map<Long, float[]> userFeatures = readFeatures(userFeaturesReader);
                    int rank= 0;
                    if(!userFeatures.isEmpty()){
                        Long firstUser = userFeatures.keySet().iterator().next();
                        rank = userFeatures.get(firstUser).length;
                    }
                    BufferedReader productFeaturesReader = new BufferedReader(new InputStreamReader(
                            featuresFileHandler.getResourceStream(location + "/productFeatures.txt.gz")
                    ));
                    Map<Long, float[]> productFeatures = readFeatures(productFeaturesReader);
                    clientStores.put(client, new ClientMfFeaturesStore(userFeatures, productFeatures));
                    logger.info("Finished loading MF features ("+userFeatures.size()+" users and "+productFeatures.size() +
                            " products at rank " + rank +") for " + client);
                    userFeaturesReader.close();
                    productFeaturesReader.close();
                } catch (FileNotFoundException e) {
                    logger.error("Couldn't reloadFeatures for client "+ client, e);
                } catch (IOException e) {
                    logger.error("Couldn't reloadFeatures for client "+ client, e);
                }
            }
        });

    }

    public ClientMfFeaturesStore getClientStore(String client){
        return clientStores.get(client);
    }


    private Map<Long,float[]> readFeatures(BufferedReader reader) throws IOException {
        Map<Long, float[]> toReturn = new HashMap<>();
        String line;
        while((line = reader.readLine()) !=null){
            String[] userAndFeatures = line.split("\\|");
            Long item = Long.parseLong(userAndFeatures[0]);
            String[] features = userAndFeatures[1].split(",");

            float[] featuresList = new float[features.length];
            for (int i = 0; i < featuresList.length; i++){
                featuresList[i]= Float.parseFloat(features[i]);
            }
            toReturn.put(item, featuresList);
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

    public static class ClientMfFeaturesStore {

        public final Map<Long, float[]> userFeatures;
        public final Map<Long, float[]> productFeatures;
        public final double[][] productFeaturesInverse;
        public final Map<Long,Integer> idMap;
        
        public ClientMfFeaturesStore(Map<Long, float[]> userFeatures,
                                     Map<Long, float[]> productFeatures){
            this.userFeatures = userFeatures;
            this.productFeatures = productFeatures;

            int numProducts = productFeatures.size();
            int numLatentFactors = productFeatures.values().iterator().next().length;
            idMap = new HashMap<>();
        	double[][] itemFactorsDouble  = new double[numProducts][numLatentFactors];            
        	int i = 0;
        	for(Map.Entry<Long, float[]> e : productFeatures.entrySet())
        	{
        		idMap.put(e.getKey(), i);
        		for(int j=0;j<numLatentFactors;j++)
        			itemFactorsDouble[i][j] = e.getValue()[j];
        		i++;
        	}
        	productFeaturesInverse = computeUserFoldInMatrix(itemFactorsDouble);
        	if (productFeaturesInverse != null)
        		logger.info("Successfully created inverse of product feature matrix for fold in");
        }
        
        /**
         * http://www.slideshare.net/fullscreen/srowen/matrix-factorization/16 
         * @param recentitemInteractions
         * @param productFeaturesInverse
         * @param idMap
         * @return
         */
        private double[][] computeUserFoldInMatrix(double[][] itemFactors) 
        {
        	try
        	{
        		RealMatrix Y = new Array2DRowRealMatrix(itemFactors);
        		RealMatrix YTY = Y.transpose().multiply(Y);
        		RealMatrix YTYInverse = new LUDecompositionImpl(YTY).getSolver().getInverse();

        		return Y.multiply(YTYInverse).getData();
        	}
        	catch (InvalidMatrixException e)
        	{
        		logger.warn("Failed to create inverse of products feature matrix");
        		return null;
        	}
    	  }
    }

}
