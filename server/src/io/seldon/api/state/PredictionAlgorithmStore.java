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
package io.seldon.api.state;

import io.seldon.prediction.FeatureTransformer;
import io.seldon.prediction.FeatureTransformerStrategy;
import io.seldon.prediction.PredictionAlgorithm;
import io.seldon.prediction.PredictionAlgorithmStrategy;
import io.seldon.prediction.PredictionStrategy;
import io.seldon.prediction.SimplePredictionStrategy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class PredictionAlgorithmStore implements ApplicationContextAware,ClientConfigUpdateListener,GlobalConfigUpdateListener {
	protected static Logger logger = Logger.getLogger(PredictionAlgorithmStore.class.getName());
	private static final String ALG_KEY = "predict_algs";
	
	private ConcurrentMap<String, PredictionStrategy> predictionStore = new ConcurrentHashMap<>();
	private ConcurrentMap<String, FeatureTransformerStrategy> transformerStore = new ConcurrentHashMap<>();
	 private PredictionStrategy defaultStrategy;
	 
	 private final ClientConfigHandler configHandler;
	 private final GlobalConfigHandler globalConfigHandler;
	 private ApplicationContext applicationContext;
	 
	 @Autowired
	 public PredictionAlgorithmStore(ClientConfigHandler configHandler,
			 GlobalConfigHandler globalConfigHandler)
	 {	
		 this.configHandler = configHandler;
		 this.globalConfigHandler = globalConfigHandler;
	 }
	 
	 @PostConstruct
	 private void init(){
		 logger.info("Initializing...");
		 configHandler.addListener(this);
	     globalConfigHandler.addSubscriber("default_prediction_strategy", this);
	    }

	 public PredictionStrategy retrieveStrategy(String client)
	 {
		 PredictionStrategy strategy = predictionStore.get(client);
		 if(strategy!=null){
			 return strategy;
		 } else {
			 return defaultStrategy;
		 }
	 }
	 
	 @Override
	 public void configUpdated(String configKey, String configValue) {
		 configValue = StringUtils.strip(configValue);
		 logger.info("KEY WAS " + configKey);
		 logger.info("Received new default strategy: " + configValue);
	        
		 if (StringUtils.length(configValue) == 0) {
			 logger.warn("*WARNING* no default strategy is set!");
		 } else {
			 try {
				 ObjectMapper mapper = new ObjectMapper();
				 List<PredictionAlgorithmStrategy> strategies = new ArrayList<>();
				 AlgorithmConfig config = mapper.readValue(configValue, AlgorithmConfig.class);
				 for (Algorithm algorithm : config.algorithms) {
					 PredictionAlgorithmStrategy strategy = toAlgorithmStrategy(algorithm);
					 strategies.add(strategy);
				 }
				 List<FeatureTransformerStrategy> featureTransformerStrategies = new ArrayList<>();
				 for (Transformer transformer : config.transformers)
				 {
					 FeatureTransformerStrategy strategy = toFeatureTransformerStrategy(transformer);
					 featureTransformerStrategies.add(strategy);
				 }
				 defaultStrategy =  new SimplePredictionStrategy(Collections.unmodifiableList(featureTransformerStrategies),Collections.unmodifiableList(strategies));
				 logger.info("Successfully added new default prediction strategy");
			 } catch (IOException | BeansException e) {
				 logger.error("Couldn't update default prediction strategy", e);
			 }
		 }
	 }
	 
	@Override
	public void configUpdated(String client, String configKey,String configValue) {
		if (configKey.equals(ALG_KEY)){
			logger.info("Received new algorithm config for "+ client+": "+ configValue);
			try {
				ObjectMapper mapper = new ObjectMapper();
				List<PredictionAlgorithmStrategy> strategies = new ArrayList<>();
				AlgorithmConfig config = mapper.readValue(configValue, AlgorithmConfig.class);
				for (Algorithm algorithm : config.algorithms) {
	                    PredictionAlgorithmStrategy strategy = toAlgorithmStrategy(algorithm);
	                    strategies.add(strategy);
	                }
				List<FeatureTransformerStrategy> featureTransformerStrategies = new ArrayList<>();
				if (config.transformers != null)
					for (Transformer transformer : config.transformers)
					{
						FeatureTransformerStrategy strategy = toFeatureTransformerStrategy(transformer);
						featureTransformerStrategies.add(strategy);
					}
				predictionStore.put(client, new SimplePredictionStrategy(Collections.unmodifiableList(featureTransformerStrategies),Collections.unmodifiableList(strategies)));
				logger.info("Successfully added new algorithm config for "+client);
	            } catch (IOException | BeansException e) {
	                logger.error("Couldn't update algorithms for client " +client, e);
	            }
		}
	}
	
	@Override
	public void configRemoved(String client, String configKey) {
		if (configKey.equals(ALG_KEY)){
			predictionStore.remove(client);
			logger.info("Removed client "+client);
		}
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		 this.applicationContext = applicationContext;
		 StringBuilder builder= new StringBuilder("Available algorithms: \n");
		 for (PredictionAlgorithm inc : applicationContext.getBeansOfType(PredictionAlgorithm.class).values()){
			 builder.append('\t');
			 builder.append(inc.getClass());
			 builder.append('\n');
		 }
		 builder.append("Available feature transformers: \n");
		 for(FeatureTransformer f : applicationContext.getBeansOfType(FeatureTransformer.class).values()){
			 builder.append('\t');
			 builder.append(f.getClass());
			 builder.append('\n');			 
		 }
		 logger.info(builder.toString());
	        
	}
	
	
	private PredictionAlgorithmStrategy toAlgorithmStrategy(Algorithm algorithm) {
		PredictionAlgorithm alg = applicationContext.getBean(algorithm.name, PredictionAlgorithm.class);
		Map<String, String> config  = toConfigMap(algorithm.config);
		return new PredictionAlgorithmStrategy(alg,algorithm.config ==null ? new HashMap<String, String>(): config , algorithm.name);
	}
	
	private FeatureTransformerStrategy toFeatureTransformerStrategy(Transformer transformer)
	{
		FeatureTransformer t = applicationContext.getBean(transformer.name,FeatureTransformer.class);
		Map<String, String> config  = toConfigMap(transformer.config);
		return new FeatureTransformerStrategy(t, transformer.inputCols, transformer.outputCols, config);
	}
	   
	private Map<String, String> toConfigMap(List<ConfigItem> config) {
		Map<String, String> configMap = new HashMap<>();
		if (config==null) return configMap;
		for (ConfigItem item : config){
			configMap.put(item.name,item.value);
		}
		return configMap;
	}
	
	 public static class AlgorithmConfig {
	        public List<Algorithm> algorithms;
	        public List<Transformer> transformers;
	        public String combiner;
	    }

	    public static class ConfigItem {
	        public String name;
	        public String value;
	    }

	    public static class Algorithm {
	        public String name;
	        public List<ConfigItem> config;
	    }

	    public static class Transformer {
	    	public String name;
	    	public List<String> inputCols;
	    	public List<String> outputCols;
	    	public List<ConfigItem> config;
	    }

		
}
