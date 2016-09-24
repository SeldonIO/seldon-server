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
package io.seldon.prediction;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import io.seldon.api.state.PredictionAlgorithmStore;
import io.seldon.api.state.PredictionAlgorithmStore.Algorithm;
import io.seldon.api.state.PredictionAlgorithmStore.AlgorithmConfig;
import io.seldon.api.state.PredictionAlgorithmStore.Strategy;
import io.seldon.api.state.PredictionAlgorithmStore.TestConfig;
import io.seldon.api.state.PredictionAlgorithmStore.TestVariation;
import io.seldon.api.state.PredictionStrategyDeserializer;
import io.seldon.clustering.recommender.RecommendationContext.OptionsHolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.codehaus.jackson.JsonNode;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class VariationPredictionStrategyTest {

	private ApplicationContext mockApplicationContext;
	
	@Before
	public void createMocks()
	{
		mockApplicationContext = createMock(ApplicationContext.class);
	}
	
	private TestConfig createVariation()
	{
		Algorithm alg = new Algorithm();
		alg.name = "myalg";
		List<Algorithm> algs = new ArrayList<>();
		algs.add(alg);
		AlgorithmConfig ac = new AlgorithmConfig();
		ac.algorithms  = algs;
		TestVariation tv1 = new TestVariation();
		tv1.ratio = "0.5";
		tv1.label = "v1";
		tv1.config = ac;
		TestVariation tv2 = new TestVariation();
		tv2.ratio = "0.5";
		tv2.label = "v2";
		tv2.config = ac;
		
		List<TestVariation> variations = new ArrayList<>();
		variations.add(tv1);
		variations.add(tv2);
		TestConfig tc = new TestConfig();
		tc.variations = variations;
		
		return tc;
	}
	
	
	private AlgorithmConfig createAlgorithmConfig()
	{
		Algorithm alg = new Algorithm();
		alg.name = "myalg";
		List<Algorithm> algs = new ArrayList<>();
		algs.add(alg);
		AlgorithmConfig ac = new AlgorithmConfig();
		ac.algorithms  = algs;
		return ac;
	}
	
	@Test
	public void testTestConfig() throws IOException
	{
		ObjectMapper omapper = new ObjectMapper();
		String configValue = omapper.writeValueAsString(createVariation());
		
		SimpleModule module = new SimpleModule("PredictionStrategyDeserializerModule");
        module.addDeserializer(Strategy.class, new PredictionStrategyDeserializer());
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(module);
        
        Strategy configStrategy = mapper.readValue(configValue, Strategy.class);
		TestConfig config = (TestConfig) configStrategy;
		
		System.out.println(config.variations.iterator().next().label);
	}
	
	@Test 
	public void testVariationStrategy() throws JsonProcessingException
	{
		TestConfig tc = createVariation();
		PredictionAlgorithmStore pas = new PredictionAlgorithmStore(null,null);
		PredictionAlgorithm palg = new PredictionAlgorithm() {
			
			@Override
			public PredictionsResult predict(String client, JsonNode json,
					OptionsHolder options) {
				// TODO Auto-generated method stub
				return null;
			}
		};
		Map<String,PredictionAlgorithm> pbeans = new HashMap<>();
		Map<String,FeatureTransformer> ptrans = new HashMap<>();
		expect(mockApplicationContext.getBean(tc.variations.iterator().next().config.algorithms.iterator().next().name,PredictionAlgorithm.class)).andReturn(palg).times(2);
		expect(mockApplicationContext.getBeansOfType(PredictionAlgorithm.class)).andReturn(pbeans);
		expect(mockApplicationContext.getBeansOfType(FeatureTransformer.class)).andReturn(ptrans);
		replay(mockApplicationContext);
		
		pas.setApplicationContext(mockApplicationContext);
		
		final String client = "client";
		ObjectMapper omapper = new ObjectMapper();
		String configValue = omapper.writeValueAsString(tc);
		pas.configUpdated(client, PredictionAlgorithmStore.ALG_KEY, configValue);
		PredictionStrategy ps = pas.retrieveStrategy(client);
		
		int v1Count = 0;
		int v2Count = 0;
		for(int i=0;i<1000;i++)
		{
			if (ps.configure().label.equals("v1"))
				v1Count++;
			else
				v2Count++;
		}
		Assert.assertTrue(v1Count > 0);
		Assert.assertTrue(v2Count > 0);
		Assert.assertEquals(v1Count, v2Count,250);
	}
	
	@Test 
	public void testSimpleStrategy() throws JsonProcessingException
	{
		AlgorithmConfig ac = createAlgorithmConfig();
		PredictionAlgorithmStore pas = new PredictionAlgorithmStore(null,null);
		PredictionAlgorithm palg = new PredictionAlgorithm() {
			
			@Override
			public PredictionsResult predict(String client, JsonNode json,
					OptionsHolder options) {
				// TODO Auto-generated method stub
				return null;
			}
		};
		Map<String,PredictionAlgorithm> pbeans = new HashMap<>();
		Map<String,FeatureTransformer> ptrans = new HashMap<>();
		expect(mockApplicationContext.getBean(ac.algorithms.iterator().next().name,PredictionAlgorithm.class)).andReturn(palg).times(1);
		expect(mockApplicationContext.getBeansOfType(PredictionAlgorithm.class)).andReturn(pbeans);
		expect(mockApplicationContext.getBeansOfType(FeatureTransformer.class)).andReturn(ptrans);
		replay(mockApplicationContext);
		
		pas.setApplicationContext(mockApplicationContext);
		
		final String client = "client";
		ObjectMapper omapper = new ObjectMapper();
		String configValue = omapper.writeValueAsString(ac);
		pas.configUpdated(client, PredictionAlgorithmStore.ALG_KEY, configValue);
		PredictionStrategy ps = pas.retrieveStrategy(client);
		Assert.assertEquals(PredictionStrategy.DEFAULT_NAME, ps.configure().label);
	}
	
}
