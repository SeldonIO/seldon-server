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

import io.seldon.api.state.PredictionAlgorithmStore.Algorithm;
import io.seldon.api.state.PredictionAlgorithmStore.AlgorithmConfig;
import io.seldon.api.state.PredictionAlgorithmStore.Strategy;
import io.seldon.api.state.PredictionAlgorithmStore.TestConfig;
import io.seldon.api.state.PredictionAlgorithmStore.TestVariation;
import io.seldon.api.state.PredictionStrategyDeserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class VariationPredictionStrategyTest {

	@Test
	public void testTestConfig() throws IOException
	{
		Algorithm alg = new Algorithm();
		alg.name = "myalg";
		List<Algorithm> algs = new ArrayList<>();
		algs.add(alg);
		AlgorithmConfig ac = new AlgorithmConfig();
		ac.algorithms  = algs;
		TestVariation tv = new TestVariation();
		tv.ratio = "100";
		tv.label = "v1";
		tv.config = ac;
		List<TestVariation> variations = new ArrayList<>();
		variations.add(tv);
		TestConfig tc = new TestConfig();
		tc.variations = variations;
		
		ObjectMapper omapper = new ObjectMapper();
		String configValue = omapper.writeValueAsString(tc);
		
		SimpleModule module = new SimpleModule("PredictionStrategyDeserializerModule");
        module.addDeserializer(Strategy.class, new PredictionStrategyDeserializer());
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(module);
        
        Strategy configStrategy = mapper.readValue(configValue, Strategy.class);
		TestConfig config = (TestConfig) configStrategy;
		
		System.out.println(config.variations.iterator().next().label);
		
	}
	
}
