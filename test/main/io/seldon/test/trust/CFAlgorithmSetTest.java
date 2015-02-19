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

package io.seldon.test.trust;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import io.seldon.api.Util;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.test.BaseTest;
import io.seldon.test.GenericPropertyHolder;
import io.seldon.trust.impl.CFAlgorithm;
import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.seldon.api.AlgorithmServiceImpl;

public class CFAlgorithmSetTest extends BaseTest {

	@Autowired
	GenericPropertyHolder props;
	
	@Before
	public void setup()
	{
		AlgorithmServiceImpl aService = new AlgorithmServiceImpl();
        ConcurrentHashMap<String, CFAlgorithm> map = new ConcurrentHashMap<>();
        map.put(props.getClient(), new CFAlgorithm());
        aService.setAlgorithmMap(map);
        Util.setAlgorithmService(aService);
	}
	
	@Test
	public void testSetRecommender() throws CloneNotSupportedException
	{
		String alg = "recommenders:CLUSTER_COUNTS|SEMANTIC_VECTORS_SORT";
		List<String> settings = new ArrayList<>();
		settings.add(alg);
		CFAlgorithm c = Util.getAlgorithmOptions(new ConsumerBean(props.getClient()),settings,null);
		Assert.assertEquals(2, c.getRecommenders().size());
		Assert.assertEquals("CLUSTER_COUNTS", c.getRecommenders().get(0).name());
		Assert.assertEquals("SEMANTIC_VECTORS_SORT", c.getRecommenders().get(1).name());
	}
	
	
	@Test
	public void testSetRecommenderWithStrategy() throws CloneNotSupportedException
	{
		String alg = "recommenders:CLUSTER_COUNTS_ITEM_CATEGORY|SEMANTIC_VECTORS_SORT|CLUSTER_COUNTS_GLOBAL";
		String strategy = "recommender_strategy:RANK_SUM";
		List<String> settings = new ArrayList<>();
		settings.add(alg);
		settings.add(strategy);
		CFAlgorithm c = Util.getAlgorithmOptions(new ConsumerBean(props.getClient()),settings,null);
		Assert.assertEquals(3, c.getRecommenders().size());
		Assert.assertEquals("CLUSTER_COUNTS_ITEM_CATEGORY", c.getRecommenders().get(0).name());
		Assert.assertEquals("SEMANTIC_VECTORS_SORT", c.getRecommenders().get(1).name());
		Assert.assertEquals("CLUSTER_COUNTS_GLOBAL", c.getRecommenders().get(2).name());
		Assert.assertEquals("RANK_SUM", c.getRecommenderStrategy().name());
	}
	 
	
	@Test
	public void testSetRecommenderClusterItemCategory() throws CloneNotSupportedException
	{
		String alg = "recommenders:CLUSTER_COUNTS_ITEM_CATEGORY";
		List<String> settings = new ArrayList<>();
		settings.add(alg);
		CFAlgorithm c = Util.getAlgorithmOptions(new ConsumerBean(props.getClient()),settings,null);
		Assert.assertEquals(1, c.getRecommenders().size());
		Assert.assertEquals("CLUSTER_COUNTS_ITEM_CATEGORY", c.getRecommenders().get(0).name());
		
	}
	
	@Test
	public void testSetRecommenderAndStrategy() throws CloneNotSupportedException
	{
		String alg = "recommenders:SEMANTIC_VECTORS_SORT|CLUSTER_COUNTS|CLUSTER_COUNTS_FOR_ITEM|CLUSTER_COUNTS_GLOBAL,recommender_strategy:RANK_SUM";
		String[] algs = alg.split(",");
		List<String> settings = new ArrayList<>();
		for(int i=0;i<algs.length;i++) settings.add(algs[i]);
		CFAlgorithm c = Util.getAlgorithmOptions(new ConsumerBean(props.getClient()),settings,null);
		Assert.assertEquals(4, c.getRecommenders().size());
		Assert.assertEquals("SEMANTIC_VECTORS_SORT", c.getRecommenders().get(0).name());
		Assert.assertEquals("CLUSTER_COUNTS", c.getRecommenders().get(1).name());
		Assert.assertEquals("CLUSTER_COUNTS_FOR_ITEM", c.getRecommenders().get(2).name());
		Assert.assertEquals("CLUSTER_COUNTS_GLOBAL", c.getRecommenders().get(3).name());
		Assert.assertEquals("RANK_SUM", c.getRecommenderStrategy().name());
	}
	
	

}
