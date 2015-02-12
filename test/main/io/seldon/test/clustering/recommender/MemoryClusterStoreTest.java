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

package io.seldon.test.clustering.recommender;

import java.util.ArrayList;
import java.util.List;

import io.seldon.clustering.recommender.UserCluster;
import junit.framework.Assert;

import org.junit.Test;

import io.seldon.clustering.recommender.ClusterRecommenderException;
import io.seldon.clustering.recommender.MemoryUserClusterStore;

public class MemoryClusterStoreTest {

	@Test
	public void testStoreRetrieveClusters()
	{
		MemoryUserClusterStore m = new MemoryUserClusterStore("",1);
		ArrayList<UserCluster> clusters = new ArrayList<UserCluster>();
		clusters.add(new UserCluster(1L,2,0.75,1,1));
		clusters.add(new UserCluster(1L,14561,0.4,1,1));
		clusters.add(new UserCluster(1L,62421,0,1,1));
		clusters.add(new UserCluster(1L,0,1,1,1));
		System.out.println("Clusters created:");
		for(UserCluster cluster : clusters)
			System.out.println(cluster.toString());
		m.store(1L, clusters);
		List<UserCluster> clustersGot = m.getClusters(1L);
		System.out.println("Clusters returned:");
		for(UserCluster cluster : clustersGot)
			System.out.println(cluster.toString());
		Assert.assertEquals(2, clustersGot.get(0).getCluster());
		Assert.assertEquals(0.75, clustersGot.get(0).getWeight(), 0.01);
		Assert.assertEquals(14561, clustersGot.get(1).getCluster());
		Assert.assertEquals(0.4, clustersGot.get(1).getWeight(), 0.01);
		Assert.assertEquals(62421, clustersGot.get(2).getCluster());
		Assert.assertEquals(0, clustersGot.get(2).getWeight(), 0.01);
		Assert.assertEquals(0, clustersGot.get(3).getCluster());
		Assert.assertEquals(1, clustersGot.get(3).getWeight(), 0.01);
	}
	
	@Test
	public void testBadClusters()
	{
		try
		{
			MemoryUserClusterStore m = new MemoryUserClusterStore("",1);
			ArrayList<UserCluster> clusters = new ArrayList<UserCluster>();
			clusters.add(new UserCluster(1L,90000,0.75,1,1));
			System.out.println("Clusters created:");
			for(UserCluster cluster : clusters)
				System.out.println(cluster.toString());
			m.store(1L, clusters);
			Assert.assertEquals(true, false);
		}
		catch (ClusterRecommenderException e)
		{

		}
	}
	
}
