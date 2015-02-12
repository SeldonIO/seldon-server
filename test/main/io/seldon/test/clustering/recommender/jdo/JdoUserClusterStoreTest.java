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

package io.seldon.test.clustering.recommender.jdo;

import java.util.ArrayList;
import java.util.List;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.clustering.recommender.TransientUserClusterStore;
import io.seldon.clustering.recommender.UserCluster;
import io.seldon.clustering.recommender.jdo.JdoUserClusterStore;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.test.GenericPropertyHolder;
import io.seldon.test.peer.BasePeerTest;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.JDOFactory;

public class JdoUserClusterStoreTest extends BasePeerTest {

	@Autowired
	GenericPropertyHolder props;
	
	private void removeClusters()
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) {
				public void process() {
					Query query = pm.newQuery("javax.jdo.query.SQL", "delete from user_clusters;");
					query.execute();
					query = pm.newQuery("javax.jdo.query.SQL", "delete from user_clusters_transient;");
					query.execute();
				}
			});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clusters");
		}
		
	}
	
	
	private void updateClusters(final long userId,final int clusterId,final double weight)
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL","insert into user_clusters values (?,?,?)");
					query.execute(userId,clusterId,weight);
					query = pm.newQuery( "javax.jdo.query.SQL","insert ignore into cluster_group values (?,0)");
					query.execute(clusterId);
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to cluster for user "+userId);
		}
		
	}
	
	
	@Test
	public void addTransientClusters()
	{
		//ensure clean
		removeClusters();
		
		long userId = 1;
		List<UserCluster> clusters = new ArrayList<UserCluster>();
		for(int i=1;i<10;i++)
			clusters.add(new UserCluster(userId,i,0.5,1,0));
		TransientUserClusterStore tstore = (TransientUserClusterStore) userClusters;
		tstore.addTransientCluster(clusters);
		
		JdoUserClusterStore.TransientUserClusters tclusters = tstore.getTransientClusters(-1);
		
		Assert.assertEquals(clusters.size(), tclusters.getClusters().size());
				
		//cleanup
		removeClusters();
		
		
	}
	
	@Test 
	public void testGetUserCount()
	{
		int numClusters = 20;
		for(int i=1;i<=numClusters;i++)
			updateClusters(1,i,0.5);
		List<UserCluster> clusters = userClusters.getClusters(1L);
		Assert.assertEquals(numClusters, clusters.size());
		for(UserCluster cluster : clusters)
		{
			System.out.println(cluster.toString());
		}
		
		//cleanup 
		removeClusters();
	}
	
	@Test 
	public void testGetEmptyCount()
	{
		List<UserCluster> clusters = userClusters.getClusters(-1L);
		
		Assert.assertNotNull(clusters);
	}
	
	@Test 
	public void testGetNumberUsers()
	{
		int numUsers = 100;
		for(int i=1;i<=numUsers;i++)
			updateClusters(i,2,0.5);
		
		int num = userClusters.getNumUsersWithClusters();
		Assert.assertTrue(num >= 0);
		Assert.assertEquals(numUsers, num);
		
		//cleanup 
		removeClusters();
	}
	
	@Test
	public void testGetAllClusters()
	{
		int numUsers = 100;
		for(int i=1;i<numUsers;i++)
			updateClusters(i,2,0.5);
		
		int num = userClusters.getNumUsersWithClusters();
		List<UserCluster> clusters = userClusters.getClusters();
		int userCount = 0;
		long currentUser = -1;
		for(UserCluster cluster : clusters)
		{
			if (currentUser != cluster.getUser())
				userCount++;
			currentUser = cluster.getUser();
		}
		Assert.assertEquals(num, userCount);
		
		//cleanup 
		removeClusters();
	}
	
	

}
