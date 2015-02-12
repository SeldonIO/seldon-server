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

import java.util.Set;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.db.jdo.JDOFactory;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.test.GenericPropertyHolder;
import io.seldon.test.peer.BasePeerTest;
import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.seldon.clustering.recommender.jdo.JdoClusterFromReferrer;
import io.seldon.db.jdo.DatabaseException;

public class jdoClusterFromReferrerTest extends BasePeerTest {

	@Autowired
	GenericPropertyHolder props;
	
	@Before 
	public void setup()
	{
		
	}
	
	private void addReferrer(final String referrer,final int cluster)
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) {
				public void process() {
					Query query = pm.newQuery("javax.jdo.query.SQL", "insert into cluster_referrer (referrer,cluster) values (?,?)");
					query.execute(referrer, cluster);
				}
			});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts");
		}
		
	}
	
	private void clearClusterReferrer()
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "delete from  cluster_referrer");
					query.execute();
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts");
		}
		
	}

	@Test
	public void simpleTest()
	{
		try
		{
			final int clusterId = 1;
			addReferrer("http://www.google.com", clusterId);
			
			JdoClusterFromReferrer c = new JdoClusterFromReferrer(props.getClient());
			Set<Integer> clusters = c.getClusters("http://www.google.com/somestuff/?v=k");
			Assert.assertNotNull(clusters);
			Assert.assertEquals(1, clusters.size());
			Assert.assertEquals(clusterId, (int)clusters.iterator().next());
		}
		finally
		{
			clearClusterReferrer();
		}
	}
	
	@Test
	public void multipleMatchesTest()
	{
		try
		{
			final int clusterId1 = 1;
			final int clusterId2 = 2;
			addReferrer("http://www.google.com", clusterId1);
			addReferrer("http://www.google.com/suffix", clusterId2);
			
			JdoClusterFromReferrer c = new JdoClusterFromReferrer(props.getClient());
			Set<Integer> clusters = c.getClusters("http://www.google.com/suffix/?v=k");
			Assert.assertNotNull(clusters);
			Assert.assertEquals(2, clusters.size());
			Assert.assertTrue(clusters.contains(clusterId1));
			Assert.assertTrue(clusters.contains(clusterId2));
		}
		finally
		{
			clearClusterReferrer();
		}
	}
	
	
	@Test
	public void testTimerReload() throws InterruptedException
	{
		try
		{
			final int clusterId = 1;
			addReferrer("http://www.google.com", clusterId);
			
			JdoClusterFromReferrer c = new JdoClusterFromReferrer(props.getClient(),5);
			Set<Integer> clusters = c.getClusters("http://www.google.com/somestuff/?v=k");
			Assert.assertNotNull(clusters);
			Assert.assertEquals(1, clusters.size());
			Assert.assertEquals(clusterId, (int)clusters.iterator().next());
			
			clusters = c.getClusters("http://www.reddit.com/somestuff/?v=k");
			Assert.assertNotNull(clusters);
			Assert.assertEquals(0, clusters.size());
			
			addReferrer("http://www.reddit.com", clusterId+1);
			
			Thread.sleep(10000);
			
			clusters = c.getClusters("http://www.reddit.com/somestuff/?v=k");
			Assert.assertNotNull(clusters);
			Assert.assertEquals(1, clusters.size());
			Assert.assertEquals(clusterId+1, (int)clusters.iterator().next());
		}
		finally
		{
			clearClusterReferrer();
		}
	}

}
