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

import java.util.Properties;
import java.util.Set;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.clustering.recommender.ClusterFromReferrerPeer;
import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.test.GenericPropertyHolder;
import io.seldon.test.peer.BasePeerTest;
import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class ClusterFromReferrerPeerTest extends BasePeerTest {

	@Autowired
	GenericPropertyHolder props;
	
	private static final int clusterId = 1;
	
	@Before 
	public void setup()
	{
		addReferrer("http://www.google.com", clusterId);
		Properties cprops = new Properties();
		cprops.setProperty(ClusterFromReferrerPeer.PROP, props.getClient());
		ClusterFromReferrerPeer.initialise(cprops);
	}
	
	@After
	public void tearDown()
	{
		clearClusterReferrer();
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

			
			
			Set<Integer> clusters = ClusterFromReferrerPeer.get().getClustersFromReferrer(props.getClient(),"http://www.google.com/somestuff/?v=k");
			Assert.assertNotNull(clusters);
			Assert.assertEquals(1, clusters.size());
			Assert.assertEquals(clusterId, (int)clusters.iterator().next());
		}
		finally
		{
			
		}
	}
}
