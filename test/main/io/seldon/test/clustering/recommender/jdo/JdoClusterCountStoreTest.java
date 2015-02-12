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

import java.util.Map;
import java.util.Properties;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.clustering.recommender.jdo.AsyncClusterCountFactory;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.test.GenericPropertyHolder;
import io.seldon.test.peer.BasePeerTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.seldon.clustering.recommender.ClusterCountNoImplementationException;
import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.JDOFactory;

public class JdoClusterCountStoreTest extends BasePeerTest {

	@Autowired
	GenericPropertyHolder props;
	
	@Before 
	public void setup()
	{
		AsyncClusterCountFactory.create(new Properties());
	}
	
	private void clearClusterCounts()
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) {
				public void process() {
					Query query = pm.newQuery("javax.jdo.query.SQL", "delete from  cluster_counts");
					query.execute();
				}
			});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts");
		}
		
	}
	
	private void clearDimensionData()
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "delete from dimension");
					query.execute();
			    	query = pm.newQuery( "javax.jdo.query.SQL", "delete from item_map_enum");
					query.execute();
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts");
		}
		
	}
	
	
	private void addDimension(final int dimension,final int attrId,final int valueId)
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "insert into dimension (dim_id,attr_id,value_id) values (?,?,?)");
					query.execute(dimension,attrId,valueId);
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts");
		}
		
	}
	
	private void addDimensionForItem(final long itemId,final int dimension)
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "insert into item_map_enum (item_id,attr_id,value_id) select ?,attr_id,value_id from dimension where dim_id=?");
					query.execute(itemId,dimension);
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts");
		}
		
	}
	
	@Test
	public void testTopGlobalCountsWithDecay() throws InterruptedException, ClusterCountNoImplementationException
	{
		clearClusterCounts();
		clusterCount.add(1, 1L,1D,1L,1L);
		clusterCount.add(2, 1L,1D,1L,1L);
		clusterCount.add(3, 1L,1D,1L,1L);
		clusterCount.add(1, 2L,2D,1L,1L);
		
		Thread.sleep(2000);

		Map<Long,Double> cmap = clusterCount.getTopCounts(3, 1, 3);
		
		Assert.assertEquals(1, cmap.size());
		for(Map.Entry<Long, Double> e : cmap.entrySet())
			logger.info("Top counts result for item "+e.getKey()+" value: "+e.getValue());
		
		Assert.assertEquals(1.24D, cmap.get(1L),0.4);

		//cleanup
		clearClusterCounts();
	}
	
	
	@Test
	public void testTopGlobalCounts() throws InterruptedException, ClusterCountNoImplementationException
	{
		clearClusterCounts();
		clusterCount.setAlpha(999999999); // 1 sec
		clusterCount.add(1, 1L,1D,1L,1L);
		clusterCount.add(2, 1L,1D,1L,1L);
		clusterCount.add(3, 1L,1D,1L,1L);
		clusterCount.add(1, 2L,2D,1L,1L);
		

		Map<Long,Double> cmap = clusterCount.getTopCounts(1, 1, 10000);
		
		Assert.assertEquals(1, cmap.size());
		
		Assert.assertEquals(3D, cmap.get(1L),0.01);

		//cleanup
		clearClusterCounts();
	}
	
	@Test
	public void testTopGlobalCountsForDimensionWithDecay() throws ClusterCountNoImplementationException, InterruptedException
	{
		clearClusterCounts();
		int dimension1 = 1;
		int dimension2 = 2;
		long item1 = 1;
		long item2 = 2;
		addDimension(dimension1, 1, 1);
		addDimensionForItem(item1, dimension1);
		addDimension(dimension2, 1, 2);
		addDimensionForItem(item2, dimension2);
		

		clusterCount.add(1, item1,1D,1L,1L);
		clusterCount.add(2, item1,1D,1L,1L);
		clusterCount.add(3, item1,1D,1L,1L);
		clusterCount.add(1, item2,20D,1L,1L);
		
		Thread.sleep(2000);

		Map<Long,Double> cmap = clusterCount.getTopCountsByDimension(dimension1, 3, 2, 1);
		
		Assert.assertEquals(1, cmap.size());
		
		Assert.assertEquals(0.406D, cmap.get(1L),0.1); //item 1 is returned even though count is lower as it is in correct dimension

		//cleanup
		clearClusterCounts();
		clearDimensionData();
	}
	
	@Test
	public void testTopGlobalCountsForDimension() throws ClusterCountNoImplementationException
	{
		clearClusterCounts();
		int dimension1 = 1;
		int dimension2 = 2;
		long item1 = 1;
		long item2 = 2;
		addDimension(dimension1, 1, 1);
		addDimensionForItem(item1, dimension1);
		addDimension(dimension2, 1, 2);
		addDimensionForItem(item2, dimension2);
		

		clusterCount.setAlpha(999999999); // 1 sec
		clusterCount.add(1, item1,1D,1L,1L);
		clusterCount.add(2, item1,1D,1L,1L);
		clusterCount.add(3, item1,1D,1L,1L);
		clusterCount.add(1, item2,20D,1L,1L);
		

		Map<Long,Double> cmap = clusterCount.getTopCountsByDimension(dimension1, 1, 2, 10000);
		
		Assert.assertEquals(1, cmap.size());
		
		Assert.assertEquals(3D, cmap.get(1L),0.01); //item 1 is returned even though count is lower as it is in correct dimension

		//cleanup
		clearClusterCounts();
		clearDimensionData();
	}
	
	@Test
	public void testTopCountsForDimensionWithDecay() throws ClusterCountNoImplementationException, InterruptedException
	{
		clearClusterCounts();
		int dimension1 = 1;
		int dimension2 = 2;
		long item1 = 1;
		long item2 = 2;
		addDimension(dimension1, 1, 1);
		addDimensionForItem(item1, dimension1);
		addDimension(dimension2, 1, 2);
		addDimensionForItem(item2, dimension2);
		

		clusterCount.setAlpha(999999999); // 1 sec
		clusterCount.add(1, item1,1D,1L,1L);
		clusterCount.add(1, item2,20D,1L,1L);
		
		Thread.sleep(2000);

		Map<Long,Double> cmap = clusterCount.getTopCountsByDimension(1, dimension1, 1, 3, 2, 1);
		
		Assert.assertEquals(1, cmap.size());
		
		Assert.assertEquals(0.135D, cmap.get(1L),0.01); //item 1 is returned even though count is lower as it is in correct dimension

		//cleanup
		clearClusterCounts();
		clearDimensionData();
	}
	
	@Test
	public void testTopCountsForDimension() throws ClusterCountNoImplementationException
	{
		clearClusterCounts();
		int dimension1 = 1;
		int dimension2 = 2;
		long item1 = 1;
		long item2 = 2;
		addDimension(dimension1, 1, 1);
		addDimensionForItem(item1, dimension1);
		addDimension(dimension2, 1, 2);
		addDimensionForItem(item2, dimension2);
		

		clusterCount.setAlpha(999999999); 
		clusterCount.add(1, item1,1D,1L,1L);
		clusterCount.add(1, item2,20D,1L,1L);
		

		Map<Long,Double> cmap = clusterCount.getTopCountsByDimension(1, dimension1, 1, 1, 2, 10000);
		
		Assert.assertEquals(1, cmap.size());
		
		Assert.assertEquals(1D, cmap.get(1L),0.01); //item 1 is returned even though count is lower as it is in correct dimension

		//cleanup
		clearClusterCounts();
		clearDimensionData();
	}
	
	@Test
	public void testTopCountsWithDecay() throws InterruptedException, ClusterCountNoImplementationException
	{
		clearClusterCounts();
		clusterCount.setAlpha(999999999); // 1 sec
		clusterCount.add(1, 1L,1D,1L,1L);
		clusterCount.add(1, 2L,20D,1L,1L);
		
		Thread.sleep(2000);
		
		Map<Long,Double> cmap = clusterCount.getTopCounts(1, 1L, 3L, 1, 1);
		
		Assert.assertEquals(1, cmap.size());
		
		Assert.assertEquals(2.706D, cmap.get(2L),0.01);

		//cleanup
		clearClusterCounts();
	}
	
	@Test
	public void testTopCounts1() throws InterruptedException, ClusterCountNoImplementationException
	{
		clearClusterCounts();
		clusterCount.setAlpha(999999999); // 1 sec
		clusterCount.add(1, 1L,1D,1L,1L);
		clusterCount.add(1, 2L,20D,1L,1L);
		

		Map<Long,Double> cmap = clusterCount.getTopCounts(1, 1L, 1L, 1, 100000);
		
		Assert.assertEquals(1, cmap.size());
		
		Assert.assertEquals(20D, cmap.get(2L),0.01);

		//cleanup
		clearClusterCounts();
	}
	
	
	@Test
	public void testTopCounts2() throws InterruptedException, ClusterCountNoImplementationException
	{
		clearClusterCounts();
		clusterCount.setAlpha(999999999); // 1 sec
		clusterCount.add(1, 1L,1D,1L,1L);
		clusterCount.add(1, 2L,20D,1L,1L);
		clusterCount.add(1, 3L,10D,1L,1L);
		

		Map<Long,Double> cmap = clusterCount.getTopCounts(1, 1L, 1L, 2, 10000);
		
		Assert.assertEquals(2, cmap.size());
		
		Assert.assertEquals(20D, cmap.get(2L),0.01);
		Assert.assertEquals(10D, cmap.get(3L),0.01);

		//cleanup
		clearClusterCounts();
	}
	
	
	@Test
	public void testAddCounts() throws InterruptedException
	{
		clearClusterCounts();
		clusterCount.setAlpha(1); // 1 sec
		clusterCount.add(1, 1L,1D,1L,1L);
		Thread.sleep(1000);
		clusterCount.add(1, 1L,1D,1L,2L);
		double count = clusterCount.getCount(1, 1,1,2L);
		Assert.assertEquals(1.367D, count,0.1);

		//cleanup
		clearClusterCounts();
	}
	
	
	@Test
	public void testAddCountsBadTime() throws InterruptedException  //time is ignored
	{
		clearClusterCounts();
		clusterCount.setAlpha(1); // 1 sec
		clusterCount.add(1, 1L,1D,1L,2L);
		Thread.sleep(1000);
		clusterCount.add(1, 1L,1D,1L,1L);
		double count = clusterCount.getCount(1, 1,1,2L);
		Assert.assertEquals(1.135D, count,0.3);

		//cleanup
		clearClusterCounts();
	}
	
	
}
