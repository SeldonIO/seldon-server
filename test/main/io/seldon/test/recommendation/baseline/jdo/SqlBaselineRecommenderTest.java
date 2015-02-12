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

package io.seldon.test.recommendation.baseline.jdo;

import java.util.Map;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import junit.framework.Assert;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.recommendation.baseline.IBaselineRecommenderUtils;
import io.seldon.recommendation.baseline.jdo.SqlBaselineRecommenderUtils;
import io.seldon.test.GenericPropertyHolder;
import io.seldon.test.peer.BasePeerTest;

public class SqlBaselineRecommenderTest extends BasePeerTest {

	@Autowired
	GenericPropertyHolder props;
	
	@Autowired PersistenceManager pm;

	private void clearData()
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "delete from items_popular");
					query.execute();
					query = pm.newQuery( "javax.jdo.query.SQL", "delete from dimension");
					query.execute();
					query = pm.newQuery( "javax.jdo.query.SQL", "delete from item_map_enum");
					query.execute();

					
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts");
		}
	}
		
	
	
	private void addItemPopularity(final long itemId,final int count)
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "insert into items_popular (item_id,opsum) values (?,?)");
					query.execute(itemId,count);
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
	public void simpleTest()
	{
		try
		{
			final long item1 = 1L;
			final long item2 = 2L;
			final long item3 = 3L;
			final int dimension = 1;
			addDimension(dimension, 1, 1);
			addDimensionForItem(item1, dimension);
			addDimensionForItem(item2, dimension);
			addDimensionForItem(item3, dimension);
			addItemPopularity(item1, 10);
			addItemPopularity(item2, 5);
			addItemPopularity(item3, 1);
			
			IBaselineRecommenderUtils b = new SqlBaselineRecommenderUtils(props.getClient());
			Map<Long,Double> res = b.getPopularItems(dimension, 3);
			
			Assert.assertNotNull(res);
			Assert.assertEquals(3, res.size());
			Assert.assertEquals(1.0D, res.get(item1));
			Assert.assertEquals(0.5D, res.get(item2));
			Assert.assertEquals(0.1D, res.get(item3));
			
		}
		finally
		{
			clearData();
		}
	}
	
	
	
	
	@Test
	public void simpleTestWithDimensions()
	{
		try
		{
			final long item1 = 1L;
			final long item2 = 2L;
			final long item3 = 3L;
			final int dimension1 = 1;
			final int dimension2 = 2;
			addDimension(dimension1, 1, 1);
			addDimension(dimension2, 1, 2);
			addDimensionForItem(item1, dimension1);
			addDimensionForItem(item2, dimension2);
			addDimensionForItem(item3, dimension1);
			addItemPopularity(item1, 10);
			addItemPopularity(item2, 5);
			addItemPopularity(item3, 1);
			
			IBaselineRecommenderUtils b = new SqlBaselineRecommenderUtils(props.getClient());
			Map<Long,Double> res = b.getPopularItems(dimension2, 3);
			
			Assert.assertNotNull(res);
			Assert.assertEquals(1, res.size());
			Assert.assertEquals(1.0D, res.get(item2));
			
		}
		finally
		{
			clearData();
		}
	}
	
	
	@Test
	public void getAllMostPopularTest()
	{
		try
		{
			final long item1 = 1L;
			final long item2 = 2L;
			final long item3 = 3L;
			
			addItemPopularity(item1, 10);
			addItemPopularity(item2, 5);
			addItemPopularity(item3, 1);
			
			IBaselineRecommenderUtils b = new SqlBaselineRecommenderUtils(props.getClient());
			Map<Long,Double> res = b.getAllItemPopularity();
			
			Assert.assertNotNull(res);
			Assert.assertEquals(3, res.size());
			Assert.assertEquals(1.0D, res.get(item1));
			Assert.assertEquals(0.5D, res.get(item2));
			Assert.assertEquals(0.1D, res.get(item3));
			
		}
		finally
		{
			clearData();
		}
	}
}
