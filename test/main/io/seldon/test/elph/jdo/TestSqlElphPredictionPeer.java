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

package io.seldon.test.elph.jdo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.db.jdo.Transaction;
import io.seldon.test.peer.BasePeerTest;
import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.elph.ElphRecommender;
import io.seldon.elph.jdo.SqlElphPredictionPeer;
import io.seldon.test.GenericPropertyHolder;

public class TestSqlElphPredictionPeer  extends BasePeerTest {

	@Autowired
	GenericPropertyHolder props;
	
	@Autowired PersistenceManager pm;
	
	private void addPrediction(final long itemId,final String hypothesis,final double prob,final int c,final double entropy)
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) {
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "insert into elph_hypothesis values (?,?,?,?,?)");
			    	List<Object> args = new ArrayList<Object>();
			    	args.add(itemId);
			    	args.add(hypothesis);
			    	args.add(prob);
			    	args.add(c);
			    	args.add(entropy);
					query.executeWithArray(args.toArray());
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts");
		}
		
	}

	private void clearData()
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "delete from elph_hypothesis");
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
	
	@Test @Ignore
	public void hardwiredTest()
	{
		SqlElphPredictionPeer p = new SqlElphPredictionPeer(props.getClient());
		
		ArrayList<Long> recentItems = new ArrayList<Long>();
		recentItems.add(127844L);
		Map<Long,Double> scores = p.getPredictions(recentItems,1);
		
		for(Map.Entry<Long, Double> e : scores.entrySet())
			System.out.println(""+e.getKey()+"->"+e.getValue());
	}
	
	@Test
	public void simpleTest()
	{
		try
		{
			addPrediction(1L, "12345", 0.1, 20, 1.0);
			addPrediction(2L, "12345", 0.1, 20, 2.0);
			addDimension(1, 1, 1);
			addDimensionForItem(1L, 1);
			addDimensionForItem(2L, 1);
			
			SqlElphPredictionPeer p = new SqlElphPredictionPeer(props.getClient());
			
			ArrayList<Long> recentItems = new ArrayList<Long>();
			recentItems.add(12345L);
			Map<Long,Double> scores = p.getPredictions(recentItems,1);
			
			Assert.assertEquals(2, scores.size());
			double score1 = scores.get(1L);
			double score2 = scores.get(2L);
			Assert.assertTrue(score1 > score2);
		}
		finally
		{
			clearData();
		}
	}
	
	@Test
	public void mixedMatches()
	{
		try
		{
		addPrediction(1L, "12345 67890", 0.1, 20, 1.0);
		addPrediction(2L, "12345", 0.1, 20, 1.0);
		addPrediction(3L, "12345", 0.1, 10, 1.0);
		addDimension(1, 1, 1);
		addDimensionForItem(1L, 1);
		addDimensionForItem(2L, 1);
		addDimensionForItem(3L, 1);
		
		SqlElphPredictionPeer p = new SqlElphPredictionPeer(props.getClient());
		
		ArrayList<Long> recentItems = new ArrayList<Long>();
		recentItems.add(12345L);
		recentItems.add(67890L);
		Map<Long,Double> scores = p.getPredictions(recentItems,1);
		
		Assert.assertEquals(3, scores.size());
		double score1 = scores.get(1L);
		double score2 = scores.get(2L);
		double score3 = scores.get(3L);
		Assert.assertTrue(score1 > score2);
		Assert.assertTrue(score2 > score3);
		}
		finally
		{
			clearData();
		}
	}
	
	
	@Test
	public void testRecommender()
	{
		try
		{
		addPrediction(1L, "12345 67890", 0.1, 20, 1.0);
		addPrediction(2L, "12345", 0.1, 20, 1.0);
		addDimension(1, 1, 1);
		addDimensionForItem(1L, 1);
		addDimensionForItem(2L, 1);
		
		SqlElphPredictionPeer p = new SqlElphPredictionPeer(props.getClient());
		
		ArrayList<Long> recentItems = new ArrayList<Long>();
		recentItems.add(12345L);
		recentItems.add(67890L);
		ElphRecommender e = new ElphRecommender(props.getClient(), p);
		Map<Long,Double> scores = e.recommend(recentItems, new HashSet<Long>(), 1);
		
		Assert.assertEquals(2, scores.size());
		double score1 = scores.get(1L);
		double score2 = scores.get(2L);
		Assert.assertTrue(score1 > score2);
		}
		finally
		{
			clearData();
		}
	}
}
