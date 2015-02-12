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

package io.seldon.test.trust.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.db.jdo.JDOFactory;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import io.seldon.test.GenericPropertyHolder;
import io.seldon.test.peer.BasePeerTest;
import io.seldon.trust.impl.CFAlgorithm;
import io.seldon.trust.impl.Recommendation;
import io.seldon.trust.impl.RecommendationResult;
import io.seldon.trust.impl.jdo.RecommendationPeer;
import junit.framework.Assert;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.seldon.api.Constants;
import io.seldon.db.jdo.DatabaseException;

public class SocialPredictRecommenderTest extends BasePeerTest {

	@Autowired
	GenericPropertyHolder props;
	
	@Autowired PersistenceManager pm;
	
	
	private void clearState()
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) {
				public void process() {
					Query query = pm.newQuery("javax.jdo.query.SQL", "delete from  dbpedia_item_tokens");
					query.execute();
					query = pm.newQuery("javax.jdo.query.SQL", "delete from  dbpedia_token_hits");
					query.execute();
					query = pm.newQuery("javax.jdo.query.SQL", "delete from  dbpedia_token_token");
					query.execute();
					query = pm.newQuery("javax.jdo.query.SQL", "delete from  ext_actions");
					query.execute();
					query = pm.newQuery("javax.jdo.query.SQL", "delete from  links");
					query.execute();
					query = pm.newQuery("javax.jdo.query.SQL", "delete from  link_type");
					query.execute();
					query = pm.newQuery("javax.jdo.query.SQL", "delete from  dbpedia_item_user");
					query.execute();
					query = pm.newQuery("javax.jdo.query.SQL", "delete from  dbpedia_item_user_searched");
					query.execute();
				}
			});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts");
		}
		
	}
	
	private void addItem(final long itemId)
	{
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "insert into items (item_id) values (?)");
					query.execute(itemId);
					query.closeAll();
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts");
		}
	}
	
	private void createCachedResult(final long itemId,final long userId,final long exItemId,final String exClientItemId,final String tokens,final double score)
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL","insert into dbpedia_item_user (user_id,item_id,ex_item_id,tokens,ex_client_item_id,score) values (?,?,?,?,?,?)");
			    	List<Object> args = new ArrayList<Object>();
			    	args.add(userId);
			    	args.add(itemId);
			    	args.add(exItemId);
			    	args.add(tokens);
			    	args.add(exClientItemId);
			    	args.add(score);
					query.executeWithArray(args.toArray());
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to create dbpedia_item_user for  user:"+userId+" item:"+itemId);
		}
		
	}
	
	
	
	@Test
	public void webSimilarityPeerSocialpredictTest()
	{
		try
		{
			clearState();
			Random r = new Random();
			final long userId = r.nextLong();

			final long[] itemIds = new long[]{1,2,3};
			final double[] scores = new double[] {1.0, 3.0, 1.5};
			List<Long> items = new ArrayList<Long>();
			double max = 0;
			for(int i=0;i<itemIds.length;i++)
			{
				if (scores[i] > max)
					max =  scores[i];
				items.add(itemIds[i]);
				addItem(itemIds[i]);
				createCachedResult(itemIds[i], userId, r.nextInt() , ""+r.nextInt(), "some tokens",scores[i]);
			}
			
			CFAlgorithm options = new CFAlgorithm();
			options.setName(props.getClient());
			List<CFAlgorithm.CF_RECOMMENDER> recommenders = new ArrayList<CFAlgorithm.CF_RECOMMENDER>();
			recommenders.add(CFAlgorithm.CF_RECOMMENDER.SOCIAL_PREDICT);
			options.setMaxRecommendersToUse(1);
			options.setRecommenders(recommenders);
			options.setRecommenderStrategy(CFAlgorithm.CF_STRATEGY.FIRST_SUCCESSFUL);
			options.setRecentArticlesForSV(10);
	    	MemCachePeer.delete(MemCacheKeys.getRecentItems(props.getClient(), Constants.DEFAULT_DIMENSION, options.getRecentArticlesForSV()));
			RecommendationPeer recPeer = new RecommendationPeer();
			
			RecommendationResult rres = recPeer.getRecommendations(userId, ""+userId,0, Constants.DEFAULT_DIMENSION, 3, options,null,null,null);
			List<Recommendation> recs = rres.getRecs();
			
			Assert.assertEquals(itemIds.length, recs.size());
			Assert.assertEquals(itemIds[1], recs.get(0).getContent());
			Assert.assertEquals(itemIds[2], recs.get(1).getContent());
			Assert.assertEquals(itemIds[0], recs.get(2).getContent());
		}
		finally
		{
			clearState();
		}

	}

}
