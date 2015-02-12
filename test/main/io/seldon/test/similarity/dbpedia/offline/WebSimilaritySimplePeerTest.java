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

package io.seldon.test.similarity.dbpedia.offline;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.api.resource.RecommendedUserBean;
import io.seldon.clustering.recommender.UserCluster;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import io.seldon.test.GenericPropertyHolder;
import io.seldon.test.peer.BasePeerTest;
import io.seldon.trust.impl.CFAlgorithm;
import io.seldon.trust.impl.jdo.RecommendationPeer;
import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;

import io.seldon.api.Constants;
import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.similarity.dbpedia.ISemanticSimilarityOfflineMatching;
import io.seldon.similarity.dbpedia.WebSimilaritySimplePeer;
import io.seldon.trust.impl.SharingRecommendation;

@ContextConfiguration({"classpath*:/test-mgm-service-ctx.xml"})
public class WebSimilaritySimplePeerTest extends BasePeerTest {

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
					query = pm.newQuery("javax.jdo.query.SQL", "delete from  dbpedia_keyword_user");
					query.execute();
					query = pm.newQuery("javax.jdo.query.SQL", "delete from  dbpedia_keyword");
					query.execute();
					query = pm.newQuery("javax.jdo.query.SQL", "delete from  user_attr");
					query.execute();
					query = pm.newQuery("javax.jdo.query.SQL", "delete from  user_map_varchar");
					query.execute();
					query = pm.newQuery("javax.jdo.query.SQL", "delete from  users");
					query.execute();

				}
			});
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
	
	private void createItemHits(final long item,final long tokenId,final String token,final int hits)
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL","insert into dbpedia_token_hits (token_id,tokens,hits) values (?,?,?)");
					query.execute(tokenId,token,hits);
			    	query = pm.newQuery( "javax.jdo.query.SQL","insert into dbpedia_item_tokens values (?,?)");
					query.execute(item,tokenId);
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to create item hits for item "+item);
		}
		
	}
	
	private void createHasBeenSearched(final long itemId)
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL","insert into dbpedia_item_user_searched values (?)");
					query.execute(itemId);
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to insert into dbpedia_item_user_seached item:"+itemId);
		}
		
	}
	private void addUser(final long user,final String clientUserId)
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL","insert into users (user_id,client_user_id,first_op,last_op,type,active,num_op) values (?,?,now(),now(),1,1,1)");
					query.execute(user,clientUserId);
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to insert into users "+user);
		}
		
	}
	private void createSimilarity(final long item1,final long item2)
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL","insert into dbpedia_token_token values (?,?,1,1,1,2,0,1)");
					query.execute(item1,item2);
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to insert into dbpedia_token_token for "+item1+" and "+item2);
		}
		
	}
	
	
	private void createLinkType(final String type,final long id)
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL","insert into link_type values (?,?,1)");
					query.execute(id,type);
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to insert into link_type for "+type+" and "+id);
		}
		
	}
	
	private void createFriendship(final long user1,final long user2)
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL","insert into links values (0,?,?,1,1,now())");
					query.execute(user1,user2);
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to insert into links for "+user1+" and "+user2);
		}
		
	}
	
	
	private void createFbId(final long user1,final String fbId)
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL","insert ignore into user_attr (attr_id,name,type) values (2,'facebookId','VARCHAR')");
					query.execute(user1,fbId);
			    	query = pm.newQuery( "javax.jdo.query.SQL","insert into user_map_varchar (user_id,attr_id,value) values (?,2,?)");
					query.execute(user1,fbId);
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to create fb id for "+user1);
		}
		
	}
	
	private void addAction(final long userId,final long itemId)
	{
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "insert into ext_actions values (0,?,?,1,1,now(),0,?,?)");
			    	List<Object> args = new ArrayList<Object>();
			    	args.add(userId);
			    	args.add(itemId);
			    	args.add(""+userId);
			    	args.add(""+itemId);
					query.executeWithArray(args.toArray());
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear cluster counts");
		}
	}
	
	public static class DummyOfflineMatcher implements ISemanticSimilarityOfflineMatching
	{
		boolean called = false;

		@Override
		public void startOfflineMatching(long userId) {
			called = true;
		}
		
		public boolean getCalled()
		{
			return called;
		}
		
	}
	
	private void createKeywordMatches(final long u1,final long u2,final String keyword,final String matchStr,final double score)
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL","insert ignore into dbpedia_keyword values (0,?,unix_timestamp())");
					query.execute(keyword);
					query = pm.newQuery( "javax.jdo.query.SQL","insert into dbpedia_keyword_user select k_id,?,?,?,1,? from dbpedia_keyword where keyword=?");
					List<Object> args = new ArrayList<Object>();
			    	args.add(u1);
			    	args.add(u2);
			    	args.add(matchStr);
			    	args.add(score);
			    	args.add(keyword);
					query.executeWithArray(args.toArray());
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to insert into dbpedia_keyword_token "+u1+"->"+u2+" "+keyword+"<-->"+matchStr+" "+score);
		}
		
	}
	
	@Ignore @Test
	public void highLevelKeywordMgMTest()
	{
		try
		{
			Random r = new Random();
			final long u1 = r.nextLong();
			final long u2 = 2;
			final String u2FbId = "123457";
			addUser(u1, "u1");
			addUser(u2, "u2");
			createFbId(u2, u2FbId);
			final String keyword = "david bowie";
			final String matchStr = "ziggy stardust";
			List<String> tags = new ArrayList<String>();
			tags.add(keyword);
			createKeywordMatches(u1, u2, keyword, matchStr, 1);
			CFAlgorithm options = new CFAlgorithm();
			options.setName("rockol_it");
			RecommendationPeer p = new RecommendationPeer();
		
		
			List<RecommendedUserBean> recs = p.getAnalysis(options).sharingRecommendation("604041084",u1, null, null, tags, 1, options);
		
			Assert.assertNotNull(recs);
			Assert.assertTrue(recs.size()>0);
		}
		finally
		{
			clearState();
		}
	}
	
	@Test
	public void highLevelKeywordTest()
	{
		try
		{
			Random r = new Random();
			final long u1 = r.nextLong();
			final long u2 = 2;
			final String u2FbId = "fb_2";
			addUser(u1, "u1");
			addUser(u2, "u2");
			createFbId(u2, u2FbId);
			final String keyword = "david bowie";
			final String matchStr = "ziggy stardust";
			List<String> tags = new ArrayList<String>();
			tags.add(keyword);
			createKeywordMatches(u1, u2, keyword, matchStr, 1);
			CFAlgorithm options = new CFAlgorithm();
			options.setName(props.getClient());
			RecommendationPeer p = new RecommendationPeer();
		
		
			List<RecommendedUserBean> recs = p.getAnalysis(options).sharingRecommendation("",u1, null, null, tags, 1, options);
		
			Assert.assertEquals(1, recs.size());
			RecommendedUserBean s = recs.get(0);
			Assert.assertEquals(s.getReasons().get(0), matchStr);
			Assert.assertEquals(u2FbId, s.getUser());
		}
		finally
		{
			clearState();
		}
	}
	
	@Test
	public void simpleKeywordTest()
	{
		try
		{
			final long u1 = 1;
			final long u2 = 2;
			final String keyword = "david bowie";
			final String matchStr = "ziggy stardust";
			List<String> tags = new ArrayList<String>();
			tags.add(keyword);
			createKeywordMatches(u1, u2, keyword, matchStr, 1);
			List<SharingRecommendation> recs = webSimilaritySimpleStore.getSharingRecommendationsForFriends(u1, tags);
			Assert.assertEquals(1, recs.size());
			SharingRecommendation s = recs.get(0);
			Assert.assertEquals(s.getReasons().get(0), matchStr);
			Assert.assertEquals(u2, (long)s.getUserId());
		}
		finally
		{
			clearState();
		}
	}
	
	
	@Test
	public void simpleKeywordMultipleResultsTest()
	{
		try
		{
			final long u1 = 1;
			final long u2 = 2;
			final long u3 = 3;
			final String keyword = "david bowie";
			final String matchStr = "ziggy stardust";
			List<String> tags = new ArrayList<String>();
			tags.add(keyword);
			createKeywordMatches(u1, u2, keyword, matchStr, 1);
			createKeywordMatches(u1, u3, keyword, matchStr, 0.5);
			List<SharingRecommendation> recs = webSimilaritySimpleStore.getSharingRecommendationsForFriends(u1, tags);
			Assert.assertEquals(2, recs.size());
			SharingRecommendation s = recs.get(0);
			Assert.assertEquals(s.getReasons().get(0), matchStr);
			Assert.assertEquals(u2, (long)s.getUserId());
		}
		finally
		{
			clearState();
		}
	}
	
	
	@Test
	public void webSimilarityPeerSocialpredictTest()
	{
		try
		{
			clearState();
			final long userId = 1;

			final long[] itemIds = new long[]{1,2,3};
			final double[] scores = new double[] {1.0, 3.0, 1.5};
			Random r = new Random();
			List<Long> items = new ArrayList<Long>();
			double max = 0;
			for(int i=0;i<itemIds.length;i++)
			{
				if (scores[i] > max)
					max =  scores[i];
				items.add(itemIds[i]);
				createCachedResult(itemIds[i], userId, r.nextInt() , ""+r.nextInt(), "some tokens",scores[i]);
			}
			WebSimilaritySimplePeer p = new WebSimilaritySimplePeer(props.getClient(), webSimilaritySimpleStore);
			Map<Long,Double> res = p.getRecommendedItems(userId, Constants.DEFAULT_DIMENSION, items, new HashSet<Long>(), 0, 3);
			Assert.assertEquals(itemIds.length, res.size());
			for(int i=0;i<itemIds.length;i++)
				Assert.assertEquals(scores[i]/max, res.get(itemIds[i]),0.01);
		}
		finally
		{
			clearState();
		}

	}
	
	@Test
	public void webSimilarityPeerSocialpredictTestWithExclusions()
	{
		try
		{
			clearState();
			final long userId = 1;

			final long[] itemIds = new long[]{1,2,3};
			final double[] scores = new double[] {1.0, 3.0, 1.5};
			final boolean[] excluded = new boolean[] {false,true,false};
			Random r = new Random();
			List<Long> items = new ArrayList<Long>();
			Set<Long> excludedItems = new HashSet<Long>();
			double max = 0;
			int numNonExcluded = 0;
			for(int i=0;i<itemIds.length;i++)
			{
				if (!excluded[i] && scores[i] > max)
					max =  scores[i];
				if (excluded[i])
					excludedItems.add(itemIds[i]);
				else
					numNonExcluded++;
				items.add(itemIds[i]);
				createCachedResult(itemIds[i], userId, r.nextInt() , ""+r.nextInt(), "some tokens",scores[i]);
			}
			WebSimilaritySimplePeer p = new WebSimilaritySimplePeer(props.getClient(), webSimilaritySimpleStore);
			Map<Long,Double> res = p.getRecommendedItems(userId, Constants.DEFAULT_DIMENSION, items, excludedItems, 0, 3);
			Assert.assertEquals(numNonExcluded, res.size());
			for(int i=0;i<itemIds.length;i++)
				if (!excluded[i])
					Assert.assertEquals(scores[i]/max, res.get(itemIds[i]),0.01);
				else
					Assert.assertFalse(res.containsKey(itemIds[i]));
		}
		finally
		{
			clearState();
		}

	}
	
	@Test
	public void webSimilarityPeerSocialpredictTestWithMemcacheExclusions()
	{
		try
		{
			clearState();
			Random r = new Random();
			final long userId = r.nextLong();

			final long[] itemIds = new long[]{1,2,3};
			final double[] scores = new double[] {1.0, 3.0, 1.5};
			final boolean[] excluded = new boolean[] {false,true,false};

			List<Long> items = new ArrayList<Long>();
			Set<Long> excludedItems = new HashSet<Long>();
			double max = 0;
			int numNonExcluded = 0;
			for(int i=0;i<itemIds.length;i++)
			{
				if (!excluded[i] && scores[i] > max)
					max =  scores[i];
				if (excluded[i])
					excludedItems.add(itemIds[i]);
				else
					numNonExcluded++;
				items.add(itemIds[i]);
				createCachedResult(itemIds[i], userId, r.nextInt() , ""+r.nextInt(), "some tokens",scores[i]);
			}
			WebSimilaritySimplePeer p = new WebSimilaritySimplePeer(props.getClient(), webSimilaritySimpleStore);
			Map<Long,Double> res = p.getRecommendedItems(userId, Constants.DEFAULT_DIMENSION, items, new HashSet<Long>(), 10, 3);
			res = p.getRecommendedItems(userId, Constants.DEFAULT_DIMENSION, items, excludedItems, 0, 3);
			Assert.assertEquals(numNonExcluded, res.size());
			for(int i=0;i<itemIds.length;i++)
				if (!excluded[i])
					Assert.assertEquals(scores[i]/max, res.get(itemIds[i]),0.01);
				else
					Assert.assertFalse(res.containsKey(itemIds[i]));
		}
		finally
		{
			clearState();
		}

	}
	
	@Test
	public void testSingleUserLimitSharingRecommendation()
	{
		clearState();
		Random rand = new Random();
		final long userId = rand.nextLong();
		final long userFriend = 4;
		final String userFriendFbId = "fb_2";
		final long userFriend2 = 5;
		final String userFriendFbId2 = "fb_2";
		final long itemId1 = 2;
		final long exItemId1 = 3;
		final long itemId2 = 3;
		final long exItemId2 = 4;
		final String exClientItemId1 = "item3";
		final String exClientItemId2 = "item4";
		final String tokens = "tokens";
		String memcacheKey = MemCacheKeys.getSharingRecommendationsForItemSetKey("test", userId);
		try
		{
			createCachedResult(itemId1, userFriend, exItemId1, exClientItemId1, tokens,1D);
			createCachedResult(itemId1, userFriend2, exItemId2, exClientItemId2, tokens,1D);
			addAction(userFriend,exItemId1);
			createLinkType("facebook",1);
			addUser(userFriend,"userFriend1");
			addUser(userFriend2,"userFriend2");
			createFriendship(userId,userFriend);
			addAction(userFriend2,exItemId2);
			createFriendship(userId,userFriend2);
			createFbId(userFriend, userFriendFbId);
			createFbId(userFriend2, userFriendFbId2);
			CFAlgorithm options = new CFAlgorithm();
			options.setName(props.getClient());
			RecommendationPeer p = new RecommendationPeer();
			
			List<RecommendedUserBean> recs = p.getAnalysis(options).sharingRecommendation("",userId, itemId1, "facebook", null, 2, options);
			Assert.assertEquals(2, recs.size()); 
			recs = p.getAnalysis(options).sharingRecommendation("",userId, itemId1, "facebook", null, 1, options);
			Assert.assertEquals(1, recs.size()); 
			for(RecommendedUserBean r : recs)
			{
				System.out.println("user:"+r.getClientUserId());
				for(String reason : r.getReasons())
					System.out.println("reason:"+reason);
			}
		}
		finally
		{
			clearState();
			MemCachePeer.delete(memcacheKey);
		}
	}
	
	
	@Test
	public void simpleSocialPredictTest()
	{
		try
		{
			clearState();
			final long userId = 1;

			final long[] itemIds = new long[]{1,2,3};
			final double[] scores = new double[] {1.0, 3.0, 1.5};
			Random r = new Random();
			List<Long> items = new ArrayList<Long>();
			for(int i=0;i<itemIds.length;i++)
			{
				items.add(itemIds[i]);
				createCachedResult(itemIds[i], userId, r.nextInt() , ""+r.nextInt(), "some tokens",scores[i]);
			}
			Map<Long,Double> res = webSimilaritySimpleStore.getSocialPredictionRecommendations(userId, items.size());
			
			Assert.assertEquals(itemIds.length, res.size());
			for(int i=0;i<itemIds.length;i++)
				Assert.assertEquals(scores[i], res.get(itemIds[i]));
		}
		finally
		{
			clearState();
		}
	}
	
	@Test
	public void simpleSocialPredictTestWithMultipleItemsForUser()
	{
		try
		{
			clearState();
			final long userId = 1;

			final long[] itemIds = new long[]{1,2,2,3};
			final double[] scores = new double[] {1.0, 1.0, 2.0, 1.5};
			Random r = new Random();
			List<Long> items = new ArrayList<Long>();
			for(int i=0;i<itemIds.length;i++)
			{
				items.add(itemIds[i]);
				createCachedResult(itemIds[i], userId, r.nextInt() , ""+r.nextInt(), "some tokens",scores[i]);
			}
			Map<Long,Double> res = webSimilaritySimpleStore.getSocialPredictionRecommendations(userId, items.size());
			
			Assert.assertEquals(3, res.size());
			Assert.assertEquals(3.0D, res.get(2L));
		}
		finally
		{
			clearState();
		}
	}
	
	@Test 
	public void checkEmptyItemsSocialPredict()
	{
		Map<Long,Double> res = webSimilaritySimpleStore.getSocialPredictionRecommendations(1L, 5);
		Assert.assertEquals(0, res.size());
	}
	
	@Test
	public void testOfflineMatcherStart()
	{
		String client = "test";
		long userId = 1L;
		long itemId = 1L;
		String linkType = "facebook";
		String memcacheKey = MemCacheKeys.getSharingRecommendationKey(client, userId, itemId, linkType);
		MemCachePeer.delete(memcacheKey);
		try
		{
			DummyOfflineMatcher dummyMatching = new DummyOfflineMatcher();
			Assert.assertEquals(true, dummyMatching.getCalled());
		}
		finally
		{
			clearState();
			MemCachePeer.delete(memcacheKey);
		}
	}
	
	@Test
	public void testMemcachedMapResults()
	{
		long userId = 1L;
		long itemId = 1L;
		String memcacheKey = MemCacheKeys.getSharingRecommendationsForItemSetKey("test", userId);
		try
		{
			SharingRecommendation share = new SharingRecommendation(userId,itemId);
			List<SharingRecommendation> l = new ArrayList<SharingRecommendation>();
			l.add(share);
			Map<Long,List<SharingRecommendation>> map = new HashMap<Long,List<SharingRecommendation>>();
			map.put(itemId, l);
			MemCachePeer.put(memcacheKey,map);
		
			WebSimilaritySimplePeer p = new WebSimilaritySimplePeer("test", webSimilaritySimpleStore,"facebook",false);
			List<SharingRecommendation> sharesFound = p.getSharingRecommendationsForFriends(userId, itemId);
			Assert.assertNotNull(sharesFound);
			Assert.assertEquals(1, sharesFound.size());
			Assert.assertEquals(0, sharesFound.get(0).getItemIds().size());
		}
		finally
		{
			MemCachePeer.delete(memcacheKey);
		}
	}
	
	@Test 
	public void mergeTestForUnknownItems() // for postprocessing
	{
		WebSimilaritySimplePeer p = new WebSimilaritySimplePeer("test", webSimilaritySimpleStore);
		List<Long> items = new ArrayList<Long>();
		items.add(-1L); 
		items.add(-2L); 
		items.add(-3L); 

		List<Long> res = p.merge(2724L, items, 0.01f);
		int pos = 1;
		for(Long i : res)
			System.out.println(""+(pos++)+":"+i);
		Assert.assertEquals(3, res.size());
		Assert.assertTrue(res.get(0).equals(-1L));
		Assert.assertTrue(res.get(1).equals(-2L));
		Assert.assertTrue(res.get(2).equals(-3L));
	}
	
	@Test
	public void sortTest()
	{
		//Clear state
		clearState();
		try
		{
		Random r = new Random();
		WebSimilaritySimplePeer p = new WebSimilaritySimplePeer("test", webSimilaritySimpleStore);
		List<Long> items = new ArrayList<Long>();
		long userId = r.nextLong();
		long item1 = r.nextLong();
		long token1 = 1;
		long item2 = r.nextLong();
		long token2 = 2;
		createItemHits(item1,token1,"item1",20);
		createItemHits(item2,token2,"item2",20);

		createSimilarity(token1,token2);
		addAction(userId,item2);
		items.add(item1);
		items.add(100L);
		
		List<Long> sorted = p.sort(userId, items);
		Assert.assertEquals(1, sorted.size());
		Assert.assertEquals(item1, (long) sorted.get(0)); 
		int pos = 1;
		for(Long i : sorted)
			System.out.println(""+(pos++)+":"+i);
		}
		finally
		{
		//Clear state
		clearState();
		}
		
	}
	
	@Test
	public void mergeTest()
	{
		try
		{
		Random r = new Random();
		WebSimilaritySimplePeer p = new WebSimilaritySimplePeer("test", webSimilaritySimpleStore);
		List<Long> items = new ArrayList<Long>();
		long userId = r.nextLong();
		long item1 = r.nextLong();
		long token1 = 1;
		long item2 = r.nextLong();
		long token2 = 2;
		createItemHits(item1,token1,"item1",20);
		createItemHits(item2,token2,"item2",20);

		createSimilarity(token1,token2);
		addAction(userId,item2);
		items.add(item1);
		items.add(100L);
		
		List<Long> sorted = p.merge(userId, items, 0.5f);
		Assert.assertEquals(2, sorted.size());
		Assert.assertEquals(item1, (long) sorted.get(0)); 
		int pos = 1;
		for(Long i : sorted)
			System.out.println(""+(pos++)+":"+i);
		}
		finally
		{
		//Clear state
		clearState();
		}
		
	}
	
	@Test
	public void mergeTestLowWeightForSortedItems()
	{
		try
		{
		Random r = new Random();
		WebSimilaritySimplePeer p = new WebSimilaritySimplePeer("test", webSimilaritySimpleStore);
		List<Long> items = new ArrayList<Long>();
		long userId = r.nextLong();
		long item1 = r.nextLong();
		long token1 = 1;
		long item2 = r.nextLong();
		long token2 = 2;
		createItemHits(item1,token1,"item1",20);
		createItemHits(item2,token2,"item2",20);

		createSimilarity(token1,token2);
		addAction(userId,item2);
		long itemNotSorted = 100L;
		items.add(itemNotSorted);
		items.add(item1);

		
		List<Long> sorted = p.merge(userId, items, 0.99f);
		Assert.assertEquals(2, sorted.size());
		Assert.assertEquals(itemNotSorted, (long) sorted.get(0)); 
		int pos = 1;
		for(Long i : sorted)
			System.out.println(""+(pos++)+":"+i);
		}
		finally
		{
		//Clear state
		clearState();
		}
		
	}
	
	@Test @Ignore
	public void mergeTestLowWeightForSortedItemsPrev() // for postprocessing
	{
		try
		{
		WebSimilaritySimplePeer p = new WebSimilaritySimplePeer("test", webSimilaritySimpleStore);
		List<Long> items = new ArrayList<Long>();
		items.add(1995963L); //random other article
		items.add(2001083L); //Facebook related article
		items.add(2035300L); //Lady GaGa article

		List<Long> res = p.merge(2724L, items, 0.01f);
		int pos = 1;
		for(Long i : res)
			System.out.println(""+(pos++)+":"+i);
		Assert.assertEquals(3, res.size());
		Assert.assertTrue(!res.get(0).equals(1995963L));
		Assert.assertTrue(res.get(2).equals(1995963L));
		}
		finally
		{
			clearState();
		}
	}
	
	@Test
	public void getUserDimClusters()
	{
		long userid = 2724;
		Set<String> clientItemIds = new HashSet<String>();
		clientItemIds.add("164232989253");
		clientItemIds.add("19691681472");
		List<UserCluster> clusters = this.webSimilaritySimpleStore.getUserDimClusters(userid, clientItemIds);
		for(UserCluster c : clusters)
			System.out.println("Cluster "+c.getCluster()+" weight:"+c.getWeight());
	}
	
	@Test 
	public void singleUserSingleItemTest()
	{
		try
		{
		List<Long> users = new ArrayList<Long>();
		List<Long> items = new ArrayList<Long>();
		
		Random ran = new Random();
		long userId = ran.nextLong();
		long item1 = ran.nextLong();
		long token1 = 1;
		long item2 = ran.nextLong();
		long token2 = 2;
		createItemHits(item1,token1,"item1",20);
		createItemHits(item2,token2,"item2",20);

		createSimilarity(token1,token2);
		addAction(userId,item2);
		
		
		users.add(userId);
		items.add(item1);
		//items.add(item2);
		
		List<SharingRecommendation> recs = this.webSimilaritySimpleStore.getSharingRecommendations(users, items);
		Assert.assertEquals(1, recs.size());
		for(SharingRecommendation r : recs)
		{
			System.out.println("user:"+r.getUserId()+" item:"+r.getItemId());
			Assert.assertEquals(1, r.getReasons().size());
			for(String reason : r.getReasons())
				System.out.println("reason:"+reason);
		}
		}
		finally
		{
		//Clear state
		clearState();
		}
	}
	
	@Test 
	public void singleUserMultipleItemsTest()
	{
		try
		{
		List<Long> users = new ArrayList<Long>();
		List<Long> items = new ArrayList<Long>();
		
		Random ran = new Random();
		long userId = ran.nextLong();
		long item1 = ran.nextLong();
		long token1 = 1;
		long item2 = ran.nextLong();
		long token2 = 2;
		long item3 = ran.nextLong();
		long token3 = 3;
		createItemHits(item1,token1,"item1",20);
		createItemHits(item2,token2,"item2",20);
		createItemHits(item3,token3,"item3",20);

		createSimilarity(token1,token2);
		createSimilarity(token3,token2);
		addAction(userId,item2);
		
		users.add(userId);
		items.add(item1);
		items.add(item3);
		
		List<SharingRecommendation> recs = this.webSimilaritySimpleStore.getSharingRecommendations(users, items);
		Assert.assertEquals(2, recs.size());
		for(SharingRecommendation r : recs)
		{
			System.out.println("user:"+r.getUserId()+" item:"+r.getItemId());
			for(String reason : r.getReasons())
				System.out.println("reason:"+reason);
		}
		}
		finally
		{
		//Clear state
		clearState();
		}
	}
	
	@Test 
	public void singleUserFriendsTestWithLinkType()
	{
		//Clear state
		clearState();
		try
		{
		List<Long> items = new ArrayList<Long>();
		List<Long> users = new ArrayList<Long>();
		
		Random ran = new Random();
		long user1 = ran.nextLong();
		long userFriend = ran.nextLong();
		long item1 = ran.nextLong();
		long token1 = 1;
		long item2 = ran.nextLong();
		long token2 = 2;
		createItemHits(item1,token1,"item1",20);
		createItemHits(item2,token2,"item2",20);

		createSimilarity(token1,token2);
		addAction(userFriend,item2);
		createLinkType("facebook",1);
		createFriendship(user1,userFriend);
		users.add(user1);
		items.add(item1);
		
		List<SharingRecommendation> recs = this.webSimilaritySimpleStore.getSharingRecommendationsForFriends(user1, "facebook", items);
		Assert.assertEquals(1, recs.size());
		for(SharingRecommendation r : recs)
		{
			System.out.println("user:"+r.getUserId()+" item:"+r.getItemId());
			for(String reason : r.getReasons())
				System.out.println("reason:"+reason);
		}
		}
		finally
		{
		//Clear state
		clearState();
		}
	}
	
	@Test 
	public void singleUserFriendsTestFull()
	{
		
		clearState();
		try
		{
		Random ran = new Random();
		long user1 = ran.nextLong();
		long userFriend = ran.nextLong();
		long item1 = ran.nextLong();
		long token1 = 1;
		long item2 = ran.nextLong();
		long token2 = 2;
		createItemHits(item1,token1,"item1",20);
		createItemHits(item2,token2,"item2",20);

		createSimilarity(token1,token2);
		addAction(userFriend,item2);
		createLinkType("facebook",1);
		createFriendship(user1,userFriend);
		
		
		List<SharingRecommendation> recs = this.webSimilaritySimpleStore.getSharingRecommendationsForFriendsFull(user1,item1);
		Assert.assertEquals(1, recs.size());
		for(SharingRecommendation r : recs)
		{
			System.out.println("user:"+r.getUserId()+" item:"+r.getItemId());
			for(String reason : r.getReasons())
				System.out.println("reason:"+reason);
		}
		}
		finally
		{
			clearState();
		}
	}
	
	
	@Test 
	public void singleUserFriendsTestFullRanFromPeer()
	{
		
		clearState();
		Random ran = new Random();
		long user1 = ran.nextLong();
		long userFriend = ran.nextLong();
		long item1 = ran.nextLong();
		long token1 = 1;
		long item2 = ran.nextLong();
		long token2 = 2;
		String key = MemCacheKeys.getDbpediaHasBeenSearched(props.getClient(), item1);
		try
		{

			createItemHits(item1,token1,"item1",20);
			createItemHits(item2,token2,"item2",20);

			createSimilarity(token1,token2);
			addAction(userFriend,item2);
			createLinkType("facebook",1);
			createFriendship(user1,userFriend);
			
			WebSimilaritySimplePeer p = new WebSimilaritySimplePeer(props.getClient(), webSimilaritySimpleStore,"facebooK",true);
			List<SharingRecommendation> recs = p.getSharingRecommendationsForFriends(user1, item1);
			
			Assert.assertEquals(1, recs.size());
			for(SharingRecommendation r : recs)
			{
				System.out.println("user:"+r.getUserId()+" item:"+r.getItemId());
				for(String reason : r.getReasons())
					System.out.println("reason:"+reason);
			}
			
			Boolean searchedFromMemcache = (Boolean) MemCachePeer.get(key);
			Assert.assertNotNull(searchedFromMemcache);
			Assert.assertFalse(searchedFromMemcache);
		}
		finally
		{
			clearState();
			MemCachePeer.delete(key);
		}
	}
	
	/*
	 * Check no results are returned if the do full search flag is false
	 */
	@Test 
	public void singleUserFriendsTestFullButFailAsFlagFalse()
	{
		
		clearState();
		Random ran = new Random();
		long user1 = ran.nextLong();
		long userFriend = ran.nextLong();
		long item1 = ran.nextLong();
		long token1 = 1;
		long item2 = ran.nextLong();
		long token2 = 2;
		String key = MemCacheKeys.getDbpediaHasBeenSearched(props.getClient(), item1);
		try
		{

			createItemHits(item1,token1,"item1",20);
			createItemHits(item2,token2,"item2",20);

			createSimilarity(token1,token2);
			addAction(userFriend,item2);
			createLinkType("facebook",1);
			createFriendship(user1,userFriend);
			
			WebSimilaritySimplePeer p = new WebSimilaritySimplePeer(props.getClient(), webSimilaritySimpleStore,"facebook",false);
			List<SharingRecommendation> recs = p.getSharingRecommendationsForFriends(user1, item1);
			
			Assert.assertEquals(0, recs.size());
			
			
			Boolean searchedFromMemcache = (Boolean) MemCachePeer.get(key);
			Assert.assertNull(searchedFromMemcache);

		}
		finally
		{
			clearState();
			MemCachePeer.delete(key);
		}
	}
	
	@Test
	public void testSingleUserCached()
	{
		clearState();
		final long userId = 1;
		final long userFriend = 4;
		final long itemId = 2;
		final long exItemId = 3;
		final String exClientItemId = "item3";
		final String tokens = "tokens";
		createCachedResult(itemId, userFriend, exItemId, exClientItemId, tokens,1D);
		addAction(userFriend,exItemId);
		createLinkType("facebook",1);
		createFriendship(userId,userFriend);
		List<SharingRecommendation> recs = this.webSimilaritySimpleStore.getSharingRecommendationsForFriends(userId,itemId);
		Assert.assertEquals(1, recs.size());
		for(SharingRecommendation r : recs)
		{
			System.out.println("user:"+r.getUserId()+" item:"+r.getItemId());
			for(String reason : r.getReasons())
				System.out.println("reason:"+reason);
		}
	}
	
	
	@Test
	public void testSingleUserCachedFromPeer()
	{
		clearState();
		final long userId = 1;
		final long userFriend = 4;
		final long itemId = 2;
		final long exItemId = 3;
		final String exClientItemId = "item3";
		final String tokens = "tokens";
		String memcacheKey = MemCacheKeys.getSharingRecommendationsForItemSetKey("test", userId);
		try
		{
			createCachedResult(itemId, userFriend, exItemId, exClientItemId, tokens,1D);
			addAction(userFriend,exItemId);
			createLinkType("facebook",1);
			createFriendship(userId,userFriend);
		
			WebSimilaritySimplePeer p = new WebSimilaritySimplePeer(props.getClient(), webSimilaritySimpleStore,"facebook",false);
			List<SharingRecommendation> recs = p.getSharingRecommendationsForFriends(userId, itemId);
		
		
			Assert.assertEquals(1, recs.size());
			for(SharingRecommendation r : recs)
			{
				System.out.println("user:"+r.getUserId()+" item:"+r.getItemId());
				for(String reason : r.getReasons())
					System.out.println("reason:"+reason);
			}
		}
		finally
		{
			clearState();
			MemCachePeer.delete(memcacheKey);
		}
	}
	
	@Test 
	public void checkHasBeenSearchedFalse()
	{
		
		clearState();
		final long itemId = 1;
		String key = MemCacheKeys.getDbpediaHasBeenSearched(props.getClient(), itemId);
		try
		{
			MemCachePeer.delete(key);
		
			boolean searched = this.webSimilaritySimpleStore.hasBeenSearched(itemId);
		
			Assert.assertFalse(searched);
			
			Boolean searchedFromMemcache = (Boolean) MemCachePeer.get(key);
			Assert.assertNotNull(searchedFromMemcache);
			Assert.assertFalse(searchedFromMemcache);
		}
		finally
		{
			MemCachePeer.delete(key);
			clearState();
		}
	}
	
	@Test 
	public void checkHasBeenSearchedTrue()
	{
		clearState();
		final long itemId = 1;
		String key = MemCacheKeys.getDbpediaHasBeenSearched(props.getClient(), itemId);
		try
		{

			MemCachePeer.delete(key);
		
			createHasBeenSearched(itemId);
		
			boolean searched = this.webSimilaritySimpleStore.hasBeenSearched(itemId);
		
			Assert.assertTrue(searched);
		
			Boolean searchedFromMemcache = (Boolean) MemCachePeer.get(key);
			Assert.assertNotNull(searchedFromMemcache);
			Assert.assertTrue(searchedFromMemcache);
		}
		finally
		{
			MemCachePeer.delete(key);
			clearState();
		}
	}
	
	
	
	
}
