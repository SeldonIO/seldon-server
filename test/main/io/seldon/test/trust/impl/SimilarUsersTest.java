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
import java.util.Date;
import java.util.List;
import java.util.Random;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import junit.framework.Assert;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.similarity.dbpedia.WebSimilaritySimplePeer;
import io.seldon.similarity.dbpedia.WebSimilaritySimpleStore;
import io.seldon.similarity.dbpedia.jdo.SqlWebSimilaritySimplePeer;
import io.seldon.test.GenericPropertyHolder;
import io.seldon.test.peer.BasePeerTest;
import io.seldon.trust.impl.SearchResult;

public class SimilarUsersTest extends BasePeerTest {

	@Autowired
	GenericPropertyHolder props;
	
	@Autowired PersistenceManager pm;

	
	private void clearState()
	{
    	final PersistenceManager pm = JDOFactory.getPersistenceManager(props.getClient());
		try 
		{
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "delete from user_similarity");
					query.execute();
					query = pm.newQuery( "javax.jdo.query.SQL", "delete from interaction");
                    query.execute();
			    	
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to clear state");
		}
		
	}
	
	private void addSimilarUser(final long u1,final long u2,final int type, final double score) throws DatabaseException
	{
		
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	Query query = pm.newQuery( "javax.jdo.query.SQL", "insert into user_similarity (u1,u2,type,score) values (?,?,?,?)");
			    	List<Object> args = new ArrayList<Object>();
			    	args.add(u1);
			    	args.add(u2);
			    	args.add(type);
			    	args.add(score);
					query.executeWithArray(args.toArray());
					query.closeAll();
			    }});
		
	}
	
	@Test
	public void lowLevelTest() throws DatabaseException
	{
		try
		{
			Random r = new Random();
			long userId = r.nextLong() ;
			userId = userId > 0 ? userId : userId * -1; 
			addSimilarUser(userId, 2L, 1, 0.8);
			addSimilarUser(userId, 3L, 1, 0.7);
			addSimilarUser(userId, 4L, 1, 0.7);
			addSimilarUser(userId, 3L, 2, 0.9); // to ensure this is not in result
			addInteraction(userId, 4L);// should not be in results
			WebSimilaritySimpleStore wstore = new SqlWebSimilaritySimplePeer(props.getClient());
			WebSimilaritySimplePeer wpeer = new WebSimilaritySimplePeer(props.getClient(), wstore);
			List<SearchResult> res = wpeer.getSimilarUsers(userId, 2, 1, 1);
			Assert.assertEquals(2, res.size());
			Assert.assertEquals(2L, res.get(0).getId());
			Assert.assertEquals(0.8D, res.get(0).getScore());
			Assert.assertEquals(3L, res.get(1).getId());
			Assert.assertEquals(0.7D, res.get(1).getScore());
			res = wpeer.getSimilarUsers(userId, 10, 1, 1);
			Assert.assertEquals(2, res.size());
		}
		finally
		{
			clearState();
		}
	}

    private void addInteraction(final long userId, final long l) throws DatabaseException {
         TransactionPeer.runTransaction(new Transaction(pm) { 
                public void process()
                { 
                    Query query = pm.newQuery( "javax.jdo.query.SQL", "insert into interaction (u1,u2,type,sub_type, date) values (?,?,?,?,?)");
                    List<Object> args = new ArrayList<Object>();
                    args.add(userId);
                    args.add(l);
                    args.add(1);
                    args.add(1);
                    args.add(new Date());
                    query.executeWithArray(args.toArray());
                    query.closeAll();
                }});
        
    }
}
