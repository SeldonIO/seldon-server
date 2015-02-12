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

package io.seldon.similarity.dbpedia.jdo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.jdo.Query;

import io.seldon.db.jdo.ClientPersistable;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.nlp.StopWordPeer;
import io.seldon.nlp.TransliteratorPeer;
import org.apache.log4j.Logger;

import io.seldon.api.resource.ItemBean;
import io.seldon.db.jdo.DatabaseException;
import io.seldon.similarity.dbpedia.DBpediaItemSearch;
import io.seldon.similarity.dbpedia.WebSearchResultsPeer;

public class SqlWebSearchResultsPeer extends ClientPersistable implements WebSearchResultsPeer {
	
	private static Logger logger = Logger.getLogger(SqlWebSearchResultsPeer.class.getName());
	
	public SqlWebSearchResultsPeer(String client) {
		super(client);
	}

	@Override
	public Integer getHits(long itemId) {
		Query query = getPM().newQuery( "javax.jdo.query.SQL", "select hits from dbpedia_searches where item_id=?");
		query.setUnique(true);
		query.setResultClass(Integer.class);
		return (Integer) query.execute(itemId);
	}
	
	@Override
	public List<DBpediaItemSearch> getHitsInRange(int hits, long minItemId,
			int oom) {
		double logHits = Math.log(hits);
		double lower = Math.max(0,logHits-oom);
		double higher = logHits+oom;
		String sql = "select 0 as userId,i.item_id as itemId,i.name as name,db.hits as hits from dbpedia_searches db,items i where db.item_id=i.item_id and db.item_id>"+minItemId+" and db.hits>0 and db.loghits>="+lower+" and db.loghits<="+higher+" order by i.item_id asc";
		Query query = getPM().newQuery( "javax.jdo.query.SQL", sql);
		query.setResultClass(DBpediaItemSearch.class);
		return (List<DBpediaItemSearch>) query.execute();
	}

	@Override
	public List<DBpediaItemSearch> getHitsInRange(int hits, int oom) {
		double logHits = Math.log(hits);
		double lower = Math.max(0,logHits-oom);
		double higher = logHits+oom;
		String sql = "select 0 as userId,i.item_id as itemId,i.name as name,db.hits as hits from dbpedia_searches db,items i where db.item_id=i.item_id and db.hits>0 and db.loghits>="+lower+" and db.loghits<="+higher+" order by i.item_id asc";
		Query query = getPM().newQuery( "javax.jdo.query.SQL", sql);
		query.setResultClass(DBpediaItemSearch.class);
		return (List<DBpediaItemSearch>) query.execute();
	}
	
	
	@Override
	public List<DBpediaItemSearch> getHitsInRange(long userId, String linkType, int hits, int oom) {
		double logHits = Math.log(hits);
		double lower = Math.max(0,logHits-oom);
		double higher = logHits+oom;
		String sql = "select u.user_id as userId,i.item_id as itemId,i.name as name,db.hits as hits from dbpedia_searches db,users u, links l, link_type lt, actions a, action_type at,items i where at.semantic and at.link_type=lt.type_id and at.type_id=a.type and i.item_id=a.item_id and lt.name=\""+linkType+"\" and l.type=lt.type_id and l.u1="+userId+" and l.u2=u.user_id and a.user_id=u.user_id and db.item_id=i.item_id and db.hits>0 and db.loghits>="+lower+" and db.loghits<="+higher+" group by u.user_id,i.item_id";
		Query query = getPM().newQuery( "javax.jdo.query.SQL", sql);
		query.setResultClass(DBpediaItemSearch.class);
		return (List<DBpediaItemSearch>) query.execute();
	}
	
	@Override
	public List<DBpediaItemSearch> getHitsForUser(long userId,String linkType) {
		String sql = "select a.user_id as userId,i.item_id as itemId,i.name as name,db.hits as hits from dbpedia_searches db,link_type lt, actions a, action_type at,items i where at.semantic and at.link_type=lt.type_id and at.type_id=a.type and i.item_id=a.item_id and lt.name=\""+linkType+"\" and a.user_id="+userId+" and db.item_id=i.item_id and db.hits>0 group by i.item_id";
		Query query = getPM().newQuery( "javax.jdo.query.SQL", sql);
		query.setResultClass(DBpediaItemSearch.class);
		return (List<DBpediaItemSearch>) query.execute();
	}
	
	private static String tokeniseImpl(String s)
	{
		s = s.replaceAll("[-:;\\,/&]"," ").trim();
		s = TransliteratorPeer.getPunctuationTransLiterator().transliterate(s);
		s = StopWordPeer.get().removeStopWords(s);
		return s;
	}
	
	/**
	 * Tokenise a string into a list of tokens. Does:
	 * <ul>
	 * <li> replace basic separators by space
	 * <li> transliterate the string to remove all other punctuation
	 * <li> remove stop words
	 * </ul>
	 * @param s
	 * @return
	 */
	public static List<String> tokenise(String s)
	{
		if (s != null)
		{
			s = tokeniseImpl(s);
			s = s.toLowerCase().trim();
			if (!"".equals(s))
			{
				String[] parts = s.split("\\s+");
				return Arrays.asList(parts);
			}
			else 
				return new ArrayList<String>();
		}
		else
			return new ArrayList<String>();
	}
	
	/**
	 * Integration point to update the dbpedia_search. Clumps inserts together to make only 1 db call
	 * @param items
	 */
	@Override
	public void storeNewItems(Collection<ItemBean> items)
	{
		if (items != null && items.size() > 0)
		try
		{
			StringBuffer insertSQL = new StringBuffer("insert ignore into dbpedia_searches values ");
			boolean notFirst = false;
			
			for(ItemBean item : items)
			{
				if (item.getName() != null && !"".equals(item.getName()))
				{
					List<String> tokens = tokenise(item.getName());
					if (tokens.size() > 0)
					{
						if (notFirst)
							insertSQL.append(", ");
						else
							notFirst=true;
						int hits = 0;
						String loghits = hits == 0 ? "null" : ""+Math.log(hits);
						insertSQL.append("(0,").append(item.getId()).append(",").append(hits).append(",").append(loghits).append(")");
					}
				}
			}
			final String sql = insertSQL.toString();
			try {
				TransactionPeer.runTransaction(new Transaction(getPM()) {
					public void process() {
						Query query = getPM().newQuery("javax.jdo.query.SQL", sql);
						query.execute();
					}
				});
			} catch (DatabaseException e) 
			{
				logger.error("Failed to Add dbpedia searches: " + sql);
			}
		
		} catch (Exception e) {

			logger.error("Failed to Add dbpedia searches due to dbpedia error: ",e);
		}
	
	}

	

	
}
