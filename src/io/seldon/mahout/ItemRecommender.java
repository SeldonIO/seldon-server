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

package io.seldon.mahout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.seldon.trust.impl.Recommendation;
import io.seldon.util.CollectionTools;
import org.apache.log4j.Logger;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.jdbc.AbstractJDBCComponent;
import org.apache.mahout.cf.taste.impl.model.jdbc.MySQLJDBCDataModel;
import org.apache.mahout.cf.taste.impl.recommender.AllSimilarItemsCandidateItemsStrategy;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.jdbc.MySQLJDBCItemSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.ItemBasedRecommender;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

import io.seldon.trust.impl.SearchResult;

/**
 * A recommender that uses the Item-Based recommender output from mahout. See 
 * <a href="http://wiki.rummble.com/mahout">mahout wiki page</a> for more details.
 * <p/>
 * Assume the existance of a table AbstractJDBCItemSimilarity.DEFAULT_SIMILARITY_TABLE in the database
 * with item similarities derived from mahout job
 * @author rummble
 *
 */
public class ItemRecommender {
	
	private static Logger logger = Logger.getLogger(ItemRecommender.class.getName());
	
	private ItemBasedRecommender recommender;
	private ItemSimilarity similarity;
	public  ItemRecommender(String datasource) throws TasteException
	{
		//DataModel dataModel = new MySQLBooleanPrefJDBCDataModel(datasource);
		
		//FIXME - needs changes to get to work with mahout 0.6
		
		DataModel dataModel = new MySQLJDBCDataModel(AbstractJDBCComponent.lookupDataSource(datasource),
                "opinions",
                "user_id",
                "item_id",
                "value",
                "time");
		// In Memory version no good for large datasets
		//similarity = new MySQLJDBCInMemoryItemSimilarity(datasource);
		similarity = new MySQLJDBCItemSimilarity(datasource);
		
		AllSimilarItemsCandidateItemsStrategy candidateStrategy =
		    new AllSimilarItemsCandidateItemsStrategy(similarity);
		recommender = new GenericItemBasedRecommender(dataModel,
		    similarity, candidateStrategy, candidateStrategy);
		    
	}
	
	public List<Recommendation> recommend(long userId,int type,int numRecommendations)
	{
		List<Recommendation> rumRs = new ArrayList<>();
		try 
		{
			List<RecommendedItem> recs = recommender.recommend(userId, numRecommendations);
			for(RecommendedItem r : recs)
			{
				rumRs.add(new Recommendation(r.getItemID(),type,new Double(r.getValue())));
			}
		} 
		catch (TasteException e) 
		{
			logger.error("Failed to get itemRecommendation for " + userId,e);
		}
		return rumRs;
	}
	
	//FIXME - finish
	// Define generic ItemFilter (look in mahout for example) so type can be used
	public List<SearchResult> findSimilar(long itemId,int type,int numResults)
	{
		try
		{
			long[] ids = similarity.allSimilarItemIDs(itemId);
			double[] similarities = similarity.itemSimilarities(itemId, ids);
			Map<Long,Double> simMap = new HashMap<>();
			for(int i=0;i<ids.length;i++)
				simMap.put(ids[i], similarities[i]);
			List<Long> ordered = CollectionTools.sortMapAndLimitToList(simMap, numResults, true);
			List<SearchResult> res = new ArrayList<>();
			for(Long key : ordered)
				res.add(new SearchResult(key,simMap.get(key)));
			return res;
		}
		catch (TasteException e)
		{
			logger.error("Failed to get item similarities for " + itemId,e);
			return null;
		}
	}
	
}
