/*
 * Seldon -- open source prediction engine
 * =======================================
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 **********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at       
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ********************************************************************************************** 
*/
package io.seldon.ar;

import io.seldon.api.caching.ActionHistoryCache;
import io.seldon.ar.AssocRuleManager.AssocRuleRecommendation;
import io.seldon.ar.AssocRuleManager.AssocRuleStore;
import io.seldon.clustering.recommender.ItemRecommendationAlgorithm;
import io.seldon.clustering.recommender.ItemRecommendationResultSet;
import io.seldon.clustering.recommender.ItemRecommendationResultSet.ItemRecommendationResult;
import io.seldon.clustering.recommender.RecommendationContext;
import io.seldon.general.Action;
import io.seldon.recommendation.RecommendationUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AssocRuleRecommender implements ItemRecommendationAlgorithm {
	private static Logger logger = Logger.getLogger(AssocRuleRecommender.class.getName());
	private static final String name = AssocRuleRecommender.class.getSimpleName();
	
	private static final int DEF_MAX_BASKET_SIZE = 3;
	
	private static final String USE_ACTION_TYPES_OPTION = "io.seldon.algorithm.assocrules.usetype";
	private static final String BASKET_MAX_SIZE_OPTION = "io.seldon.algorithm.assocrules.basket.maxsize";
	private static final String ADD_BASKET_ACTION_TYPE_OPTION = "io.seldon.algorithm.assocrules.add.basket.action.type";
	private static final String REMOVE_BASKET_ACTION_TYPE_OPTION = "io.seldon.algorithm.assocrules.remove.basket.action.type";
	
	final int[][] indices3 = {{0,1,2},{0,1},{0,2},{1,2},{0},{1},{2}};
	final int[][] indices2 = {{0,1},{0},{1}};
	final int[][] indices1 = {{0}};
	
	AssocRuleManager ruleManager;
	ActionHistoryCache actionCache;
	
	@Autowired
	public AssocRuleRecommender(AssocRuleManager ruleManager,ActionHistoryCache actionCache) 
	{
		this.ruleManager = ruleManager;
		this.actionCache = actionCache;
	}
	
	
	@Override
	public ItemRecommendationResultSet recommend(String client, Long user, Set<Integer> dimensions, int maxRecsCount,
	                                                 RecommendationContext ctxt, List<Long> recentItemInteractions)
	{
		long start = System.currentTimeMillis();
		 AssocRuleStore store = ruleManager.getClientStore(client, ctxt.getOptsHolder());
		 if (store == null)
		 {
			 if (logger.isDebugEnabled())
				 logger.debug("Failed to get assoc rule store for client "+client);
			 return new ItemRecommendationResultSet(Collections.<ItemRecommendationResult>emptyList(), name);
		 }
		 
		 // decide on basket
		 // 1. by using add/remove actions if available, or
		 // 2. all recent items interacted with
		 RecommendationContext.OptionsHolder optionsHolder = ctxt.getOptsHolder();
		 boolean useActionTypes = optionsHolder.getBooleanOption(USE_ACTION_TYPES_OPTION);
		 int maxBasketSize = optionsHolder.getIntegerOption(BASKET_MAX_SIZE_OPTION);
		 if (maxBasketSize == 0)
			 maxBasketSize = DEF_MAX_BASKET_SIZE;
		 List<Long> basket = null;
		 if (useActionTypes)
		 {
			 List<Action> actions = actionCache.getRecentFullActions(client, user, maxBasketSize*2);
			 Collections.reverse(actions);
			 basket = new ArrayList<Long>();
			 int addBasketType = optionsHolder.getIntegerOption(ADD_BASKET_ACTION_TYPE_OPTION);
			 int removeBaskeyType = optionsHolder.getIntegerOption(REMOVE_BASKET_ACTION_TYPE_OPTION);
			 // go through actions and create basket by handling add/remove basket actions
			 for(Action a : actions)
			 {
				 if (a.getType() != null)
				 {
					 if (a.getType() == addBasketType)
					 {
						 if (!basket.contains(a.getItemId()))
							 basket.add(a.getItemId());
					 }
					 else if (a.getType() == removeBaskeyType)
						 basket.remove(a.getItemId());
				 }
			 }
			 if (basket.size() > maxBasketSize)
				 basket = basket.subList(0, maxBasketSize);
		 }
		 else
		 {
			 if (recentItemInteractions.size() > maxBasketSize)
				 basket = recentItemInteractions.subList(0, maxBasketSize);
			 else
				 basket = recentItemInteractions;
		 }
		 if (basket.size() > 0)
		 {
		 Map<Long,Double> scores = new HashMap<Long,Double>();
		 int[][] indices;
		 switch(basket.size())
		 {
		 case 3:
			 indices = indices3;
			 break;
		 case 2:
			 indices = indices2;
			 break;
		 case 1:
			 indices = indices1;
		 default:
			 logger.warn("max basket size too big "+basket.size()+" can only handle up to "+DEF_MAX_BASKET_SIZE);
			 return new ItemRecommendationResultSet(Collections.<ItemRecommendationResult>emptyList(), name);
		 }
		 for (int i=0;i<indices.length;i++)
		 {
			 Set<Integer> matchedRules = null;
			 for(int j=0;j<indices[i].length;j++)
			 {
				 // find matching rules for subset of items as antecedent
				 Map<Integer,Set<Integer>> ruleLen = store.itemToRules.get(basket.get(indices[i][j]));
				 if (ruleLen != null)
				 {
					 Set<Integer> rules = ruleLen.get(indices[i].length);
					 if (rules != null)
					 {
						 if (matchedRules == null)
							 matchedRules = new HashSet<Integer>(rules);
						 else
							 matchedRules.retainAll(rules);
					 }
				 }
			 }
			 // if we have matched rules then add recommended item with score to map
			 if (matchedRules != null)
			 {
				 for(Integer rule : matchedRules)
				 {
					 AssocRuleRecommendation r = store.assocRules.get(rule);
					 Double score = scores.get(r.item);
					 if (score == null)
						 scores.put(r.item, r.score);
					 else
						 scores.put(r.item,score+r.score);
				 }
			 }
		 }
		 if (scores.size() > 0)
         {
        	 scores = RecommendationUtils.getTopK(scores, maxRecsCount);
        	 List<ItemRecommendationResultSet.ItemRecommendationResult> results = new ArrayList<>();
        	 for (Map.Entry<Long, Double> entry : scores.entrySet()) {
        		 results.add(new ItemRecommendationResultSet.ItemRecommendationResult(entry.getKey(), entry.getValue().floatValue()));
        	 }
        	 long end = System.currentTimeMillis();
        	 logger.info("took "+(end-start)+" to get "+scores.size()+" results");
        	 return new ItemRecommendationResultSet(results, name);
         }
         else
         {
        	 long end = System.currentTimeMillis();
        	 logger.info("took "+(end-start)+" to get 0 results");
        	 return new ItemRecommendationResultSet(Collections.<ItemRecommendationResult>emptyList(), name);
         }
		 }
		 else
			 return new ItemRecommendationResultSet(Collections.<ItemRecommendationResult>emptyList(), name);
		 
	}
	
	 @Override
	 public String name() {
		 return name;
	 }
}
