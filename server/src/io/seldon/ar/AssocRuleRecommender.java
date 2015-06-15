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

import io.seldon.ar.AssocRuleManager.AssocRuleRecommendation;
import io.seldon.ar.AssocRuleManager.AssocRuleStore;
import io.seldon.clustering.recommender.ItemRecommendationAlgorithm;
import io.seldon.clustering.recommender.ItemRecommendationResultSet;
import io.seldon.clustering.recommender.ItemRecommendationResultSet.ItemRecommendationResult;
import io.seldon.clustering.recommender.RecommendationContext;

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
	
	AssocRuleManager ruleManager;
	
	@Autowired
	public AssocRuleRecommender(AssocRuleManager ruleManager) 
	{
		this.ruleManager = ruleManager;
	}
	
	public static <T extends Comparable<? super T>> List<List<T>> binPowSet(
			List<T> A){
		List<List<T>> ans= new ArrayList<List<T>>();
		int ansSize = (int)Math.pow(2, A.size());
		for(int i= 0;i< ansSize;++i){
			String bin= Integer.toBinaryString(i); //convert to binary
			while(bin.length() < A.size()) bin = "0" + bin; //pad with 0's
			ArrayList<T> thisComb = new ArrayList<T>(); //place to put one combination
			for(int j= 0;j< A.size();++j){
				if(bin.charAt(j) == '1')thisComb.add(A.get(j));
			}
			Collections.sort(thisComb); //sort it for easy checking
			ans.add(thisComb); //put this set in the answer list
		}
		return ans;
	}
	
	@Override
	public ItemRecommendationResultSet recommend(String client, Long user, Set<Integer> dimensions, int maxRecsCount,
	                                                 RecommendationContext ctxt, List<Long> recentItemInteractions) 
	{
		 AssocRuleStore store = ruleManager.getStore(client);	
		 if (store == null)
		 {
			 logger.debug("Failed to get tag store for client "+client);
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
			 
		 }
		 else
		 {
			 if (recentItemInteractions.size() > maxBasketSize)
				 basket = recentItemInteractions.subList(0, maxBasketSize);
			 else
				 basket = recentItemInteractions;
		 }
		 
		 List<List<Long>> itemsets = binPowSet(basket);
		 Map<Long,Double> scores = new HashMap<Long,Double>();
		 for(List<Long> itemset : itemsets)
		 {
			 if (itemset.size() > 0)
			 {
				 // find matching rules for subset of items as antecedent
				 Set<Integer> matchedRules = null;
				 for(Long item : itemset)
				 {
					 Map<Integer,Set<Integer>> ruleLen = store.itemToRules.get(item);
					 if (ruleLen != null)
					 {
						 Set<Integer> rules = ruleLen.get(itemset.size());
						 if (matchedRules == null)
							 matchedRules = new HashSet<Integer>(rules);
						 else
							 matchedRules.retainAll(rules);
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
		 }
		 List<ItemRecommendationResultSet.ItemRecommendationResult> results = new ArrayList<>();
         for (Map.Entry<Long, Double> entry : scores.entrySet()) {
             results.add(new ItemRecommendationResultSet.ItemRecommendationResult(entry.getKey(), entry.getValue().floatValue()));
         }
         return new ItemRecommendationResultSet(results, name);
		 
	}
	
	public static void main(String[] args)
	{
		ArrayList<Integer> a = new ArrayList<>();
		a.add(1); a.add(2); a.add(3);
		for (List<Integer> l : AssocRuleRecommender.binPowSet(a))
		{
			System.out.println(l.toString());
		}
	}
	
	 @Override
	 public String name() {
		 return name;
	 }
}
