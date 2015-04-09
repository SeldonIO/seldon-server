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

package io.seldon.trust.impl.jdo;

import io.seldon.api.APIException;
import io.seldon.api.Constants;
import io.seldon.api.caching.ActionHistoryCache;
import io.seldon.api.state.ClientAlgorithmStore;
import io.seldon.api.state.options.DefaultOptions;
import io.seldon.clustering.recommender.CountRecommender;
import io.seldon.clustering.recommender.ItemRecommendationResultSet;
import io.seldon.clustering.recommender.RecommendationContext;
import io.seldon.clustering.recommender.jdo.JdoCountRecommenderUtils;
import io.seldon.db.jdo.ClientPersistable;
import io.seldon.recommendation.AlgorithmStrategy;
import io.seldon.recommendation.ClientStrategy;
import io.seldon.recommendation.combiner.AlgorithmResultsCombiner;
import io.seldon.trust.impl.CFAlgorithm;
import io.seldon.trust.impl.CFAlgorithm.CF_SORTER;
import io.seldon.trust.impl.Recommendation;
import io.seldon.trust.impl.RecommendationNetwork;
import io.seldon.trust.impl.RecommendationResult;
import io.seldon.trust.impl.SortResult;
import io.seldon.util.CollectionTools;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Core recommendation algorithm selection. Calls particular classes and methods to carry out the various API methods
 * @author rummble
 *
 */
@Component
public class RecommendationPeer {
;


	private static Logger logger = Logger.getLogger(RecommendationPeer.class.getName());
	
	public static final int MEMCACHE_TRUSTNET_EXPIRE_SECS = 60 * 60 * 3;
	public static final int MEMCACHE_RECENT_ITEMS_EXPIRE_SECS = 60 * 15;
	private final DefaultOptions defaultOptions;

	private double HIGH_RATING_THRESHOLD = 0.5;
	private double INITIAL_REVIEW_SET_RATIO = 2F;

	private ClientAlgorithmStore algStore;

	@Autowired
    JdoCountRecommenderUtils cUtils;
	
	@Autowired
	ActionHistoryCache actionCache;

    private boolean debugging = false;

	@Autowired
    public RecommendationPeer(ClientAlgorithmStore algStore, DefaultOptions defaultOptions) {
        this.algStore = algStore;
		this.defaultOptions = defaultOptions;
    }


	public RecommendationResult getRecommendations(long user, String client, String clientUserId, Integer type,
                                                   int dimension, int numRecommendationsAsked,
                                                   String lastRecListUUID,
                                                   Long currentItemId, String referrer, String recTag, List<String> algorithmOverride) {
        ClientStrategy strategy;
        if (algorithmOverride != null && !algorithmOverride.isEmpty()) {
            logger.debug("Overriding algorithms from JS");
            strategy  = algStore.retrieveStrategy(client, algorithmOverride);
        } else {
            strategy = algStore.retrieveStrategy(client);
        }

        if (strategy == null) {
            throw new APIException(APIException.NOT_VALID_STRATEGY);
        }

		//Set base values - will be used for anonymous users
		int numRecommendations = numRecommendationsAsked;
		int numRecentActions = 0;

		Double diversityLevel = strategy.getDiversityLevel(clientUserId, recTag);
		if (diversityLevel > 1.0f)
		{
			int numRecommendationsDiverse = new Long(Math.round(numRecommendationsAsked * diversityLevel)).intValue();
			if (debugging)
				logger.debug("Updated num recommendations as for client "+client+" diversity is "+diversityLevel+" was "+numRecommendationsAsked+" will now be "+numRecommendationsDiverse);
			numRecommendations = numRecommendationsDiverse;
		}
		else
			numRecommendations = numRecommendationsAsked;

		List<Long> recentActions = null;
		if (user != Constants.ANONYMOUS_USER) // only can get recent actions for non anonymous user
		{
			//TODO - fix limit
			recentActions = actionCache.getRecentActions(client,user, 100);
			numRecentActions = recentActions.size();
			if (debugging)
				logger.debug("RecentActions for user with client "+client+" internal user id "+user+" num." + numRecentActions);
		}
		else if (debugging)
			logger.debug("Can't get recent actions for anonymous user "+clientUserId);


		Map<Long,Double> recommenderScores = new HashMap<>();
		List<String> algsUsed = new ArrayList<>();
		List<RecResultContext> resultSets = new ArrayList<>();
		AlgorithmResultsCombiner combiner = strategy.getAlgorithmResultsCombiner(clientUserId, recTag);
		for(AlgorithmStrategy algStr : strategy.getAlgorithms(clientUserId, recTag))
		{
			logger.debug("Using recommender class " + algStr.name);

			List<Long> recentItemInteractions;
			// add items from recent history if there are any and algorithm options says to use them
			if (recentActions != null && recentActions.size() > 0)
				recentItemInteractions = new ArrayList<>(recentActions);
			else
				recentItemInteractions = new ArrayList<>();

			// add current item id if not in recent actions
			if (currentItemId != null && !recentItemInteractions.contains(currentItemId))
				recentItemInteractions.add(currentItemId);
			RecommendationContext ctxt = RecommendationContext.buildContext(client,
					algStr,user,clientUserId,currentItemId, dimension, lastRecListUUID, numRecommendations,defaultOptions);
			ItemRecommendationResultSet results = algStr.algorithm.recommend(client, user, dimension,
					numRecommendations, ctxt, recentItemInteractions);

		    resultSets.add(new RecResultContext(results, results.getRecommenderName()));
			if(combiner.isEnoughResults(numRecommendations, resultSets))
				break;
		}
        RecResultContext combinedResults = combiner.combine(numRecommendations, resultSets);
        logger.debug("After combining, we have "+combinedResults.resultSet.getResults().size()+
                " results with alg key "+combinedResults.algKey + " : " + StringUtils.join(combinedResults.resultSet.getResults(),':'));
		for (ItemRecommendationResultSet.ItemRecommendationResult result : combinedResults.resultSet.getResults()) {
			recommenderScores.put(result.item, result.score.doubleValue());
		}
		if (recommenderScores.size() > 0)
		{
//			switch(options.getPostprocessing())
//			{
//				case REORDER_BY_POPULARITY:
//				{
//					IBaselineRecommenderUtils baselineUtils = new SqlBaselineRecommenderUtils(options.getName());
//					BaselineRecommender br = new BaselineRecommender(options.getName(), baselineUtils);
//					recommenderScores = br.reorderRecommendationsByPopularity(recommenderScores);
//				}
//				break;
//				default:
//					break;
//			}
			List<Long> recommendationsFinal = CollectionTools.sortMapAndLimitToList(recommenderScores, numRecommendations, true);
			logger.debug("recommendationsFinal size was " +recommendationsFinal.size());
            return createFinalRecResult(numRecommendationsAsked, client, clientUserId, dimension,
                    lastRecListUUID, recommendationsFinal, combinedResults.algKey,
                    currentItemId, numRecentActions, diversityLevel,strategy,recTag);
		}
		else
		{
			logger.warn("Returning no recommendations for user with client id "+clientUserId);
			return createFinalRecResult(numRecommendationsAsked,client, clientUserId,
					dimension, lastRecListUUID, new ArrayList<Long>(),"",currentItemId,
					numRecentActions, diversityLevel,strategy, recTag);
		}
	}


    private RecommendationResult createFinalRecResult(int numRecommendationsAsked, String client, String clientUserId,
													  int dimension,String currentRecUUID,List<Long> recs,String algKey,
													  Long currentItemId,int numRecentActions, Double diversityLevel,
                                                      ClientStrategy strat, String recTag)
    {
    	List<Long> recsFinal;
    	if (diversityLevel > 1.0)
    		recsFinal = RecommendationUtils.getDiverseRecommendations(numRecommendationsAsked, recs,client,clientUserId,dimension);
    	else
    		recsFinal = recs;
        logger.debug("recs final size "+ recsFinal.size());
    	String uuid=RecommendationUtils.cacheRecommendationsAndCreateNewUUID(client, clientUserId, dimension,
                currentRecUUID, recsFinal, algKey,currentItemId,numRecentActions, strat, recTag);
    	List<Recommendation> recBeans = new ArrayList<>();
    	for(Long itemId : recsFinal)
    		recBeans.add(new Recommendation(itemId, 0, 0.0));
    	return new RecommendationResult(recBeans, uuid);
    }







	public RecommendationNetwork getNetwork(long user, int type, CFAlgorithm cfAlgorithm) {
		return new SimpleTrustNetworkProvider(cfAlgorithm).getTrustNetwork(user, type);
	}


	

	public SortResult sort(Long userId,List<Long> items, CFAlgorithm options, List<Long> recentActions) {
        ClientPersistable cp = new ClientPersistable(options.getName());

        if (debugging)
			logger.debug("Calling sort for user "+userId);
		if(items !=null && items.size()>0)
		{
			if (debugging)
				logger.debug("Trying sort for user "+userId);
			Map<Long,Integer> results = new HashMap<>();
			List<CF_SORTER> successfulMethods = new ArrayList<>();
			int successfulPrevAlg = 0;
			for(CF_SORTER sortMethod : options.getSorters())
			{
				if (debugging)
					logger.debug("Trying "+sortMethod.name());
				List<Long> res = null;
				switch (sortMethod)
				{
				case NOOP:
					res = items;
					break;


				
				case CLUSTER_COUNTS:
				case CLUSTER_COUNTS_DYNAMIC:
				{
					CountRecommender r = cUtils.getCountRecommender(options.getName());
					if (r != null)
					{
						boolean includeShortTermClusters = sortMethod == CF_SORTER.CLUSTER_COUNTS_DYNAMIC;
						long t1 = System.currentTimeMillis();
						res = r.sort(userId, items, null,includeShortTermClusters,options.getLongTermWeight(),options.getShortTermWeight()); // group hardwired to null
						long t2 = System.currentTimeMillis();
						if (debugging)
							logger.debug("Sorting for user "+userId+" took "+(t2-t1));
					}
				}
					break;
				case DEMOGRAPHICS:
					res = null;
					break;
				}

				boolean success = res != null && res.size() > 0;
				if (success)
				{
					successfulMethods.add(sortMethod);
					if (debugging)
						logger.debug("Successful sort for user "+userId+" for sort "+sortMethod.name());
					switch(options.getSorterStrategy())
					{
					case FIRST_SUCCESSFUL:
					case RANK_SUM:
					case WEIGHTED: // presently does same as RANK_SUM
					{
						// add results to Map
						int pos = 0;
						// update the counts for articles not seen in this algorithm set
						int currentResultsSize = results.size();
						Set<Long> missedItems = new HashSet<>(results.keySet());
						for(Long itemId : res)
						{
							missedItems.remove(itemId);
							pos++;
							Integer count = results.get(itemId);
							int countNew;
							if (count != null)
								countNew = count + pos;
							else
								countNew = pos + (currentResultsSize * successfulPrevAlg);
							results.put(itemId, countNew);
						}
						for(Long item : missedItems)
							results.put(item, results.get(item)+res.size());
						successfulPrevAlg++;
					}
					break;
					case ADD_MISSING:
					{
						if (debugging)
							logger.debug("Adding missing items returned "+sortMethod.name()+" to global list: "+CollectionTools.join(res, ","));
						successfulPrevAlg++;
						Set<Long> currentItems = new HashSet<>(results.keySet());
						int pos = currentItems.size();
						for(Long itemId : res)
						{
							if (!currentItems.contains(itemId))
							{
								pos++;
								results.put(itemId, pos);
							}
						}
					}
					break;
					}
				}
				else if (debugging)
					logger.debug("unsuccessful sort for user "+userId+" for sort "+sortMethod.name());

				if (success && options.getSorterStrategy() == CFAlgorithm.CF_STRATEGY.FIRST_SUCCESSFUL)
					break;
				else
				{
					// just continue
				}
			}
			List<Long> res = CollectionTools.sortMapAndLimitToList(results, results.size(), false);

			if (res == null)
				res = new ArrayList<>();
			return new SortResult(res,successfulMethods,options.getSorterStrategy());
		}
		else
		{
			logger.warn("No items to sort for user "+userId);
			return new SortResult(new ArrayList<Long>(),new ArrayList<CF_SORTER>(),options.getSorterStrategy());
		}
	}


    public static class RecResultContext {
        public static final RecResultContext EMPTY = new RecResultContext(new ItemRecommendationResultSet("UNKNOWN"), "UNKNOWN");
        public final ItemRecommendationResultSet resultSet;
        public final String algKey;

        public RecResultContext(ItemRecommendationResultSet resultSet, String algKey) {
            this.resultSet = resultSet;
            this.algKey = algKey;
        }
    }


}
