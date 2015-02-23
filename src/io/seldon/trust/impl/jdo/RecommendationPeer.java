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

import backtype.storm.utils.DRPCClient;
import io.seldon.api.Constants;
import io.seldon.api.TestingUtils;
import io.seldon.api.Util;
import io.seldon.api.caching.ActionHistoryCache;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.DimensionBean;
import io.seldon.api.resource.RecommendedUserBean;
import io.seldon.api.resource.service.ItemService;
import io.seldon.api.state.ClientAlgorithmStore;
import io.seldon.clustering.recommender.*;
import io.seldon.clustering.recommender.jdo.JdoCountRecommenderUtils;
import io.seldon.cooc.CooccurrencePeer;
import io.seldon.cooc.CooccurrencePeerFactory;
import io.seldon.cooc.CooccurrenceRecommender;
import io.seldon.cooc.ICooccurrenceStore;
import io.seldon.cooc.jdo.JDBCCooccurrenceStore;
import io.seldon.db.jdo.ClientPersistable;
import io.seldon.facebook.FBUtils;
import io.seldon.general.ItemPeer;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;

import io.seldon.recommendation.AlgorithmStrategy;
import io.seldon.recommendation.ClientStrategy;
import io.seldon.recommendation.baseline.BaselineRecommender;
import io.seldon.recommendation.baseline.IBaselineRecommenderUtils;
import io.seldon.recommendation.baseline.jdo.SqlBaselineRecommenderUtils;
import io.seldon.recommendation.combiner.AlgorithmResultsCombiner;
import io.seldon.semvec.*;
import io.seldon.similarity.item.IItemSimilarityPeer;
import io.seldon.similarity.item.ItemSimilarityRecommender;
import io.seldon.similarity.item.JdoItemSimilarityPeer;
import io.seldon.similarity.vspace.TagSimilarityPeer;
import io.seldon.similarity.vspace.TagSimilarityPeer.VectorSpaceOptions;
import io.seldon.similarity.vspace.TagStore;
import io.seldon.similarity.vspace.jdo.TagStorePeer;
import io.seldon.storm.DRPCSettings;
import io.seldon.storm.DRPCSettingsFactory;
import io.seldon.storm.TrustDRPCRecommender;
import io.seldon.trust.impl.*;
import io.seldon.trust.impl.CFAlgorithm.CF_ITEM_COMPARATOR;
import io.seldon.trust.impl.CFAlgorithm.CF_RECOMMENDER;
import io.seldon.trust.impl.CFAlgorithm.CF_SORTER;
import io.seldon.trust.impl.TrustNetworkSupplier.CF_TYPE;
import io.seldon.util.CollectionTools;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Core recommendation algorithm selection. Calls particular classes and methods to carry out the various API methods
 * @author rummble
 *
 */
@Component
public class RecommendationPeer implements  RummbleLabsAPI, RummbleLabsAnalysis {

	private static Logger logger = Logger.getLogger(RecommendationPeer.class.getName());
	
	public static final int MEMCACHE_TRUSTNET_EXPIRE_SECS = 60 * 60 * 3;
	public static final int MEMCACHE_RECENT_ITEMS_EXPIRE_SECS = 60 * 15;

	private double HIGH_RATING_THRESHOLD = 0.5;
	private double INITIAL_REVIEW_SET_RATIO = 2F;

	private ClientAlgorithmStore algStore;


    private boolean debugging = false;

	@Autowired
    public RecommendationPeer(ClientAlgorithmStore algStore) {
        this.algStore = algStore;
    }


	public RecommendationResult getRecommendations(long user, String clientUserId,Integer type,
												   int dimension, int numRecommendationsAsked,
												   CFAlgorithm options,String lastRecListUUID,
												   Long currentItemId, String referrer) {

		String abTestingKey = options.getAbTestingKey() == null ? "<default>" : options.getAbTestingKey();
		if (debugging)
			logger.debug("Get recommendations for " + user + " AB Testing Key:"+abTestingKey+" algorithm: " + options.toString());

		//Set base values - will be used for anonymous users
		int numRecommendations = numRecommendationsAsked;
		Set<Long> exclusions = new HashSet<>();
		int numRecentActions = 0;


		if (options.getRecommendationDiversity() > 1.0f)
		{
			int numRecommendationsDiverse = Math.round(numRecommendationsAsked * options.getRecommendationDiversity());
			if (debugging)
				logger.debug("Updated num recommendations as for client "+options.getName()+" diversity is "+options.getRecommendationDiversity()+" was "+numRecommendationsAsked+" will now be "+numRecommendationsDiverse);
			numRecommendations = numRecommendationsDiverse;
		}
		else
			numRecommendations = numRecommendationsAsked;

		List<Long> recentActions = null;
		if (user != Constants.ANONYMOUS_USER) // only can get recent actions for non anonymous user
		{
			// get recent actions for user
			ActionHistoryCache ah = new ActionHistoryCache(options.getName());
			recentActions = ah.getRecentActions(user, options.getNumRecentActions() > 0 ? options.getNumRecentActions() : numRecommendations);
			numRecentActions = recentActions.size();
			if (debugging)
				logger.debug("RecentActions for user with client "+options.getName()+" internal user id "+user+" num." + numRecentActions);
		}
		else if (debugging)
			logger.debug("Can't get recent actions for anonymous user "+clientUserId);


		Map<Long,Double> recommenderScores = new HashMap<>();
		Map<Long,Double> recommendations = new HashMap<>();
		boolean tryNext = true;
		StringBuffer algorithmKey = new StringBuffer();
		ClientStrategy strategy = algStore.retrieveStrategy(options.getName());
		List<ItemRecommendationResultSet> resultSets = new ArrayList<>();
		AlgorithmResultsCombiner combiner = strategy.getAlgorithmResultsCombiner(clientUserId);
		for(AlgorithmStrategy algStr : strategy.getAlgorithms(clientUserId))
		{
			if (!tryNext)
				break;


			logger.debug("Using recommender " + algStr.algorithm.name());

			List<Long> recentItemInteractions;
			// add items from recent history if there are any and algorithm options says to use them
			if (recentActions != null && recentActions.size() > 0)
				recentItemInteractions = new ArrayList<>(recentActions);
			else
				recentItemInteractions = new ArrayList<>();

			// add current item id if not in recent actions
			if (currentItemId != null && !recentItemInteractions.contains(currentItemId))
				recentItemInteractions.add(currentItemId);
			RecommendationContext ctxt = RecommendationContext.buildContext(algStr.includers, algStr.filters,
					options.getName(), user,clientUserId,currentItemId, dimension, lastRecListUUID,algStr.itemsPerIncluder, numRecommendations,options );
			ItemRecommendationResultSet results = algStr.algorithm.recommend(options, options.getName(), user, dimension,
					numRecommendations, ctxt, recentItemInteractions);

			for (ItemRecommendationResultSet.ItemRecommendationResult result : results.getResults()) {
				recommendations.put(result.item, result.score.doubleValue());
			}
			if(combiner.isEnoughResults(numRecommendations, resultSets))
				break;
		}
		ItemRecommendationResultSet combinedResults = combiner.combine(numRecommendations, resultSets);
		for (ItemRecommendationResultSet.ItemRecommendationResult result : combinedResults.getResults()) {
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
			return createFinalRecResult(numRecommendationsAsked,clientUserId, dimension,
					lastRecListUUID, recommendationsFinal,algorithmKey.toString(),
					currentItemId,numRecentActions, options);
		}
		else
		{
			logger.warn("Returning no recommendations for user with client id "+clientUserId);
			return createFinalRecResult(numRecommendationsAsked,clientUserId,
					dimension, lastRecListUUID, new ArrayList<Long>(),"",currentItemId,
					numRecentActions, options);
		}
	}

	private Integer getDimensionForAttrName(long itemId, CFAlgorithm options)
    {
        ClientPersistable cp = new ClientPersistable(options.getName());
        String attrName = options.getCategoryDim();
        String key = MemCacheKeys.getDimensionForAttrName(options.getName(), itemId, attrName);
    	Integer dimId = (Integer) MemCachePeer.get(key);
    	if (dimId == null)
    	{
    		ItemPeer iPeer = Util.getItemPeer(cp.getPM());
    		dimId = iPeer.getDimensionForAttrName(itemId, attrName);
    		if (dimId != null)
    		{
    			MemCachePeer.put(key, dimId, Constants.CACHING_TIME);
    			logger.info("Get dim for item "+itemId+" for attrName "+attrName+" and got "+dimId);
    		}
    		else
    			logger.info("Got null for dim for item "+itemId+" for attrName "+attrName);
    	}
    	return dimId;
    }



    private RecommendationResult createFinalRecResult(int numRecommendationsAsked,String clientUserId,int dimension,String currentRecUUID,List<Long> recs,String algKey,Long currentItemId,int numRecentActions, CFAlgorithm options)
    {
    	List<Long> recsFinal;
    	if (options.getRecommendationDiversity() > 1.0)
    		recsFinal = RecommendationUtils.getDiverseRecommendations(numRecommendationsAsked, recs,options.getName(),clientUserId,dimension);
    	else
    		recsFinal = recs;

    	String uuid=RecommendationUtils.cacheRecommendationsAndCreateNewUUID(options.getName(), clientUserId, dimension, currentRecUUID, recsFinal, options,algKey,currentItemId,numRecentActions);
    	List<Recommendation> recBeans = new ArrayList<>();
    	for(Long itemId : recsFinal)
    		recBeans.add(new Recommendation(itemId, 0, 0.0));
    	return new RecommendationResult(recBeans, uuid);
    }







	@Override
	public RecommendationNetwork getNetwork(long user, int type, CFAlgorithm cfAlgorithm) {
		return new SimpleTrustNetworkProvider(cfAlgorithm).getTrustNetwork(user, type);
	}

	@Override
	public RummbleLabsAnalysis getAnalysis(CFAlgorithm options) {
		//trustNetProvider = new TrustNetworkProvider(getPM(),client);
		return this;
	}

	@Override
	public List<SearchResult> findSimilar(long content, int type, int numResults, CFAlgorithm options) {
		List<SearchResult> res = new ArrayList<>();
		for(CF_ITEM_COMPARATOR comparator : options.getItemComparators())
		{
			switch(comparator)
			{
			case SEMANTIC_VECTORS:
			{
				SemVectorsPeer sem = SemanticVectorsStore.get(options.getName(), SemanticVectorsStore.PREFIX_FIND_SIMILAR, type);
				ArrayList<SemVectorResult<Long>> results = new ArrayList<>();
				sem.searchDocsUsingDocQuery(content, results, new DocumentIdTransform(),numResults);

				int count = 0;
				for(SemVectorResult<Long> r : results)
				{
					if (!r.getResult().equals(content))
						res.add(new SearchResult(r.getResult(),r.getScore()));
					if (++count>=numResults)
						break;
				}
			}
			break;
			case TRUST_ITEM:
			{
				RecommendationNetwork trustNet = new SimpleTrustNetworkProvider(options).getTrustNetwork(content, Trust.TYPE_GENERAL, false, CF_TYPE.ITEM);
				List<Long> people = trustNet.getSimilarityNeighbourhood(numResults);
				int count = 0;
				for(Long t : people)
				{
					res.add(new SearchResult(t,trustNet.getSimilarity(t)));
					if (++count>=numResults)
						break;
				}
			}
			break;
			}
			switch (options.getItemComparatorStrategy())
			{
			case FIRST_SUCCESSFUL:
				if (res != null && res.size()>0)
				{
					logger.info("Succesful call to " + comparator.name() + " strategy is "+options.getItemComparatorStrategy().name() + " returning recommendations");
					return res;
				}
				else
					logger.info("Unsuccessful call to " + comparator.name() + " will try next");
				break;
			case WEIGHTED:
				//FIXME
				break;
			}

		}

		return res;
	}

	@Override
	public List<SearchResult> searchContent(String query, Long user, DimensionBean d,int numResults,
				CFAlgorithm options) {
		ClientPersistable cp = new ClientPersistable(options.getName());

        logger.info("Getting sem vectors peer for user " + user + " query="+query+" dimension item_type:"+(d != null ? d.getItemType() : "null")+" dimension attr:"+(d != null ? d.getAttr() : "null")+" dimension value:"+(d != null ? d.getVal() : "null"));
		SemVectorsPeer sem = SemanticVectorsStore.get(options.getName(),SemanticVectorsStore.PREFIX_KEYWORD_SEARCH,d != null ? d.getItemType() : null);
		logger.info("Got sem vectors peer for user " + user + " query="+query);
		List<SearchResult> res = new ArrayList<>();
		if (sem != null)
		{
			ArrayList<SemVectorResult<Long>> results = new ArrayList<>();
			logger.info("semvec search start " + user + " query="+query);
			long t = System.currentTimeMillis();
			sem.searchDocsUsingTermQuery(query, results, new DocumentIdTransform(),new StringTransform(),numResults);
			long dur = System.currentTimeMillis() - t;
			logger.info("semvec search end " + user + " time:"+dur);

			int count = 0;
			ItemPeer iPeer = null;
			if (d != null && d.getAttr() != null && d.getVal() != null)
				iPeer = Util.getItemPeer(cp.getPM());
			for(SemVectorResult<Long> r : results)
			{
				if (iPeer != null) // filter results
				{
					Collection<Integer> itemDimensions =  ItemService.getItemDimensions(new ConsumerBean(options.getName()), r.getResult());
					if (itemDimensions == null || !itemDimensions.contains(d.getDimId()))
						continue;
				}
				res.add(new SearchResult(r.getResult(),r.getScore()));
				if (++count>=numResults)
					break;
			}


			Collections.sort(res);
		}
		return res;
	}



	@Override
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
				case COOCCURRENCE:
					{
						CooccurrencePeer p = CooccurrencePeerFactory.get(options.getName());
						ICooccurrenceStore store = new JDBCCooccurrenceStore(options.getName());
						CooccurrenceRecommender recommender = new CooccurrenceRecommender(options.getName(), p, store);
						res = recommender.sort(userId, recentActions, items);
					}
					break;
				case STORM_TRUST:
					{
						DRPCSettings drpcSettings = DRPCSettingsFactory.get(options.getName());
						if (drpcSettings != null)
						{
							DRPCClient drpcClient = new DRPCClient(drpcSettings.getHost(), drpcSettings.getPort(), drpcSettings.getTimeout());
							TrustDRPCRecommender r = new TrustDRPCRecommender(drpcSettings, drpcClient);
							res = r.sort(userId, -1, items);
						}
					}
					break;
				case MOST_POPULAR_WEIGHTED_MEMBASED:
					MemoryWeightedClusterCountMap map = GlobalWeightedMostPopular.get(options.getName());
					if (map != null)
					{
						GlobalWeightedMostPopularUtils countUtils = new GlobalWeightedMostPopularUtils(map,TestingUtils.getTime());
						res = countUtils.sort(items);
					}
					break;
				case MOST_POPULAR_MEMBASED:
					res= ItemsRankingManager.getInstance().getItemsByHits(options.getName(), items);
					break;
				case MOST_RECENT_MEMBASED:
					res = ItemsRankingManager.getInstance().getItemsByDate(options.getName(), items);
					break;
				case MOST_POP_RECENT_MEMBASED:
					res = ItemsRankingManager.getInstance().getCombinedList(options.getName(), items, null);
					break;
				case TAG_SIMILARITY:
					TagStore tstore = new TagStorePeer(cp.getPM());
        			VectorSpaceOptions dOptions = new VectorSpaceOptions(VectorSpaceOptions.TF_TYPE.LOGTF,VectorSpaceOptions.DF_TYPE.NONE,VectorSpaceOptions.NORM_TYPE.COSINE);
        			VectorSpaceOptions qOptions = new VectorSpaceOptions(VectorSpaceOptions.TF_TYPE.LOGTF,VectorSpaceOptions.DF_TYPE.NONE,VectorSpaceOptions.NORM_TYPE.COSINE);
        			TagSimilarityPeer tsp = new TagSimilarityPeer(dOptions,qOptions,tstore);
        			res = tsp.sortBySimilarity(userId, items);
        			break;
				case SEMANTIC_VECTORS:
				{
					if (debugging)
						logger.debug("Getting semantic vectors peer for client ["+options.getName()+"]");
					SemVectorsPeer sem = SemanticVectorsStore.get(options.getName(),SemanticVectorsStore.PREFIX_FIND_SIMILAR);
					if (debugging)
						logger.debug("Getting recent actions for user "+userId+" SV History size:"+options.getTxHistorySizeForSV()+" with min "+options.getMinNumTxsForSV());
					//FIXME - needs to ignore FacebookLikes etc.. needs this done in generic way
					if (recentActions.size() >= options.getMinNumTxsForSV())
					{
						//Limiting the list size
						List<Long> limitedRecentActions;
						if(recentActions.size() > options.getTxHistorySizeForSV()) {
							limitedRecentActions = recentActions.subList(0, options.getTxHistorySizeForSV());
						}
						else {
							limitedRecentActions = recentActions;
						}
						if (debugging)
							logger.debug("Calling semantic vectors to sort");
						res = sem.sortDocsUsingDocQuery(limitedRecentActions, items, new DocumentIdTransform());
						if (debugging)
						{
							if (res != null && res.size() > 0)
								logger.debug("Got result of size "+res.size());
							else
								logger.debug("Got no results from semantic vectors");
						}
					}
					else if (debugging)
						logger.debug("Not enough tx for user "+userId+" they had "+recentActions.size()+" min is "+options.getMinNumTxsForSV());
				}
					break;
				case CLUSTER_COUNTS:
				case CLUSTER_COUNTS_DYNAMIC:
				{
					JdoCountRecommenderUtils cUtils = new JdoCountRecommenderUtils(options.getName());
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

			//Post-processing - if specified
			if (res != null)
			{
				if (debugging)
					logger.debug("Calling post processor " + options.getPostprocessing().name());
				switch(options.getPostprocessing())
				{
				case NONE:
					break;
				case ADD_MISSING:
					for(Long item : items)
						if (!res.contains(item))
							res.add(item);
					break;
				case HITS:
					res = ItemsRankingManager.getInstance().getCombinedListWithHits(options.getName(), items, res);
					break;
				case TIME_HITS:
					res = ItemsRankingManager.getInstance().getCombinedList(options.getName(), items, res);
					break;
				case HITS_WEIGHTED:
					MemoryWeightedClusterCountMap map = GlobalWeightedMostPopular.get(options.getName());
					if (map != null)
					{
						GlobalWeightedMostPopularUtils countUtils = new GlobalWeightedMostPopularUtils(map,TestingUtils.getTime());
						res = countUtils.merge(res,0.5f);
					}
					break;
				}
			}
			if (res == null)
				res = new ArrayList<>();
			return new SortResult(res,successfulMethods,options.getSorterStrategy(),options.getPostprocessing());
		}
		else
		{
			logger.warn("No items to sort for user "+userId);
			return new SortResult(new ArrayList<Long>(),new ArrayList<CF_SORTER>(),options.getSorterStrategy(),options.getPostprocessing());
		}
	}



	@Override
	public List<RecommendedUserBean> sharingRecommendation(String userFbId,long userId,Long itemId,String linkType,
			List<String> tags,int limit,CFAlgorithm options) {
		List<RecommendedUserBean> recUserList = new ArrayList<>();
//		if (Util.getMgmKeywordConf().isDBClient(options.getName())) {
//			List<SharingRecommendation> sharingRecs = sharingRecommendationFromDb(userId, itemId, linkType, tags, limit, options);
//			ConsumerBean c = new ConsumerBean();
//			c.setShort_name(options.getName());
//			for (SharingRecommendation s : sharingRecs) {
//				try {
//					recUserList.add(new RecommendedUserBean(c, s));
//				} catch (Exception e) {
//					logger.error("Not able to retrieve FB ID for internal user ID" + s.getUserId(), e);
//				}
//			}
//		}
		return recUserList;
	}

	@Override
	public List<SearchResult> getSimilarUsers(long userId, int limit, int filterType, CFAlgorithm options) {
//		WebSimilaritySimpleStore wstore = new SqlWebSimilaritySimplePeer(options.getName());
//		WebSimilaritySimplePeer wpeer = new WebSimilaritySimplePeer(options.getName(), wstore);
//		return wpeer.getSimilarUsers(userId, limit, options.getSimilarUsersMetric(), filterType);
	return null;
	}

}
