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
import io.seldon.clustering.recommender.*;
import io.seldon.clustering.recommender.jdo.JdoCountRecommenderUtils;
import io.seldon.clustering.tag.TagClusterRecommender;
import io.seldon.clustering.tag.jdo.JdoItemTagCache;
import io.seldon.clustering.tag.jdo.JdoTagClusterCountStore;
import io.seldon.cooc.CooccurrencePeer;
import io.seldon.cooc.CooccurrencePeerFactory;
import io.seldon.cooc.CooccurrenceRecommender;
import io.seldon.cooc.ICooccurrenceStore;
import io.seldon.cooc.jdo.JDBCCooccurrenceStore;
import io.seldon.db.jdo.ClientPersistable;
import io.seldon.facebook.FBUtils;
import io.seldon.general.ItemPeer;
import io.seldon.memcache.DogpileHandler;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import io.seldon.memcache.UpdateRetriever;

import io.seldon.recommendation.baseline.BaselineRecommender;
import io.seldon.recommendation.baseline.IBaselineRecommenderUtils;
import io.seldon.recommendation.baseline.jdo.SqlBaselineRecommenderUtils;
import io.seldon.semvec.*;
import io.seldon.similarity.dbpedia.WebSimilaritySimplePeer;
import io.seldon.similarity.dbpedia.WebSimilaritySimpleStore;
import io.seldon.similarity.dbpedia.jdo.SqlWebSimilaritySimplePeer;
import io.seldon.similarity.item.IItemSimilarityPeer;
import io.seldon.similarity.item.ItemSimilarityRecommender;
import io.seldon.similarity.item.JdoItemSimilarityPeer;
import io.seldon.similarity.tagcount.ITagCountPeer;
import io.seldon.similarity.tagcount.TagCountRecommender;
import io.seldon.similarity.tagcount.jdo.SqlTagCountPeer;
import io.seldon.similarity.vspace.TagSimilarityPeer;
import io.seldon.similarity.vspace.TagSimilarityPeer.VectorSpaceOptions;
import io.seldon.similarity.vspace.TagStore;
import io.seldon.similarity.vspace.jdo.TagStorePeer;
import io.seldon.storm.DRPCSettings;
import io.seldon.storm.DRPCSettingsFactory;
import io.seldon.storm.TrustDRPCRecommender;
import io.seldon.trust.impl.*;
import io.seldon.trust.impl.CFAlgorithm.CF_ITEM_COMPARATOR;
import io.seldon.trust.impl.CFAlgorithm.CF_PREDICTOR;
import io.seldon.trust.impl.CFAlgorithm.CF_RECOMMENDER;
import io.seldon.trust.impl.CFAlgorithm.CF_SORTER;
import io.seldon.trust.impl.TrustNetworkSupplier.CF_TYPE;
import io.seldon.util.CollectionTools;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
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

    // using @Resource as @Autowired doesn't work with maps --
    // see http://forum.spring.io/forum/spring-projects/container/57136-autowire-and-maps
    @Resource
    private Map<String, ItemRecommendationAlgorithm> recommendersByLabel;


    private boolean debugging = false;
    
    public RecommendationPeer() {
        this.debugging = logger.isDebugEnabled();
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

    private List<Long> getRecentItems(final int dimension, final int limit, final CFAlgorithm options)
    {
        final ClientPersistable cp = new ClientPersistable(options.getName());

    	String key = MemCacheKeys.getRecentItems(options.getName(), dimension, limit);
    	List<Long> items = (List<Long>) MemCachePeer.get(key);
		List<Long> newerItems = null;
		try {
			newerItems = DogpileHandler.get().retrieveUpdateIfRequired(key, items, new UpdateRetriever<List<Long>>() {
                @Override
                public List<Long> retrieve() throws Exception {

                    ItemPeer iPeer = Util.getItemPeer(cp.getPM());
                    ConsumerBean c = new ConsumerBean();
                    c.setShort_name(options.getName());
					List<Long> theitems = iPeer.getRecentItemIds(dimension, limit, c);

					logger.debug("Getting recent item with dimension "+dimension+" limit "+limit+" from db and found "+theitems.size());
					return theitems;
                }
            },MEMCACHE_RECENT_ITEMS_EXPIRE_SECS);
		} catch (Exception e) {
			logger.warn("Couldn't use dogpile handler : ", e);
		}
		if (newerItems!=null){
			MemCachePeer.put(key, newerItems,MEMCACHE_RECENT_ITEMS_EXPIRE_SECS);
			return newerItems;
		}else if (items !=null){
			logger.debug("Getting recent item with dimension "+dimension+" limit "+limit+" from memcache and found "+items.size());

		}
		return items;
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




    //TODO UPDATE AND USER ITEM TYPE INFO
	@Override
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
			recentActions = ah.getRecentActions(user, 100); // set to a large number - any reason to just remove this in future?
			numRecentActions = recentActions.size();
			if (debugging)
				logger.debug("RecentActions for user with client "+options.getName()+" internal user id "+user+" num." + numRecentActions);
		}
		else if (debugging)
			logger.debug("Can't get recent actions for anonymous user "+clientUserId);

		// get exclusion for user
		if (!TestingUtils.get().getTesting())
			exclusions = RecommendationUtils.getExclusions(false,options.getName(), clientUserId, currentItemId, lastRecListUUID, options,numRecentActions);
		else
			logger.warn("Not getting exclusions as we seem to be in test mode ");

		if (debugging)
			logger.debug("Exclusions is initially of size "+exclusions.size()+" for user "+user+" with client user id "+clientUserId+" for client "+options.getName());

		if (user != Constants.ANONYMOUS_USER && recentActions != null)
		{
			if (debugging)
				logger.debug("Adding recent actions of size "+recentActions.size()+" to exclusions for user "+user+" for client "+options.getName());
			exclusions.addAll(recentActions);
		}

		if (currentItemId != null)
		{
			exclusions.add(currentItemId); // add current page to exclusions
		}

		Map<Long,Double> recommenderScores = new HashMap<>();
		Map<Long,Double> recommendations = new HashMap<>();
		int numSuccessfulRecommenders = 0;
		boolean tryNext = true;
		Double worstScore = null;
		StringBuffer algorithmKey = new StringBuffer();
		for(CF_RECOMMENDER recommender_type : options.getRecommenders())
		{
			if (!tryNext)
				break;

			recommendations = new HashMap<>();
            ItemRecommendationAlgorithm alg;
            if(recommendersByLabel != null && (alg = recommendersByLabel.get(recommender_type.name()))!=null){
                logger.debug("Using recommender " + recommender_type.name());

				List<Long> recentItemInteractions;
				// add items from recent history if there are any and algorithm options says to use them
				if (recentActions != null && recentActions.size() > 0)
					recentItemInteractions = new ArrayList<>(recentActions);
				else
					recentItemInteractions = new ArrayList<>();

				// add current item id if not in recent actions
				if (currentItemId != null && !recentItemInteractions.contains(currentItemId))
					recentItemInteractions.add(0,currentItemId);

				RecommendationContext ctxt = RecommendationContext.buildContext(null, null,
						options.getName(), user,clientUserId,currentItemId, dimension, lastRecListUUID,10, numRecommendations,options );
				ItemRecommendationResultSet results = alg.recommend(options,options.getName(),user,dimension,
                        numRecommendations, ctxt,recentItemInteractions);

                for (ItemRecommendationResultSet.ItemRecommendationResult result : results.getResults()){
                    recommendations.put(result.item,result.score.doubleValue());
                }
            }
            switch (recommender_type)
			{
				case CLUSTER_COUNTS:
				case CLUSTER_COUNTS_DYNAMIC:
				case CLUSTER_COUNTS_SIGNIFICANT:
				{

					JdoCountRecommenderUtils cUtils = new JdoCountRecommenderUtils(options.getName());
					CountRecommender r = cUtils.getCountRecommender(options.getName());
					if (r != null)
					{
						r.setReferrer(referrer);
						boolean includeShortTermClusters = recommender_type == CF_RECOMMENDER.CLUSTER_COUNTS_DYNAMIC;
						long t1 = System.currentTimeMillis();
						r.setRecommenderType(recommender_type);
						recommendations = r.recommend(user, null, dimension, numRecommendations, exclusions, includeShortTermClusters, options.getLongTermWeight(), options.getShortTermWeight(),options.getDecayRateSecs(),options.getMinNumberItemsForValidClusterResult());
						long t2 = System.currentTimeMillis();
						if (debugging)
							logger.debug("Recommendation via cluster counts for user "+user+" took "+(t2-t1)+" and got back "+recommendations.size()+" recommednations");
					}
				}
				break;
				case TAG_CLUSTER_COUNTS:
				{
					if (currentItemId != null && options.isTagClusterCountsActive())
					{
						TagClusterRecommender tagClusterRecommender = new TagClusterRecommender(options.getName(),
								new JdoTagClusterCountStore(options.getName()),
								new JdoItemTagCache(options.getName()),
								options.getTagAttrId());
						Set<Long> userTagHistory = new HashSet<>();
						if (recentActions != null && options.getTagUserHistory() > 0)
						{
							if (recentActions.size() > options.getTagUserHistory())
								userTagHistory.addAll(recentActions.subList(0, options.getTagUserHistory()));
							else
								userTagHistory.addAll(recentActions);
						}
						recommendations = tagClusterRecommender.recommend(user, currentItemId, userTagHistory, dimension, numRecommendations, exclusions, options.getDecayRateSecs());
					}
				}
				break;
				case CLUSTER_COUNTS_GLOBAL:
				{
					JdoCountRecommenderUtils cUtils = new JdoCountRecommenderUtils(options.getName());
					CountRecommender r = cUtils.getCountRecommender(options.getName());
					if (r != null)
					{
						long t1 = System.currentTimeMillis();
						//RECENT ACTIONS

						recommendations = r.recommendGlobal(dimension, numRecommendations, exclusions,options.getDecayRateSecs(),null);
						long t2 = System.currentTimeMillis();
						if (debugging)
							logger.debug("Recommendation via cluster counts for user "+user+" took "+(t2-t1));
					}
				}
				break;
				case CLUSTER_COUNTS_ITEM_CATEGORY:
				{
					if (currentItemId != null)
					{
						Integer dimId = getDimensionForAttrName(currentItemId, options);
						if (dimId != null)
						{
							JdoCountRecommenderUtils cUtils = new JdoCountRecommenderUtils(options.getName());
							CountRecommender r = cUtils.getCountRecommender(options.getName());
							if (r != null)
							{

								long t1 = System.currentTimeMillis();
								recommendations = r.recommendGlobal(dimension, numRecommendations, exclusions,options.getDecayRateSecs(),dimId);
								long t2 = System.currentTimeMillis();
								if (debugging)
									logger.debug("Recommendation via cluster counts for dimension "+dimId+" for item  "+currentItemId+" for user "+user+" took "+(t2-t1));


							}
							else
								logger.warn("Can't get count recommender for "+options.getName());
						}
						else
							logger.info("Can't get dim for item "+currentItemId+" so can't run cluster counts for dimension algorithm ");
					}
					else
						logger.info("Can't cluster count for category for user "+user+" client user id "+clientUserId+" as no current item passed in");
				}
				break;
				case CLUSTER_COUNTS_FOR_ITEM:
				case CLUSTER_COUNTS_FOR_ITEM_SIGNIFICANT:
				{
					if (currentItemId != null)
					{
						JdoCountRecommenderUtils cUtils = new JdoCountRecommenderUtils(options.getName());
						CountRecommender r = cUtils.getCountRecommender(options.getName());
						if (r != null)
						{
							//change to significant version of cluster counts if needed
							if (recommender_type == CFAlgorithm.CF_RECOMMENDER.CLUSTER_COUNTS_FOR_ITEM_SIGNIFICANT)
								r.setRecommenderType(CFAlgorithm.CF_RECOMMENDER.CLUSTER_COUNTS_SIGNIFICANT);
							long t1 = System.currentTimeMillis();
							recommendations = r.recommendUsingItem(currentItemId, dimension, numRecommendations, exclusions, options.getDecayRateSecs(), options.getClusterAlgorithm(),options.getMinNumberItemsForValidClusterResult());
							long t2 = System.currentTimeMillis();
							if (debugging)
								logger.debug("Recommendation via cluster counts for item  "+currentItemId+" for user "+user+" took "+(t2-t1));


						}
					}
					else
						logger.info("Can't cluster count for item for user "+user+" client user id "+clientUserId+" as no current item passed in");

				}
				break;
				case USER_TAG_COUNT:
				{
					if (user != Constants.ANONYMOUS_USER)
					{
						ITagCountPeer tPeer = new SqlTagCountPeer(options.getName());
						TagCountRecommender r = new TagCountRecommender(options.getName(), tPeer);
						recommendations = r.recommend(user, dimension, exclusions, options.getUserTagAttrId(), 500, numRecommendations);
					}
				}
				break;
				case MOST_POPULAR_ITEM_CATEGORY:
				{
					if (currentItemId != null)
					{
						Integer dimId = getDimensionForAttrName(currentItemId, options);
						if (dimId != null)
						{
							IBaselineRecommenderUtils baselineUtils = new SqlBaselineRecommenderUtils(options.getName());
							BaselineRecommender br = new BaselineRecommender(options.getName(), baselineUtils);
							recommendations = br.mostPopularRecommendations(exclusions, dimId, numRecommendations);
						}
						else
							logger.info("Can't get dim for item "+currentItemId+" so can't run most popular for item category");
					}
				}
				break;
				case MOST_POPULAR:
				{
					IBaselineRecommenderUtils baselineUtils = new SqlBaselineRecommenderUtils(options.getName());
					BaselineRecommender br = new BaselineRecommender(options.getName(), baselineUtils);
					recommendations = br.mostPopularRecommendations(exclusions, dimension, numRecommendations);
				}
				break;
				case SOCIAL_PREDICT:
				{
					if (user != Constants.ANONYMOUS_USER)
					{
						WebSimilaritySimpleStore wstore = new SqlWebSimilaritySimplePeer(options.getName());
						WebSimilaritySimplePeer wpeer = new WebSimilaritySimplePeer(options.getName(), wstore);
						recommendations = wpeer.getRecommendedItems(user, dimension, getRecentItems(dimension,500, options), exclusions, MEMCACHE_RECENT_ITEMS_EXPIRE_SECS, numRecommendations);
						if (debugging)
							logger.debug("Social predict for user "+user+" returned "+recommendations.size());
					}
				}
				break;
				case ITEM_SIMILARITY_RECOMENDER:
				{
					if (user != Constants.ANONYMOUS_USER)
					{
						IItemSimilarityPeer peer = new JdoItemSimilarityPeer(options.getName());
						ItemSimilarityRecommender r = new ItemSimilarityRecommender(options.getName(), peer);
						recommendations = r.recommend(user, dimension, numRecommendations, exclusions);
						if (recommendations != null && debugging)
							logger.debug("Item similarity recommender returned "+recommendations.size()+" recommendations");
					}
				}
				break;
				case SIMILAR_ITEMS:
				{
					if (currentItemId != null)
					{
						IItemSimilarityPeer peer = new JdoItemSimilarityPeer(options.getName());
						ItemSimilarityRecommender r = new ItemSimilarityRecommender(options.getName(), peer);
						recommendations = r.recommendSimilarItems(currentItemId, dimension, numRecommendations, exclusions);
						if (recommendations != null && debugging)
							logger.debug("Similar items recommender returned "+recommendations.size()+" recommendations");
					}
				}
				break;
				case RECENT_SIMILAR_ITEMS:
				{
					List<Long> recentItemInteractions;
					// add items from recent history if there are any and algorithm options says to use them
					if (recentActions != null && recentActions.size() > 0)
						recentItemInteractions = new ArrayList<>(recentActions);
					else
						recentItemInteractions = new ArrayList<>();

					// add current item id if not in recent actions
					if (currentItemId != null && !recentItemInteractions.contains(currentItemId))
						recentItemInteractions.add(0,currentItemId);
					
					if (recentItemInteractions.size() > options.getNumRecentActions())
					{
						logger.debug("Decreasing item interactions from "+recentItemInteractions.size()+" to "+options.getNumRecentActions());
						recentItemInteractions = recentItemInteractions.subList(0, options.getNumRecentActions());
					}

					// if we have any items we can run the algorithm
					if (recentItemInteractions.size() > 0)
					{
						IItemSimilarityPeer peer = new JdoItemSimilarityPeer(options.getName());
						ItemSimilarityRecommender r = new ItemSimilarityRecommender(options.getName(), peer);
						recommendations = r.recommendSimilarItems(recentItemInteractions, dimension, numRecommendations, exclusions);
						if (recommendations != null && debugging)
							logger.info("Recent similar items recommender returned "+recommendations.size()+" recommendations");
					}
					else
						logger.info("failing RECENT_SIMILAR_ITEMS as no recent actions with getTxHistorySizeForSV:"+options.getTxHistorySizeForSV());
				}
				break;
				case SEMANTIC_VECTORS_RECENT_TAGS:
				case SEMANTIC_VECTORS_SORT:
				{
					if (currentItemId != null)
					{
						if (debugging)
							logger.debug("Getting semantic vectors peer for client ["+options.getName()+"] prefix ["+options.getSvPrefix()+"]");
						SemVectorsPeer sem = SemanticVectorsStore.get(options.getName(),options.getSvPrefix() != null ? options.getSvPrefix() : SemanticVectorsStore.PREFIX_FIND_SIMILAR);
						//FIXME - needs to ignore FacebookLikes etc.. needs this done in generic way

						List<Long> limitedRecentActions;
						if (options.getTxHistorySizeForSV() > 1 && recentActions != null && recentActions.size() > 0)
						{
							if(recentActions.size() > options.getTxHistorySizeForSV()) {
								limitedRecentActions = recentActions.subList(0, options.getTxHistorySizeForSV());
							}
							else {
								limitedRecentActions = recentActions;
							}
						}
						else
						{
							limitedRecentActions = new ArrayList<>();;
							limitedRecentActions.add(currentItemId);
						}

						List<Long> itemsToCompare = null;
						/*
						JdoCountRecommenderUtils cUtils = new JdoCountRecommenderUtils(options.getName());
						CountRecommender r = cUtils.getCountRecommender(options.getName());
						if (r != null)
						{
							long t1 = System.currentTimeMillis();
							//RECENT ACTIONS

							Map<Long,Double> globalClusterItems = r.recommendGlobal(dimension, numRecommendations*10, exclusions,options.getDecayRateSecs(),null);
							if (globalClusterItems.size() > 0)
							{
								logger.debug("Using global cluster counts for SV items of size "+globalClusterItems.size());
								itemsToCompare = new ArrayList<Long>(globalClusterItems.keySet());
							}
						}
						*/
						if (itemsToCompare == null && options.getRecentArticlesForSV() > 0)
						{
							itemsToCompare = getRecentItems(dimension,options.getRecentArticlesForSV(), options);
							if (debugging)
								logger.debug("Using recent textual items for SV of size  "+itemsToCompare.size());
						}
						if (recommender_type == CFAlgorithm.CF_RECOMMENDER.SEMANTIC_VECTORS_SORT)
						{
							if (options.getRecentArticlesForSV() > 0)
							{
								if (debugging)
									logger.debug("Calling semantic vectors to recommend via sort for client "+options.getName());
								recommendations = sem.recommendDocsUsingDocQuery(limitedRecentActions,itemsToCompare , new LongIdTransform(),exclusions,numRecommendations,options.isIgnorePerfectSVMatches());
							}
							else
							{
								if (debugging)
									logger.debug("Calling full SV recommendation on all items in SV db for client "+options.getName());
								recommendations = sem.recommendDocsUsingDocQuery(limitedRecentActions, new LongIdTransform(), numRecommendations, exclusions, null,options.isIgnorePerfectSVMatches());
							}
						}
						else if (recommender_type == CFAlgorithm.CF_RECOMMENDER.SEMANTIC_VECTORS_RECENT_TAGS)
						{
							Set<Long> inclusions = null;
							if (itemsToCompare != null)
								inclusions = new HashSet<>(itemsToCompare);
							recommendations = sem.recommendDocsFromItemTags(limitedRecentActions, options.getTagAttrId(), new JdoItemTagCache(options.getName()), numRecommendations, exclusions, inclusions, options.isIgnorePerfectSVMatches());
						}
					}
					else
						logger.info("Can't run semantic vectors for user "+user+" client user id "+clientUserId+" as no current item passed in");

				}
				break;
				case RECENT_ITEMS:
				case R_RECENT_ITEMS:
				{
					List<Long> recList = getRecentItems(dimension,numRecommendations+50, options);
					if (recommender_type == CFAlgorithm.CF_RECOMMENDER.R_RECENT_ITEMS)
					{
						Collections.shuffle(recList); //Randomize order
					}
					recommendations = new HashMap<>();
					if (recList.size() > 0)
					{
						double scoreIncr = 1.0/(double)recList.size();
						int count = 0;
						for(Long item : recList)
						{
							if (count >= numRecommendations)
								break;
							else if (!exclusions.contains(item))
								recommendations.put(item, 1.0 - (count++ * scoreIncr));
						}
						if (debugging)
							logger.debug("Recent items algorithm returned "+recommendations.size()+" items");
					}
					else
					{
						logger.warn("No items returned for recent items of dimension "+dimension+" for "+options.getName());
					}
				}
				break;
			}
			switch (options.getRecommenderStrategy())
			{
			case ORDERED:
				if (recommendations != null && recommendations.size() > 0)
				{
					if (debugging)
						logger.debug("Successful call to " + recommender_type.name() + " strategy is "+options.getRecommenderStrategy().name() + " returning recommendations");
					if (numSuccessfulRecommenders > 1)
						algorithmKey.append(".").append(recommender_type.name()); //dot also useful for graphite stat based on this name
					else
						algorithmKey.append(recommender_type.name());
					if (worstScore == null)
					{
						recommenderScores = recommendations;
						worstScore = 1.0D;
						for(Map.Entry<Long, Double> e : recommenderScores.entrySet())
						{
							if (worstScore > e.getValue())
								worstScore = e.getValue();
						}
						if (recommenderScores.size() >= numRecommendations)
							tryNext = false;
						else if (debugging)
							logger.debug("Not enough recommendations for "+user+"+ will try next");
					}
					else
					{
						List<Long> sortedRecs = CollectionTools.sortMapAndLimitToList(recommendations, recommendations.size(), true);
						for(Long id : sortedRecs)
						{
							if (!recommenderScores.containsKey(id))
							{
								recommenderScores.put(id, recommendations.get(id));
								if (recommenderScores.size() >= numRecommendations)
								{
									tryNext = false;
									break;
								}
							}
						}
						if (tryNext && debugging)
							logger.debug("Not enough recommendations for "+user+"+ will try next");
					}
				}
				else if (debugging)
					logger.debug("Unsuccessful call to " + recommender_type.name() + " will try next");
				break;
			case FIRST_SUCCESSFUL:
				if (recommendations != null && recommendations.size() > 0)
				{
					if (debugging)
						logger.debug("Successful call to " + recommender_type.name() + " strategy is "+options.getRecommenderStrategy().name() + " returning recommendations");
					algorithmKey.append(recommender_type.name());
					recommenderScores = recommendations;
					tryNext = false;
				}
				else if (debugging)
					logger.debug("Unsuccessful call to " + recommender_type.name() + " will try next");
				break;
			case RANK_SUM:
			{
				if (recommendations != null && recommendations.size() > 0)
				{
					numSuccessfulRecommenders++;
					if (debugging)
						logger.debug("Successful call to " + recommender_type.name() + " strategy is "+options.getRecommenderStrategy().name() + " returning recommendations");
					if (numSuccessfulRecommenders > 1)
						algorithmKey.append(".").append(recommender_type.name()); //dot also useful for graphite stat based on this name
					else
						algorithmKey.append(recommender_type.name());
					double weight = 1;
					if (options.getRecommendationWeightMap() != null && options.getRecommendationWeightMap().containsKey(recommender_type))
					{
						weight = options.getRecommendationWeightMap().get(recommender_type);
						if (debugging)
							logger.debug("Using Ranking weight "+weight+" for algoritm "+recommender_type.name());
					}
					else if (debugging)
						logger.debug("No weight got for recommender algorithm "+recommender_type.name()+" so using weight of 1.0 for ranking");
					List<Long> ranked = CollectionTools.sortMapAndLimitToList(recommendations, numRecommendations, true);
					double rankScore = numRecommendations;
					for(Long id : ranked)
					{
						if (rankScore <= 0)
						{
							logger.error("Rankscore less than zero. Got "+ranked.size()+" recommendations when asked for "+numRecommendations);
							break;
						}
						Double current = recommenderScores.get(id);
						if (current != null)
							recommenderScores.put(id, current + (rankScore*weight));
						else
							recommenderScores.put(id, rankScore*weight);
						rankScore--;
					}
					if (numSuccessfulRecommenders >= options.getMaxRecommendersToUse())
						tryNext = false;
				}
				else if (debugging)
					logger.debug("Unsuccessful call to " + recommender_type.name() + " will try next");
			}
			break;
			case WEIGHTED:
			{
				if (recommendations != null && recommendations.size() > 0)
				{
					numSuccessfulRecommenders++;
					if (debugging)
						logger.debug("Successful call to " + recommender_type.name() + " strategy is "+options.getRecommenderStrategy().name() + " returning recommendations");
					if (numSuccessfulRecommenders > 1)
						algorithmKey.append(".").append(recommender_type.name()); //dot also useful for graphite stat based on this name
					else
						algorithmKey.append(recommender_type.name());
					double weight = 1;
					if (options.getRecommendationWeightMap() != null && options.getRecommendationWeightMap().containsKey(recommender_type))
					{
						weight = options.getRecommendationWeightMap().get(recommender_type);
						if (debugging)
							logger.debug("Using weight "+weight+" for algoritm "+recommender_type.name());
					}
					else if (debugging)
						logger.debug("No weight got recommender algorithm "+recommender_type.name()+" so using weight of 1.0");
					for(Map.Entry<Long, Double> score : recommendations.entrySet())
					{
						Double current = recommenderScores.get(score.getKey());
						if (current != null)
							recommenderScores.put(score.getKey(), current + (score.getValue()*weight));
						else
							recommenderScores.put(score.getKey(), (score.getValue()*weight));
					}
					if (numSuccessfulRecommenders >= options.getMaxRecommendersToUse())
						tryNext = false;
				}
				else if (debugging)
					logger.debug("Unsuccessful call to " + recommender_type.name() + " will try next");

			}
				break;
			}
		}

		if (recommenderScores.size() > 0)
		{
			switch(options.getPostprocessing())
			{
			case REORDER_BY_POPULARITY:
			{
				IBaselineRecommenderUtils baselineUtils = new SqlBaselineRecommenderUtils(options.getName());
				BaselineRecommender br = new BaselineRecommender(options.getName(), baselineUtils);
				recommenderScores = br.reorderRecommendationsByPopularity(recommenderScores);
			}
			break;
			default:
				break;
			}
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
				SemVectorsPeer sem = SemanticVectorsStore.get(options.getName(),SemanticVectorsStore.PREFIX_FIND_SIMILAR,type);
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
				RecommendationNetwork trustNet = new SimpleTrustNetworkProvider(options).getTrustNetwork(content,Trust.TYPE_GENERAL,false,CF_TYPE.ITEM);
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
				case WEB_SIMILARITY:
				{
					boolean isFacebookActive = FBUtils.isFacebookActive(new ConsumerBean(options.getName()), userId);
					if (isFacebookActive)
					{
						if (debugging)
							logger.debug("User "+userId+" is facebook user");
						WebSimilaritySimpleStore wstore = new SqlWebSimilaritySimplePeer(options.getName());
						WebSimilaritySimplePeer wpeer = new WebSimilaritySimplePeer(options.getName(), wstore);
						res = wpeer.sort(userId, items);
					}
					else if (debugging)
						logger.debug("User "+userId+" is not a facebook user");
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
				case WEB_SIMILARITY:
				{
					/*
					 * Presently hardwired to only be active if cluster counts was successfil. Facebook users
					 * will always have a cluster (at present). This should be made more customizable.
					 */
					if (successfulMethods.contains(CF_SORTER.CLUSTER_COUNTS))
					{
						boolean isFacebookActive = FBUtils.isFacebookActive(new ConsumerBean(options.getName()), userId);
						if (isFacebookActive)
						{
							logger.info("User "+userId+" is a facebook user. Running web similarity post processing");
							WebSimilaritySimpleStore wstore = new SqlWebSimilaritySimplePeer(options.getName());
							WebSimilaritySimplePeer wpeer = new WebSimilaritySimplePeer(options.getName(), wstore);
							//weight is how much weight to give sorted items ..so 0.1 would be low and thus post processing
							// would be given more weight. For web simialrity where matches are rare, this would mean those
							// matches that are found are given high weight
							res = wpeer.merge(userId, res, 0.1f);
						}
					}
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


	private  List<SharingRecommendation> sharingRecommendationFromDb(long userId,Long itemId,String linkType,List<String> tags,int limit,CFAlgorithm options)
	{
		WebSimilaritySimpleStore wstore = new SqlWebSimilaritySimplePeer(options.getName());
		WebSimilaritySimplePeer wpeer = new WebSimilaritySimplePeer(options.getName(), wstore,linkType,options.isAllowFullSocialPredictSearch());
		List<SharingRecommendation> res;
		if (tags != null && tags.size() > 0)
		{
			logger.info("As keywords is not null will call get sharing recommendation using tags for user "+userId);
			res = wpeer.getSharingRecommendationsForFriends(userId, tags);
		}
		else
		{
			logger.info("Calling item based sharing recommendations for user "+userId);
			res = wpeer.getSharingRecommendationsForFriends(userId, itemId);
		}
		if (res != null)
			logger.info("Social prediction result for "+options.getName()+" user "+userId+" of size "+res.size());
		if (res != null && res.size() > limit)
			return res.subList(0, limit);
		else
			return res;
	}

	@Override
	public List<RecommendedUserBean> sharingRecommendation(String userFbId,long userId,Long itemId,String linkType,
			List<String> tags,int limit,CFAlgorithm options) {
		List<RecommendedUserBean> recUserList = new ArrayList<>();
		if (Util.getMgmKeywordConf().isDBClient(options.getName())) {
			List<SharingRecommendation> sharingRecs = sharingRecommendationFromDb(userId, itemId, linkType, tags, limit, options);
			ConsumerBean c = new ConsumerBean();
			c.setShort_name(options.getName());
			for (SharingRecommendation s : sharingRecs) {
				try {
					recUserList.add(new RecommendedUserBean(c, s));
				} catch (Exception e) {
					logger.error("Not able to retrieve FB ID for internal user ID" + s.getUserId(), e);
				}
			}
		}
		return recUserList;
	}

	@Override
	public List<SearchResult> getSimilarUsers(long userId, int limit, int filterType, CFAlgorithm options) {
		WebSimilaritySimpleStore wstore = new SqlWebSimilaritySimplePeer(options.getName());
		WebSimilaritySimplePeer wpeer = new WebSimilaritySimplePeer(options.getName(), wstore);
		return wpeer.getSimilarUsers(userId, limit, options.getSimilarUsersMetric(), filterType);
	}

}
