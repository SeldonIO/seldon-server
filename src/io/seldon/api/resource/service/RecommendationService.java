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

package io.seldon.api.resource.service;

import io.seldon.api.APIException;
import io.seldon.api.Constants;
import io.seldon.api.TestingUtils;
import io.seldon.api.Util;
import io.seldon.api.caching.ActionHistoryCache;
import io.seldon.api.resource.*;
import io.seldon.api.service.ABTestingServer;
import io.seldon.api.service.DynamicParameterServer;
import io.seldon.general.RecommendationStorage;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import io.seldon.trust.impl.CFAlgorithm;
import io.seldon.trust.impl.CFAlgorithm.CF_SORTER;
import io.seldon.trust.impl.Recommendation;
import io.seldon.trust.impl.RecommendationResult;
import io.seldon.trust.impl.RummbleLabsAPI;
import io.seldon.trust.impl.SearchResult;
import io.seldon.trust.impl.SortResult;
import io.seldon.trust.impl.jdo.LastRecommendationBean;
import io.seldon.trust.impl.jdo.RecommendationPeer;

import java.util.*;

import javax.jdo.JDODataStoreException;

import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author claudio
 */

@Service
public class RecommendationService {
    private static Logger logger = Logger.getLogger(RecommendationService.class.getName());

    private static final String RECOMMENDATION_UUID_ATTR = "recommendationUuid";
    
    public static final String KEYWORD_PAR = "keywords";

    @Autowired
    private RecommendationPeer recommender;
    @Autowired
    private RecommendationStorage recommendationStorage;
    @Autowired
    private ItemService itemService;


//    public RecommendationBean getRecommendation(ConsumerBean c, String userId, Integer type, int dimensionId, String itemId, long pos,List<String> algorithms) throws APIException {
//        RecommendationBean bean;
//        if(pos == Constants.POSITION_NOT_DEFINED) {
//            ListBean recs = getRecommendations(c,userId,type,dimensionId,Constants.DEFAULT_BIGRESULT_LIMIT,false,algorithms);
//            bean = findRecommendationBean(itemId,recs);
//            if(bean == null) {
//                bean = new RecommendationBean(itemId,Constants.POSITION_NOT_DEFINED,null);
//            }
//        }
//        else { bean = new RecommendationBean(itemId,pos,null); }
//        //Set source of the recommendation
//        //bean.setSrcUsers(users);
//        return bean;
//    }

    //FIX handle FULL
    //TODO use TYPE
    public ListBean getRecommendations(ConsumerBean c,String userId,List<String> keywords, int limit, boolean full,int dimension,List<String> algorithms) throws APIException {
        logger.info("Get RecommendationsBean for " + userId + " with keywords:" + keywords);
        //ALGORITHM
        CFAlgorithm cfAlgorithm = getAlgorithmOptions(c, userId, algorithms,null);
        ListBean bean = (ListBean) MemCachePeer.get(MemCacheKeys.getRecommendationsBeanKey(c.getShort_name(),cfAlgorithm, userId,keywords.toString(),full,dimension));
        bean = Util.getLimitedBean(bean, limit);
        if(bean == null) {
            bean = new ListBean();

            long pos = 1;
            String word = "";
            for (String k : keywords) {
                word += k + " ";
            }
            List<SearchResult> res = recommender.searchContent(word, UserService.getInternalUserId(c, userId), ItemService.getDimension(c, dimension), limit, cfAlgorithm);
            for (SearchResult r : res) {
                String itemId = null;
                try {
                    itemId = ItemService.getClientItemId(c, r.getId());
                }
                //item not found
                catch (APIException a) {
                }
                ;
                if (itemId != null) {
                    bean.addBean(new RecommendationBean(itemId, pos++, null));
                }
            }
            if (Constants.CACHING && cfAlgorithm.getRecommendationCachingTimeSecs() > 0)
                MemCachePeer.put(MemCacheKeys.getRecommendationsBeanKey(c.getShort_name(), cfAlgorithm, userId, keywords.toString(), full, dimension), bean, cfAlgorithm.getRecommendationCachingTimeSecs());
        }
        logger.info("Return RecommendationsBean for " + userId + " with keywords:" + keywords);
        return bean;
    }

//    //FIX handle FULL
//    public ListBean getRecommendations(ConsumerBean c,String userId,Integer type, int dimensionId, int limit, boolean full,List<String> algorithms) throws APIException {
//        logger.info("Get RecommendationsBean for " + userId + " with dimension:" + dimensionId);
//        //ALGORITHM
//        CFAlgorithm cfAlgorithm = getAlgorithmOptions(c, userId, algorithms,null);
//        ListBean bean = (ListBean) MemCachePeer.get(MemCacheKeys.getRecommendationsBeanKey(c.getShort_name(),cfAlgorithm,userId,type,dimensionId,full));
//        bean = Util.getLimitedBean(bean, limit);
//        if(bean == null) {
//            bean = new ListBean();
//
//            Long internalUserId;
//            try {
//                internalUserId = UserService.getInternalUserId(c, userId);
//            } catch (APIException e) {
//                internalUserId = Constants.ANONYMOUS_USER;
//            }
//
//            RecommendationResult recResult = recommender.getRecommendations(internalUserId, userId, type, dimensionId, limit, cfAlgorithm,null,null, null);//FIXME
//            List<Recommendation> recs = recResult.getRecs();
//            long pos = 1;
//            for (Recommendation t : recs) {
//                String itemId = ItemService.getClientItemId(c, t.getContent());
//                if (itemId != null) {
//                    bean.addBean(new RecommendationBean(itemId, pos++, null));
//                }
//            }
//            bean.setRequested(limit);
//            bean.setSize(recs.size());
//            if (Constants.CACHING && cfAlgorithm.getRecommendationCachingTimeSecs() > 0)
//                MemCachePeer.put(MemCacheKeys.getRecommendationsBeanKey(c.getShort_name(), cfAlgorithm,userId, type, dimensionId, full), bean, cfAlgorithm.getRecommendationCachingTimeSecs());
//        }
//        logger.info("Return RecommendationsBean for " + userId + " with dimension:" + dimensionId);
//        return bean;
//    }


    public static RecommendationBean findRecommendationBean(String itemId,ListBean list) {
        for(ResourceBean r : list.getList()) {
            RecommendationBean res = (RecommendationBean) r;
            if(res.getItem().equals(itemId)) { return res; }
        }
        return null;
    }

    //Object[0] = RecommendationsBean (result)
    //Object[1] = String representing the algorithm used
    public Object[] sort(ConsumerBean c,String userId,RecommendationsBean recs, List<String> algorithms) {
        //ALGORITHM
        CFAlgorithm cfAlgorithm = getAlgorithmOptions(c, userId, algorithms,null);

        //RANKING CACHING
        /*String memcacheKey = MemCacheKeys.getRankedItemsKey(c.getShort_name(), cfAlgorithm.hashCode(), userId, recs.toItems());
        RecommendationsBean res = (RecommendationsBean)MemCachePeer.get(memcacheKey);
        if(res == null) {*/

        //INIT
        Object[] res = new Object[2];
        RecommendationsBean resBean = new RecommendationsBean();
        String sortAlg = cfAlgorithm.toLogSorter();
        //res[0] = recs;
        res[0] = resBean;
        res[1] = sortAlg;

        long intUserId;
        try { 
            intUserId = UserService.getInternalUserId(c, userId);	
        }
        // USER NOT EXISTING
        catch(Exception e) {
            logger.debug("Not possibile to sort for a user not yet in the system");
            return res;
        }

        if(cfAlgorithm != null && cfAlgorithm.getSorters() != null && cfAlgorithm.getSorters().size()==1 && cfAlgorithm.getSorters().iterator().next() == CF_SORTER.NOOP) {
            res[0] = recs;
            return res;
        }

        //RECENT ACTIONS
        ActionHistoryCache ah = new ActionHistoryCache(cfAlgorithm.getName());
        List<Long> recentActions = ah.getRecentActions(intUserId, cfAlgorithm.getTxHistorySizeForSV()*2);
        logger.debug("RecentActions for user with client "+cfAlgorithm.getName()+" userId " + userId + " internal user id "+intUserId+" num." + recentActions.size() + " => " + StringUtils.join(recentActions,","));     
        //NOT ENOUGH ACTIONS
        //if(recentActions.size() < cfAlgorithm.getMinNumTxsForSV()) { return res; }

        //INPUT LIST
        List<Long> items = new ArrayList<>();
        for (RecommendationBean r : recs.getList()) {
            try {
                items.add(ItemService.getInternalItemId(c, r.getItem()));
            }
            catch(Exception e) {
                logger.warn("Not possible to rank the item " + r.getItem() + ". The item is not in the system", e);
            }
        }

        boolean testing = TestingUtils.get().getTesting();

        //print input list
        logger.debug("Input list for user " + userId + " num." + items.size() + " => " + StringUtils.join(items,","));
        //remove recentActions from recs
        List<Long> itemsToRemove = ListUtils.intersection(items, recentActions);
        //remove duplicate items
        HashSet hs = new HashSet();
        hs.addAll(itemsToRemove);
        itemsToRemove.clear();
        itemsToRemove.addAll(hs);
        List<Long> filteredItems = items;
        if(!itemsToRemove.isEmpty() && !testing && cfAlgorithm.isRankingRemoveHistory()) { // don't remove items if testing or alg options  says so
            logger.info("Removing recent items for user from list to rank");
            filteredItems = ListUtils.subtract(items, itemsToRemove);
        }

        //SORT
        SortResult sortResult = recommender.sort(intUserId, filteredItems, cfAlgorithm,recentActions);
        List<Long> itemsSorted = sortResult.getSortedItems();
        sortAlg = sortResult.toLog();
        logger.debug("Sorted list for user " + userId + " num." + itemsSorted.size() + " => " + StringUtils.join(itemsSorted,","));
        //INCLUDE VIEWED ITEMS
        //append recentActions (that were in the input list) only if it was able to sort the items (otherwise return an empty list)
        if(!itemsSorted.isEmpty() && !testing && cfAlgorithm.isRankingRemoveHistory()) { //only add back items if not testing
            itemsSorted.addAll(itemsToRemove);
        }
        //RESULT LIST
        int pos = 1;
        for (Long l : itemsSorted) { resBean.addBean(new RecommendationBean(ItemService.getClientItemId(c, l), pos++, null)); }

        /*MemCachePeer.put(memcacheKey, res,300);
	    //}
	    else*/
        logger.debug("Final list for user " + userId + " num." + itemsSorted.size() + " => " + StringUtils.join(itemsSorted,","));
        res[0] = resBean;
        res[1] = sortAlg;
        return res;
    }

    @SuppressWarnings("unchecked")
    public ListBean getRecommendedUsers(ConsumerBean c,String userId, String itemId, String linkType, List<String> algorithms, int limit) {
        return getRecommendedUsers(c, userId, itemId, linkType, null, limit);
    }


   

    public static CFAlgorithm getAlgorithmOptions(ConsumerBean c,String userId,List<String> algorithms,String recTag)
    {
        //ALGORITHM
        CFAlgorithm cfAlgorithm = null;
        try {
        	if (algorithms != null && !algorithms.isEmpty()) // if algorithms passed in this takes priority
        	{
        		cfAlgorithm = Util.getAlgorithmOptions(c, algorithms,recTag);
        	}
        	else
        	{
                //check if the AB testing is on and try to get algorithm if it is
                if(DynamicParameterServer.isABTesting(c.getShort_name(),recTag)) 
                {
                    cfAlgorithm = ABTestingServer.getUserTest(c.getShort_name(), recTag,userId);
                }
                
                if (cfAlgorithm == null) // get server side assigned algorithm for client 
                {
                	cfAlgorithm = Util.getAlgorithmService().getAlgorithmOptions(c,recTag);
                }
        	}
        }
        catch (CloneNotSupportedException e) {
            throw new APIException(APIException.CANNOT_CLONE_CFALGORITHM);
        }
        return cfAlgorithm;
    }

    public ResourceBean getRecommendedItems(ConsumerBean consumerBean, String userId, Long currentItemId,
                                            int dimensionId, String lastRecommendationListUuid, int limit,
                                            String attributes,List<String> algorithms,String referrer,String recTag) {
//        CFAlgorithm cfAlgorithm = getAlgorithmOptions(consumerBean, userId, algorithms,recTag); // default
        int typeId = 0;
        boolean full = true;

        final String shortName = consumerBean.getShort_name();

//        ListBean listBean = (ListBean) MemCachePeer.get(recommendedItemsKey(userId, cfAlgorithm, typeId, dimensionId, full, shortName));

        // Limit the size of the retrieved bean
//        listBean = Util.getLimitedBean(listBean, limit);

//        if (listBean == null) {
        ListBean listBean = new ListBean();

            Long internalUserId;
            try {
                internalUserId = UserService.getInternalUserId(consumerBean, userId);
            } catch (APIException e) {
                internalUserId = Constants.ANONYMOUS_USER;
            }
            catch (JDODataStoreException e)
            {
                logger.warn("Got a datastore exception trying to get userid for "+userId+" client "+shortName,e);
                internalUserId = Constants.ANONYMOUS_USER;
            }

            //Attributes
            List<String> attributeList = null;
            if(StringUtils.isNotBlank(attributes)) {
                attributeList = Arrays.asList(attributes.split(","));
            }

            RecommendationResult recResult = recommender.getRecommendations(
                    internalUserId, consumerBean.getShort_name(), userId, typeId, dimensionId, limit,
                    lastRecommendationListUuid, currentItemId, referrer, recTag
                    );
            List<Recommendation> recommendations = recResult.getRecs(); 
            for (Recommendation recommendation : recommendations) {
                String recommendedItemId = null;
                long internalId = recommendation.getContent();
                try {
                    recommendedItemId = ItemService.getClientItemId(consumerBean, internalId);
                } catch (APIException e) {
                    logger.warn("Item with internal ID " + internalId + " not found; ignoring..." , e);
                }
                if (recommendedItemId != null) {
                    final ItemBean itemBean = ItemService.getItem(consumerBean, recommendedItemId, full);
                    //filter the item
                    ItemBean resItem = ItemService.filter(itemBean, attributeList);
                    addUuidAttribute(resItem, recResult);
                    listBean.addBean(resItem);
                }
            }
            listBean.setRequested(limit);
            listBean.setSize(recommendations.size());

        return listBean;
    }

    private static void addUuidAttribute(ItemBean itemBean, RecommendationResult recResult) {
        Map<String,String> attributesName = itemBean.getAttributesName();
        if ( attributesName == null ) {
            attributesName = new HashMap<>();
        }
        attributesName.put(RECOMMENDATION_UUID_ATTR, recResult.getUuid());
    }

    private static String recommendedItemsKey(String userId, CFAlgorithm cfAlgorithm, int typeId, int dimensionId,
            boolean full, String shortName) {
        return MemCacheKeys.getRecommendedItemsKey(shortName, cfAlgorithm, userId, typeId, dimensionId, full);
    }

    public LastRecommendationBean retrieveLastRecs(ConsumerBean consumerBean, ActionBean actionBean,String recsCounter){
        return recommendationStorage.retrieveLastRecommendations(consumerBean.getShort_name(),
                actionBean.getUser(), recsCounter);


    }

    public List<Long> findIgnoredItemsFromLastRecs(ConsumerBean consumerBean, ActionBean actionBean, LastRecommendationBean lastRecs) {
        Long currentItem = itemService.getInternalItemId(consumerBean, actionBean.getItem());
        if(lastRecs!=null) {
            for (int i = 0; i < lastRecs.getRecs().size(); i++) {
                if (lastRecs.getRecs().get(i).equals(currentItem))
                    return lastRecs.getRecs().subList(0, i);
            }
        }
        // not in there
        return Collections.emptyList();
    }


}
