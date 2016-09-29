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
import io.seldon.api.Util;
import io.seldon.api.caching.ActionHistoryCache;
import io.seldon.api.resource.ActionBean;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ItemBean;
import io.seldon.api.resource.ItemRecommendationsBean;
import io.seldon.api.resource.ListBean;
import io.seldon.api.resource.RecommendationBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.general.RecommendationStorage;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.recommendation.CFAlgorithm;
import io.seldon.recommendation.LastRecommendationBean;
import io.seldon.recommendation.Recommendation;
import io.seldon.recommendation.RecommendationPeer;
import io.seldon.recommendation.RecommendationResult;
import io.seldon.recommendation.explanation.ExplanationPeer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jdo.JDODataStoreException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
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
    private static final String EXPLANATION_ATTR = "_explanation";
    
    public static final String KEYWORD_PAR = "keywords";

    @Autowired
    private RecommendationPeer recommender;
    @Autowired
    private RecommendationStorage recommendationStorage;
    @Autowired
    private ItemService itemService;
    @Autowired
    private UserService userService;
    @Autowired
    private ActionHistoryCache actionCache;
    @Autowired
    private ExplanationPeer explanationPeer;

    public static RecommendationBean findRecommendationBean(String itemId,ListBean list) {
        for(ResourceBean r : list.getList()) {
            RecommendationBean res = (RecommendationBean) r;
            if(res.getItem().equals(itemId)) { return res; }
        }
        return null;
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
                                            Set<Integer> dimensions, String lastRecommendationListUuid, int limit,
                                            String attributes,List<String> algorithms,String referrer,String recTag, boolean includeCohort,Set<Long> scoreItems,String locale) {
        List<String> actualAlgorithms = new ArrayList<>();
        if(algorithms!=null) {
            for (String alg : algorithms) {
                if (alg.startsWith("recommenders")) {
                    actualAlgorithms = Arrays.asList(alg.split(":")[1].split("\\|"));
                }
            }
        }
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
                internalUserId = userService.getInternalUserId(consumerBean, userId);
            } catch (APIException e) {
                internalUserId = Constants.ANONYMOUS_USER;
            }
            catch (JDODataStoreException e)
            {
                logger.error("Got a datastore exception trying to get userid for "+userId+" client "+shortName,e);
                internalUserId = Constants.ANONYMOUS_USER;
            }

            //Attributes
            List<String> attributeList = null;
            if(StringUtils.isNotBlank(attributes)) {
                attributeList = Arrays.asList(attributes.split(","));
            }

            
            final ImmutablePair<RecommendationResult, String> recResultPair = recommender.getRecommendations(
                        internalUserId, consumerBean.getShort_name(), userId, typeId, dimensions, limit,
                        lastRecommendationListUuid, currentItemId, referrer, recTag,actualAlgorithms, scoreItems);
            
            final RecommendationResult recResult = recResultPair.left;
            final String algKey = recResultPair.right;
            String recExplanation = null;
            final boolean isExplanationNeeded = explanationPeer.isExplanationNeededForClient(shortName);
            if (isExplanationNeeded) {
                recExplanation = explanationPeer.explainRecommendationResult(shortName, algKey, locale);
            }
            List<Recommendation> recommendations = recResult.getRecs();
            List<ItemBean> itemRecs = new ArrayList<>();
            for (Recommendation recommendation : recommendations) {
                String recommendedItemId = null;
                long internalId = recommendation.getContent();
                try {
                    recommendedItemId = itemService.getClientItemId(consumerBean, internalId);
                } catch (APIException e) {
                    logger.info("Item with internal ID " + internalId + " not found; ignoring..." , e);
                }
                if (recommendedItemId != null) {
                    final ItemBean itemBean = itemService.getItemLocalized(consumerBean, recommendedItemId, full,locale);
                    if (isExplanationNeeded) {
                        addExplanationAttribute(itemBean, recExplanation);
                    }
                    //filter the item
                    ItemBean resItem = ItemService.filter(itemBean, attributeList);
                    addUuidAttribute(resItem, recResult);
                    itemRecs.add(resItem);
                }
            }
            if (includeCohort) {
                return new ItemRecommendationsBean(itemRecs,recResult.getCohort());
            } else {
                listBean.addAll(itemRecs);
                listBean.setRequested(limit);
                listBean.setSize(recommendations.size());

                return listBean;
            }
    }

    private static void addUuidAttribute(ItemBean itemBean, RecommendationResult recResult) {
        Map<String,String> attributesName = itemBean.getAttributesName();
        if ( attributesName == null ) {
            attributesName = new HashMap<>();
        }
        attributesName.put(RECOMMENDATION_UUID_ATTR, recResult.getUuid());
    }
    
    private static void addExplanationAttribute(ItemBean itemBean, String recExplanation) {
        Map<String,String> attributesName = itemBean.getAttributesName();
        if ( attributesName == null ) {
            attributesName = new HashMap<>();
        }
        attributesName.put(EXPLANATION_ATTR, recExplanation);
    }

    private static String recommendedItemsKey(String userId, CFAlgorithm cfAlgorithm, int typeId, int dimensionId,
            boolean full, String shortName) {
        return MemCacheKeys.getRecommendedItemsKey(shortName, cfAlgorithm, userId, typeId, dimensionId, full);
    }

    public LastRecommendationBean retrieveLastRecs(ConsumerBean consumerBean, ActionBean actionBean,String recsCounter, String recTag){
        return recommendationStorage.retrieveLastRecommendations(consumerBean.getShort_name(),
                actionBean.getUser(), recsCounter, recTag);


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
