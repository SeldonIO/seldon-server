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

import java.util.List;

import io.seldon.api.Util;
import io.seldon.trust.impl.jdo.RecommendationPeer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.seldon.api.APIException;
import io.seldon.api.Constants;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ItemBean;
import io.seldon.api.resource.ItemSimilarityNodeBean;
import io.seldon.api.resource.ListBean;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import io.seldon.trust.impl.CFAlgorithm;
import io.seldon.trust.impl.RummbleLabsAPI;
import io.seldon.trust.impl.SearchResult;

/**
 * @author claudio
 */

@Service
public class ItemSimilarityGraphService {

    @Autowired
    private RecommendationPeer recommendationPeer;
	
	public ItemSimilarityNodeBean getNode(ConsumerBean c, String itemId, String fromItemId) throws APIException {
		ListBean sims = getGraph(c,fromItemId,Constants.DEFAULT_BIGRESULT_LIMIT);
		ItemSimilarityNodeBean bean = (ItemSimilarityNodeBean)sims.getElement(itemId);
		if(bean == null) {
			bean = new ItemSimilarityNodeBean(itemId,Constants.SIMILARITY_NOT_DEFINED);
		}
		return bean;
	}
	
	public ListBean getGraph(ConsumerBean c, String itemId, int limit) throws APIException {
		ListBean bean = (ListBean)MemCachePeer.get(MemCacheKeys.getItemSimilarityGraphBeanKey(c.getShort_name(), itemId));
		bean = Util.getLimitedBean(bean, limit);
		if(bean == null) {
            bean = new ListBean();
            CFAlgorithm cfAlgorithm;
            try {
                cfAlgorithm = Util.getAlgorithmService().getAlgorithmOptions(c);
            } catch (CloneNotSupportedException e) {
                throw new APIException(APIException.CANNOT_CLONE_CFALGORITHM);
            }
            ItemBean ib = ItemService.getItem(c, itemId, false);
            List<SearchResult> res = recommendationPeer.findSimilar(ItemService.getInternalItemId(c, itemId), ib.getType(), limit, cfAlgorithm);
            for (SearchResult r : res) {
                bean.addBean(new ItemSimilarityNodeBean(ItemService.getClientItemId(c, r.getId()), r.getScore()));
            }
            if (Constants.CACHING)
                MemCachePeer.put(MemCacheKeys.getItemSimilarityGraphBeanKey(c.getShort_name(), itemId), bean, Constants.CACHING_TIME);
        }
		return bean;
	}
	
}
