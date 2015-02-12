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

import java.util.Collection;

import io.seldon.api.APIException;
import io.seldon.api.Util;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ListBean;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import io.seldon.trust.impl.CFAlgorithm;
import io.seldon.trust.impl.Recommendation;
import io.seldon.trust.impl.RummbleLabsAPI;
import org.springframework.stereotype.Service;

import io.seldon.api.Constants;
import io.seldon.api.resource.OpinionBean;
import io.seldon.general.Opinion;

/**
 * @author claudio
 */

@Service
public class OpinionService {
	
	public static ListBean getUserOpinions(ConsumerBean c, String userId, int limit, boolean full) throws APIException {
		ListBean bean = (ListBean) MemCachePeer.get(MemCacheKeys.getUserOpinionBeansKey(c.getShort_name(), userId, full));
		bean = Util.getLimitedBean(bean, limit);
		if(bean == null) {
			bean = new ListBean();
			Collection<Opinion> res = Util.getOpinionPeer(c).getUserOpinions(UserService.getInternalUserId(c, userId), limit);
			for(Opinion o : res) { bean.addBean(new OpinionBean(o,c,full)); }
			if(Constants.CACHING) MemCachePeer.put(MemCacheKeys.getUserOpinionBeansKey(c.getShort_name(), userId, full),bean,Constants.CACHING_TIME);
		}
		return bean;
			
	}

	public static ListBean getItemOpinions(ConsumerBean c, String itemId, int limit, boolean full) throws APIException {
		ListBean bean = (ListBean) MemCachePeer.get(MemCacheKeys.getItemOpinionBeansKey(c.getShort_name(),itemId,full));
		bean = Util.getLimitedBean(bean, limit);
		if(bean == null) {
			bean = new ListBean();
			Collection<Opinion> res = Util.getOpinionPeer(c).getItemOpinions(ItemService.getInternalItemId(c, itemId), limit);
			for(Opinion o : res) { bean.addBean(new OpinionBean(o,c,full)); }
			if(Constants.CACHING) MemCachePeer.put(MemCacheKeys.getItemOpinionBeansKey(c.getShort_name(), itemId, full),bean,Constants.CACHING_TIME);
		}
		return bean;
	} 
	
	//get the real opinion if existing, otherwise create a prediction
	public static OpinionBean getOpinion(ConsumerBean c, String userId, String itemId) throws APIException {
		OpinionBean bean = (OpinionBean)MemCachePeer.get(MemCacheKeys.getOpinionBeanKey(c.getShort_name(), userId, itemId));
		if(bean == null) {
			Opinion o = Util.getOpinionPeer(c).getOpinion(ItemService.getInternalItemId(c, itemId),UserService.getInternalUserId(c, userId));
			if(o != null) {
				bean = new OpinionBean(o,c,true);
			}
			else {
				bean = new OpinionBean(userId,itemId);
                CFAlgorithm cfAlgorithm;
                try {
                    cfAlgorithm = Util.getAlgorithmService().getAlgorithmOptions(c);
                } catch (CloneNotSupportedException e) {
                    throw new APIException(APIException.CANNOT_CLONE_CFALGORITHM);
                }
                RummbleLabsAPI tp = Util.getLabsAPI(cfAlgorithm);

				Recommendation prediction;
				prediction = tp.getAnalysis(cfAlgorithm).getPersonalisedRating(UserService.getInternalUserId(c, userId), ItemService.getInternalItemId(c, itemId), Constants.DEFAULT_DIMENSION,cfAlgorithm);
				if (prediction != null) {
					bean.setValue(prediction.getPrediction());
					bean.initSrcUsers();
					bean.addSrcUser(prediction.getMostTrustedUser());
				}
			}
			if(Constants.CACHING) MemCachePeer.put(MemCacheKeys.getOpinionBeanKey(c.getShort_name(), userId, itemId),bean,Constants.CACHING_TIME);
		}
		return bean;
	}
}
