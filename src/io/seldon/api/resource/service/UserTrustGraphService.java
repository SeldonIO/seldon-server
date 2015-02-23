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

import io.seldon.trust.impl.CFAlgorithm;
import io.seldon.api.APIException;
import io.seldon.api.Util;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ListBean;
import io.seldon.api.resource.UserTrustNodeBean;
import io.seldon.general.ExplicitLink;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import io.seldon.trust.impl.RecommendationNetwork;
import io.seldon.trust.impl.RummbleLabsAPI;
import io.seldon.trust.impl.jdo.RecommendationPeer;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.seldon.api.Constants;

/**
 * @author claudio
 */

@Service
public class UserTrustGraphService {
	private static Logger logger = Logger.getLogger(UserTrustGraphService.class.getName());

	@Autowired
	private RecommendationPeer recommendationPeer;
	public UserTrustNodeBean getNode(ConsumerBean c, String user, String fromUser) throws APIException {
		ListBean graph = getGraph(c,fromUser,Constants.DEFAULT_BIGRESULT_LIMIT);
		UserTrustNodeBean bean = (UserTrustNodeBean)graph.getElement(user);
		if(bean == null) {
			bean = new UserTrustNodeBean(fromUser,user,Constants.TRUST_NOT_DEFINED,Constants.POSITION_NOT_DEFINED,Constants.DEFAULT_DIMENSION);
		}
		return bean;
	}
	
	public ListBean getGraph(ConsumerBean c, String user, int limit) throws APIException {
		logger.info("Get TrustGraphBean for " + user);
		ListBean bean = (ListBean) MemCachePeer.get(MemCacheKeys.getUserTrustGraphBeanKey(c.getShort_name(), user));
		bean = Util.getLimitedBean(bean, limit);
		if(bean == null) {
            bean = new ListBean();
            RummbleLabsAPI tp;
            CFAlgorithm cfAlgorithm;
            try {
                cfAlgorithm = Util.getAlgorithmService().getAlgorithmOptions(c);
            } catch (CloneNotSupportedException e) {
                throw new APIException(APIException.CANNOT_CLONE_CFALGORITHM);
            }
            RecommendationNetwork trustNet =recommendationPeer
                    .getNetwork(UserService.getInternalUserId(c, user), Constants.DEFAULT_DIMENSION, cfAlgorithm);
            List<Long> network = trustNet.getSimilarityNeighbourhood(limit);
            int count = 1;
            for (Long u : network) {
                bean.addBean(new UserTrustNodeBean(user, UserService.getClientUserId(c, u), trustNet.getSimilarity(u), count++, Constants.DEFAULT_DIMENSION));
            }
            if (Constants.CACHING)
                MemCachePeer.put(MemCacheKeys.getUserTrustGraphBeanKey(c.getShort_name(), user), bean, Constants.CACHING_TIME);
        }
		logger.info("Return TrustGraphBean for " + user);
		return bean;
	}

	public static void addEplicitLink(ConsumerBean c, UserTrustNodeBean bean) {
		ExplicitLink e = bean.createExplicitLink(c);
		Util.getNetworkPeer(c).addExplicitLink(e);
	}
	
}

