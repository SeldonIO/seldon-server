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

import java.util.Set;

import io.seldon.db.jdo.ClientPersistable;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;
import io.seldon.trust.impl.CFAlgorithm;
import io.seldon.trust.impl.RecommendationNetwork;
import io.seldon.trust.impl.TrustNetworkSupplier;

public class SimpleTrustNetworkProvider extends ClientPersistable implements TrustNetworkSupplier {

    private CFAlgorithm cfAlgorithm;

	public SimpleTrustNetworkProvider(CFAlgorithm cfAlgorithm) {
		super(cfAlgorithm.getName());
        this.cfAlgorithm = cfAlgorithm;
	}

	@Override
	public RecommendationNetwork getTrustNetwork(long user, int type) {
		return getTrustNetwork(user,type,false,CF_TYPE.USER);
	}

	@Override
	public RecommendationNetwork getTrustNetwork(long id, int type,boolean fullNetworkNeeded, CF_TYPE trustType) {
		RecommendationNetwork net = (RecommendationNetwork) MemCachePeer.get(MemCacheKeys.getRecommendationNetworkKey(cfAlgorithm.getName(),id, type,trustType));
		if (net == null)
		{
			SimpleTrustNetworkPeer p = new SimpleTrustNetworkPeer(getPM());
			net = p.getNetwork(id, type, trustType);
			if (net != null)
				MemCachePeer.put(MemCacheKeys.getRecommendationNetworkKey(cfAlgorithm.getName(),id, type,trustType), net, RecommendationPeer.MEMCACHE_TRUSTNET_EXPIRE_SECS);
		}
		return net;
	}
	
	@Override
	public RecommendationNetwork getCooccurenceNetwork(long user) {
		RecommendationNetwork net = (RecommendationNetwork)  MemCachePeer.get(MemCacheKeys.getCooccurenceNetworkKey(cfAlgorithm.getName(),user));
		if (net == null)
		{
			SimpleTrustNetworkPeer p = new SimpleTrustNetworkPeer(getPM());
			net = p.getCooccurrenceNetwork(user);
			if (net != null) { MemCachePeer.put(MemCacheKeys.getCooccurenceNetworkKey(cfAlgorithm.getName(),user), net, RecommendationPeer.MEMCACHE_TRUSTNET_EXPIRE_SECS); }
		}
		return net;
	}


	@Override
	public void addTrustLink(long u1, int type,long u2, double trust) {
		SimpleTrustNetworkPeer p = new SimpleTrustNetworkPeer(getPM());
		p.addTrust(u1, type,u2, trust);
	}

	@Override
	public RecommendationNetwork getJaccardUpdatedNetwork(long userId,Set<Long> users,Long actions) {
		SimpleTrustNetworkPeer p = new SimpleTrustNetworkPeer(getPM());
		return p.getJaccardUpdatedNetwork(userId,users,actions);
	}

}
