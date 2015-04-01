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

package io.seldon.trust.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;




/**
 * An item to hold the trust network for a member in the cache
 * @author clive
 *
 */
public class TrustNetwork implements Serializable, RecommendationNetwork  {
	private static Logger logger = Logger.getLogger( TrustNetwork.class.getName() );
	
	private int type;
	private Map<Long,TrustNetworkMember> trustedMembers;
	private Map<Long,TrustNetworkMember> trustedFriends;	
	
	public TrustNetwork(int type,Map<Long,TrustNetworkMember> tm,Map<Long,TrustNetworkMember> tf)
	{
		this.trustedMembers = tm;
		this.trustedFriends = tf;
		this.type = type;
	}
	
	public void addMember(TrustNetworkMember tcm) 
	{
		trustedMembers.put(tcm.getMember(), tcm);
		if (tcm.getSixd() == 1)
			trustedFriends.put(tcm.getMember(), tcm);
	}

	public TrustNetworkMember getTrustData(Long member) { return trustedMembers.get(member); }
	public Map<Long,TrustNetworkMember> getTrustedMembers() { return this.trustedMembers; }
	public Map<Long,TrustNetworkMember> getTrustedFriends() { return this.trustedFriends; }	
	public List<TrustNetworkMember> getNetwork() 
	{ 
		List<TrustNetworkMember> res = new ArrayList<>();
		res.addAll(this.trustedMembers.values());
		return res;
	}
	public Set<Long> getMembers() { return new HashSet<>(trustedMembers.keySet()); }
	public Set<Long> getFriends() { return new HashSet<>(trustedFriends.keySet()); }
	public int getSixd(Long member)
	{
		TrustNetworkMember tcm = trustedMembers.get(member);
		if (tcm != null)
			return tcm.getSixd();
		else 
		{
			return -1;
		}
	}
	public Trust getTrust(Long member)
	{
		TrustNetworkMember tcm = trustedMembers.get(member);
		if (tcm != null)
		{
			return tcm.getTrust();
		}
		else 
			return null;
	}
	public double getExpectation(Long member)
	{
		TrustNetworkMember tcm = trustedMembers.get(member);
		if (tcm != null)
		{
			return tcm.getE();
		}
		else 
			return 0;
	}

	public int getType() {
		return type;
	}

	@Override
	public long getId() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Double getSimilarity(long id) {
		return getExpectation(id);
	}

	@Override
	public List<Long> getSimilarityNeighbourhood(int k) {
		List<TrustNetworkMember> res = new ArrayList<>();
		res.addAll(this.trustedMembers.values());
		Collections.sort(res);
		List<Long> neighbourhood = new ArrayList<>();
		int count = 0;
		for(TrustNetworkMember m : res)
		{
			if (count>=k)
				break;
			neighbourhood.add(m.m);
			count++;
		}
		return neighbourhood;
	}

	@Override
	public Set<Long> getSimilarityNetwork() {
		return getMembers();
	}

	@Override
	public Map<Long, Double> getSimilarityNeighbourhoodMap(int k) {
		// TODO Auto-generated method stub
		return null;
	}
	
}
