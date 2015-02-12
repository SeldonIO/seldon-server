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
import java.util.List;

public class SharingRecommendation implements Comparable<SharingRecommendation>, Serializable  {
	private Long userId; // sharing recommendation for this user
	private Long itemId; // the item we suggest is of interest to the user
	private double score; // a score so different sharing recommendations can be compared
	private List<String> clientItemIds; // the corresponding client items ids that helped in producing the score for the user
	private List<Long> itemIds; // the items that helped in producing the score for the user
	private List<String> reasons; // tokens ("likes") that show why these items were picked
	
	
	public SharingRecommendation(Long userId,Long itemId)
	{
		this.userId = userId;
		this.itemId = itemId;
		this.score = 0;
		itemIds = new ArrayList<Long>();
		clientItemIds = new ArrayList<String>();
		reasons = new ArrayList<String>();
	}
	
	public SharingRecommendation(Long userId, double score, List<Long> itemIds) {
		super();
		this.userId = userId;
		this.score = score;
		this.itemIds = itemIds;
	}
	
	public SharingRecommendation merge(SharingRecommendation r)
	{
		itemIds.addAll(r.itemIds);
		return this;
	}
	
	public void addItem(Long itemId,String clientItemId,String reason,double scoreDelta)
	{
		itemIds.add(itemId);
		this.clientItemIds.add(clientItemId);
		reasons.add(reason);
		score = score + scoreDelta;
	}

	public Long getUserId() {
		return userId;
	}

	public void setUserId(Long userId) {
		this.userId = userId;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	public Long getItemId() {
		return itemId;
	}

	public List<String> getReasons() {
		return reasons;
	}

	public List<Long> getItemIds() {
		return itemIds;
	}

	public void setItemIds(List<Long> itemIds) {
		this.itemIds = itemIds;
	}

	
	
	public List<String> getClientItemIds() {
		return clientItemIds;
	}

	public void setClientItemIds(List<String> clientItemIds) {
		this.clientItemIds = clientItemIds;
	}

	@Override
	public int compareTo(SharingRecommendation o) {
		if (this.score < o.score)
			return -1;
		else if (this.score > o.score)
			return 1;
		else
			return 0;
	}
	
}