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

package io.seldon.similarity.dbpedia.jdo;

public class ItemItemSimilarity {

	long userId;
	long likeId;
	Double ngd;
	Double jaccard;
	Double pmi;
	
	public ItemItemSimilarity()
	{
		
	}
	
	public ItemItemSimilarity(long userId, long likeId, Double ngd,
			Double jaccard, Double pmi) {
		super();
		this.userId = userId;
		this.likeId = likeId;
		this.ngd = ngd;
		this.jaccard = jaccard;
		this.pmi = pmi;
	}
	public long getUserId() {
		return userId;
	}
	public void setUserId(long itemId) {
		this.userId = itemId;
	}
	public long getLikeId() {
		return likeId;
	}
	public void setLikeId(long likeId) {
		this.likeId = likeId;
	}
	public Double getNgd() {
		return ngd;
	}
	public void setNgd(Double ngd) {
		this.ngd = ngd;
	}
	public Double getJaccard() {
		return jaccard;
	}
	public void setJaccard(Double jaccard) {
		this.jaccard = jaccard;
	}
	public Double getPmi() {
		return pmi;
	}
	public void setPmi(Double pmi) {
		this.pmi = pmi;
	}
	
	
	
}
