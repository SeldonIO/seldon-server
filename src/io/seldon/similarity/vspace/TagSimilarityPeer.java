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

package io.seldon.similarity.vspace;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import io.seldon.util.CollectionTools;


/**
 * Provide simple tag similarity based on a vector space model using term frequency - inverse document frequency
 * ideas. Options can be provided by @link VectorSpaceOptions to control the similarity matching.
 * @author rummble
 *
 */
public class TagSimilarityPeer {
	
	private static Logger logger = Logger.getLogger(TagSimilarityPeer.class.getName());
	
	VectorSpaceOptions documentOptions;
	VectorSpaceOptions queryOptions;
	TagStore tagStore;
	VectorUtils vectorUtils;
	
	public TagSimilarityPeer(VectorSpaceOptions documentOptions,
			VectorSpaceOptions queryOptions, TagStore tagStore) {
		this.documentOptions = documentOptions;
		this.queryOptions = queryOptions;
		this.tagStore = tagStore;
		this.vectorUtils = new VectorUtils(tagStore);
	}

	/**
	 * Given a list of items return a sorted list of items ordered by similarity to a user based on the vector
	 * space similarity of the user's document vector to the query vectors for each item
	 * @param user_id
	 * @param items
	 * @return
	 */
	public List<Long> sortBySimilarity(long user_id,Collection<Long> items)
	{
		Map<String,Long> userTags = tagStore.getUserTags(user_id);
		Map<String,Double> uVec = vectorUtils.getVector(userTags, this.documentOptions);
		Map<Long,Double> scores = new HashMap<>();
		for(Long item_id : items)
		{
			Map<String,Long> itemTags = tagStore.getItemTags(item_id);
			Map<String,Double> iVec = vectorUtils.getVector(itemTags, this.queryOptions);
			double score = vectorUtils.dotProduct(uVec, iVec);
			logger.info("User:"+user_id+" item:"+item_id+" score:"+score);
			scores.put(item_id, score);
		}
		List<Long> result = CollectionTools.sortMapAndLimitToList(scores, scores.size());
		return result;
	}
	


	/**
	 * Provide options for vector space similarity 
	 * <ul>
	 * <li> <b>TF_TYPE</b> - term frequency. <b>TF</b> simple term frequency, <b>LOGTF</b> log term frequency
	 * <b>AUGMENTED</b> augemented term frequency
	 * <li> <b>DF_TYPE</b> - document frequency. <b>NONE</> no document frequency weighting. <b>IDF</b> inverse document 
	 * frequency
	 * <li> <b>NORM_TYPE</b> - normalization. <b>NONE</b> no normalization. <b>COSINE</b> cosine normalization
	 * </ul>
	 * See @link http://nlp.stanford.edu/IR-book/html/htmledition/document-and-query-weighting-schemes-1.html
	 * @author rummble
	 *
	 */
	public static class VectorSpaceOptions 
	{
		public enum TF_TYPE { TF, LOGTF, AUGMENTED } ;
		public enum DF_TYPE { NONE, IDF };
		public enum NORM_TYPE { NONE, COSINE };
	
		TF_TYPE termFreq = TF_TYPE.LOGTF;
		DF_TYPE docFreq = DF_TYPE.NONE;
		NORM_TYPE normalization = NORM_TYPE.COSINE;
		
		public VectorSpaceOptions(TF_TYPE termFreq, DF_TYPE docFreq,
				NORM_TYPE normalization) {
			this.termFreq = termFreq;
			this.docFreq = docFreq;
			this.normalization = normalization;
		}

		public TF_TYPE getTermFreq() {
			return termFreq;
		}

		public DF_TYPE getDocFreq() {
			return docFreq;
		}

		public NORM_TYPE getNormalization() {
			return normalization;
		}
		
		
	}
	
	
}
