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

import java.util.HashMap;
import java.util.Map;


public class VectorUtils {

	TagStore tagStore;
	
	public VectorUtils(TagStore tagStore) {
		this.tagStore = tagStore;
	}

	private double getMagnitude(Map<String,Double> v)
	{
		double sum = 0;
		for(Double val : v.values())
			sum = sum + (val * val);
		return Math.sqrt(sum);
	}
	
	private Map<String,Double> createTermVector(Map<String,Long> tags,TagSimilarityPeer.VectorSpaceOptions.TF_TYPE tf_type)
	{
		Map<String,Double> v = new HashMap<>();
		double maxTF = 0;
		if (tf_type == TagSimilarityPeer.VectorSpaceOptions.TF_TYPE.AUGMENTED)
		{
			for(Long val : tags.values())
				if (val > maxTF)
					maxTF = val;
		}
		for(Map.Entry<String, Long> e : tags.entrySet())
		{
			double tval = 0;
			switch(tf_type)
			{
			case TF:
				tval = e.getValue();
				break;
			case LOGTF:
				tval = 1+Math.log(e.getValue());
				break;
			case AUGMENTED:
				tval = 0.4 + 0.6*(e.getValue()/maxTF);
				break;
			}
			v.put(e.getKey(), tval);
		}
		return v;
	}
	
	
	
	private Map<String,Double> documentFrequency(Map<String,Double> v,TagSimilarityPeer.VectorSpaceOptions.DF_TYPE dt_type)
	{
		if (dt_type == TagSimilarityPeer.VectorSpaceOptions.DF_TYPE.NONE)
			return v;
		else
		{
			Map<String,Double> vn = new HashMap<>();
			for(Map.Entry<String, Double> e : v.entrySet())
			{
				switch(dt_type)
				{
				case IDF:
					double idf = tagStore.getIDF(e.getKey());
					vn.put(e.getKey(), e.getValue()*idf);
					break;
				}
			}
			return vn;
		}
	}
	
	private Map<String,Double> normalise(Map<String,Double> v,TagSimilarityPeer.VectorSpaceOptions.NORM_TYPE norm_type)
	{
		if (norm_type == TagSimilarityPeer.VectorSpaceOptions.NORM_TYPE.NONE)
			return v;
		else if (norm_type == TagSimilarityPeer.VectorSpaceOptions.NORM_TYPE.COSINE)
		{
			double sum = getMagnitude(v);
			Map<String,Double> vn = new HashMap<>();
			for(Map.Entry<String, Double> e : v.entrySet())
			{
				double normFactor = 1/sum;
				vn.put(e.getKey(), e.getValue() * normFactor);
			}
			return vn;
		}
		else
			return null;

	}
	
	public double dotProduct(Map<String,Double> v1,Map<String,Double> v2)
	{
		double sum = 0;
		for(Map.Entry<String, Double> e : v1.entrySet())
			if (v2.containsKey(e.getKey()))
				sum = sum + (e.getValue() * v2.get(e.getKey()));
		return sum;
	}
	
	public Map<String,Double> getVector(Map<String,Long> tags,TagSimilarityPeer.VectorSpaceOptions options)
	{
		Map<String,Double> v = createTermVector(tags,options.getTermFreq());
		v = documentFrequency(v,options.getDocFreq());
		v = normalise(v,options.getNormalization());
		return v;
	}
}
