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

package io.seldon.pearson;

import java.util.Collection;

import io.seldon.prediction.ContentRatingResolver;
import io.seldon.trust.impl.TrustRecommendation;

public class PearsonPredictor {
	
	PearsonSimilarityHandler sim;
	ContentRatingResolver cr;
	
	public PearsonPredictor(PearsonSimilarityHandler sim,ContentRatingResolver cr)
	{
		this.cr = cr;
		this.sim = sim;
	}
	
	public TrustRecommendation ResnickPrediction(Long src,Long contentId,int type,Collection<Long> contentMembers,double minRating,double maxRating,boolean transformE,boolean useCaseEnhancement,double weightThreshold)
	{
		if (contentMembers == null || contentMembers.size() == 0)
			return null;
		else
		{
			double ratingSum = 0;
			double weightSum = 0;
			
			for(Long user : contentMembers)
			{
				Double weight = sim.getSimilarity(src, user);
				if (weight > weightThreshold)
				{
					if (transformE)
					{
						// transform E probability to range -1 to +1
						if (weight > 0.5)
							weight = (weight - 0.5) * 2;
						else if (weight <= 0.5)
							weight = -1 + (weight * 2);
					}
					
					if (useCaseEnhancement)
					{
						//use case enhancement
						weight = weight * Math.pow(Math.abs(weight), 1.5);
					}
					
					Double avgRating = cr.getAvgRating(user, type);
					if (avgRating != null)
					{
						double rating = cr.getRating(user, contentId);
						ratingSum = ratingSum + ((rating - avgRating) * weight);
						weightSum = weightSum + Math.abs(weight);
					}
				}
			}
			
			if (weightSum > 0)
			{
				Double userAvg = cr.getAvgRating(src, type);
				if (userAvg != null)
				{
					double cfRating = userAvg + (ratingSum/weightSum);
					if (cfRating > maxRating)
						cfRating = maxRating;
					else if (cfRating < minRating)
						cfRating = minRating;
					
					return new TrustRecommendation(contentId,type,cfRating,userAvg,null,null);
				}
				else
					return null;
			}
			else
				return null;
			
		}
	}

}
