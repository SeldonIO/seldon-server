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

package io.seldon.prediction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import io.seldon.trust.impl.Recommendation;
import io.seldon.util.CollectionTools;

/**
 * Provides various standard collaborate filtering neighbourhood prediction implementations
 * <ul>
 * <li> Weighted Median - taken from "Rating Aggregation in Collaborative Filtering Systems" by Garcin et al
 * <li> Resnick formula - classic neighbourhood formula
 * <li> Resnick-Saric - modification of resnick formula see Saric in "Alternative formulas for rating prediction using collaborative filter" (formula 5))
 * <li> Weighted mean - basic weighted mean
 * </ul>
 * @author rummble
 *
 */
public class PersonalisedRatingCreator {

	public enum Strategy { WEIGHTED, MOST_TRUSTED_OR_WEIGHTED};
	ContentRatingResolver cr;
	
	public PersonalisedRatingCreator(ContentRatingResolver cr)
	{
		this.cr = cr;
	}
	
	
	
	public Double getAvgRating(Long contentId,int type,Collection<Long> contentMembers)
	{
		double ratings = 0;
		double count = 0;
		for(Long user : contentMembers)
		{
			double rating = cr.getRating(user, contentId);
			ratings = ratings + rating;
			count++;
		}
		if (count > 0)
			return ratings/count;
		else
			return null;
	}
	
	public Recommendation weightedMedianPrediction(Long src,Long contentId,int type,Map<Long,Double> weights,double weightCutoff)
	{
		
		ArrayList<WeightedRating> ratings = new ArrayList<WeightedRating>();
			
		for(Long user : weights.keySet())
		{
			double weight = weights.get(user);
			if (weight > weightCutoff)
			{
				Double rating = cr.getRating(user, contentId);
				if (rating != null)
					ratings.add(new WeightedRating(user,weight,rating));
			}
		}

		Collections.sort(ratings);
		
		ArrayList<Double> weightSum = new ArrayList<Double>();
		for(int i=0;i<ratings.size();i++)
			if (i==0)
				weightSum.add(ratings.get(i).weight);
			else
				weightSum.add(weightSum.get(weightSum.size()-1) + ratings.get(i).weight);
		
		double sum = 0;
		int i;
		for(i=ratings.size()-1;i>=0;i--)
		{
			sum = sum + ratings.get(i).weight;
			if (weightSum.get(i) < sum )
				break;
		}
		
		if (i >= 0)
			return new Recommendation(contentId,type,ratings.get(i).rating,null,ratings.get(i).user,null);
		else if (ratings.size()==1)
			return new Recommendation(contentId,type,ratings.get(0).rating,null,ratings.get(0).user,null);
		else
			return null;
	
	}
	
	public Recommendation weightedAveragePrediction(Long src,Long contentId,int type,Map<Long,Double> weights,double minRating,double maxRating,boolean useCaseEnhancement,boolean useUserAvg,double weightCutoff)
	{
		
		double ratingSum = 0;
		double weightSum = 0;
			
		for(Long user : weights.keySet())
		{
			Double rating = cr.getRating(user, contentId);
			if (rating != null)
			{
				double weight = weights.get(user);
				if (weight > weightCutoff)
				{
					if (useCaseEnhancement)
					{
						//use case enhancement
						weight = weight * Math.pow(Math.abs(weight), 1.5);
					}
				
					ratingSum = ratingSum + (rating * weight);
					weightSum = weightSum + Math.abs(weight);
				}
			}
		}
		
		if (weightSum > 0)
		{
			double cfRating;
			Double userAvg = cr.getAvgRating(src, type);
			if (userAvg != null && useUserAvg)
				cfRating = (userAvg + (ratingSum/weightSum))/2;
			else
				cfRating = ratingSum/weightSum;
			if (cfRating > maxRating)
				cfRating = maxRating;
			else if (cfRating < minRating)
				cfRating = minRating;
				
			return new Recommendation(contentId,type,cfRating,userAvg,null,null);
				
		}
		else
			return null;
		
		
	}
	
	public Recommendation ResnickItemBasedPrediction(Long src,long contentId,int type,Map<Long,Double> weights,double minRating,double maxRating,double weightCutoff)
	{
		
		double ratingSum = 0;
		double weightSum = 0;
		
		for(Long content : weights.keySet())
		{
			Double rating = cr.getRating(src, content);
			if (rating != null)
			{
				double weight = weights.get(content);
				if (weight > weightCutoff)
				{
					Double avgRating = cr.getAvgContentRating(content, type);
					if (avgRating != null)
					{
						ratingSum = ratingSum + ((rating - avgRating) * weight);
						weightSum = weightSum + Math.abs(weight);
					}
				}
			}
		}
		
		if (weightSum > 0)
		{
			Double contentAvg = cr.getAvgContentRating(contentId, type);
			if (contentAvg != null)
			{
				double cfRating = contentAvg + (ratingSum/weightSum);
				if (cfRating > maxRating)
					cfRating = maxRating;
				else if (cfRating < minRating)
					cfRating = minRating;
				
				return new Recommendation(contentId,type,cfRating,contentAvg,null,null);
			}
			else
				return null;
		}
		else
			return null;
		
	
	}
	
	public Recommendation ResnickPrediction(Long src,Long contentId,int type,Map<Long,Double> weights,double minRating,double maxRating,boolean transformE,boolean useCaseEnhancement)
	{
		double ratingSum = 0;
		double weightSum = 0;
		
		for(Long user : weights.keySet())
		{
			Double rating = cr.getRating(user, contentId);
			if (rating != null)
			{
				double weight = weights.get(user);
				
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
				
				return new Recommendation(contentId,type,cfRating,userAvg,null,null);
			}
			else
				return null;
		}
		else
			return null;
		
		
	}
	
	
	public Recommendation ModifiedResnickPrediction(Long src,Long contentId,int type,Map<Long,Double> weights,int k,double minRating,double maxRating,boolean transformE,boolean useCaseEnhancement,double weightCutoff,boolean useSimilarityWeighting)
	{
		
		double avgSum = 0;
		double ratingSum = 0;
		double weightSum = 0;

		int count = 0;
		for(Map.Entry<Long, Double> e : CollectionTools.sortByValue(weights))
		{
			Double rating = cr.getRating(e.getKey(), contentId);
			if (rating != null)
			{
				count++;
				double weight = e.getValue();
				if (weight > weightCutoff)
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
					
					Double avgRating = cr.getAvgRating(e.getKey(), type);
					Double avgSharedRating = cr.getAvgRating(src, e.getKey(), type);
					
					double weighting;
					if (useSimilarityWeighting)
					{
						// no need for sign maintenance as all our similarities are from 0-1 at present
						weighting = weight * weight;
					}
					else
						weighting = weight;
					
					if (avgRating != null && avgSharedRating != null)
					{
						ratingSum = ratingSum + ((rating - avgRating) * weighting);
						weightSum = weightSum + Math.abs(weighting);
					
						avgSum = avgSum + (avgSharedRating * weighting);
					}
				}
				else 
					break;
				if (count >= k)
					break;
			}
		}
		
		if (weightSum > 0)
		{
			double baseLine = (avgSum/weightSum);
			double cfRating =  baseLine + (ratingSum/weightSum);
			if (cfRating > maxRating)
				cfRating = maxRating;
			else if (cfRating < minRating)
				cfRating = minRating;
				
			return new Recommendation(contentId,type,cfRating,baseLine,null,null);
			
		}
		else
			return null;
		
	
	
		
	}
	
}
