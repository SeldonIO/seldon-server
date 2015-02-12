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

package io.seldon.bayes;

/**
 * A Naive Baysian predictor as described in Candillier et al. "Comparing State-of-the-Art Collaborative Filtering Systems"
 * @author Clive
 *
 */
public class BayesRecommender {

	BayesDataProvider dataProvider;

	
	static boolean preloaded = false;
	
	static BayesData bayesData = null;

	/**
	 * A method to pre load the data we need to speed up predictions
	 * 
	 * @param dataProvider
	 */
	public static synchronized void preloadData(BayesDataProvider dataProvider)
	{
		if (bayesData == null)
			bayesData = new BayesData(dataProvider);
	}
	
	public BayesRecommender(BayesDataProvider dataProvider) {
		this.dataProvider = dataProvider;
	}
	
	private Double getPr_u(long user,int rating)
	{
		if (bayesData != null)
			return bayesData.getPr_U(user, rating);
		else
			return dataProvider.getPrGivenU(user, rating);
	}
	
	private Double getPr_i(long content,int rating)
	{
		if (bayesData != null)
			return bayesData.getPr_I(content, rating);
		else
			return dataProvider.getPrGivenI(content, rating);
	}
	
	private Double getPr(int rating)
	{
		if (bayesData != null)
			return bayesData.getPr(rating);
		else
			return dataProvider.getPr(rating);
	}
	
	/**
	 * Provide a Mean Absolute Error minimized naive Baysian predcitor
	 * 
	 * @param user
	 * @param content
	 * @param minRating
	 * @param maxRating
	 * @return integer prediction in rating scale 
	 */
	public Integer MAEPredictIntRating(long user,long content,int minRating,int maxRating)
	{
		double mae = Double.MAX_VALUE;
		Integer bestRating = null;
		for(int i=minRating;i<=maxRating;i++)
		{
			double sum = 0;
			boolean sumValid = false;
			for(int j=minRating;j<=maxRating;j++)
			{
				Double pr_u = getPr_u(user,j);
				Double pr_i = getPr_i(content,j);
				Double pr = getPr(j);
				if (pr_u != null && pr_i != null && pr != null)
				{
					sumValid = true;
					sum = sum + (((pr_u * pr_i)/pr) * Math.abs(i-j));
				}
			}
			if (sumValid && sum < mae)
			{
				mae = sum;
				bestRating = i;
			}
		}
		return bestRating;
	}
	
	/**
	 * Provide a Mean Squared Error minimized naive Bayesian predictor
	 * 
	 * @param user
	 * @param content
	 * @param minRating
	 * @param maxRating
	 * @return integer rating between minRating and maxRating incl.
	 */
	public int MSEPredictIntRating(long user,long content,int minRating,int maxRating)
	{
		double prediction = 0;
		for(int i=minRating;i<maxRating;i++)
		{
			Double pr_u = getPr_u(user,i);
			Double pr_i = getPr_i(content,i);
			Double pr = getPr(i);
			if (pr_u != null && pr_i != null && pr != null)
				prediction = prediction + (i * ((pr_u * pr_i)/pr));
		}
		return (int) Math.round(prediction);
	}
}
