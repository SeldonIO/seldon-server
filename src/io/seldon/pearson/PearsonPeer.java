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

import java.util.Set;

import javax.jdo.PersistenceManager;

import io.seldon.api.APIException;
import io.seldon.api.Util;
import io.seldon.general.Opinion;
import io.seldon.pearson.impl.jdo.PearsonSimilarityDBPeer;
import io.seldon.prediction.ContentRatingResolver;
import io.seldon.trust.impl.CFAlgorithm;
import io.seldon.trust.impl.TrustRecommendation;
import io.seldon.trust.impl.jdo.generic.ContentReviewPeer;

public class PearsonPeer {
	
	PersistenceManager pm;
	CFAlgorithm options;
	
	public PearsonPeer(PersistenceManager pm,CFAlgorithm options)
	{
		this.pm = pm;
		this.options = options;
	}
	
	public TrustRecommendation getPersonalisedRating(long user,long content,int type)
	{
		ContentReviewPeer cp = new ContentReviewPeer(pm);
		Opinion o = null;
		try {
			o = Util.getOpinionPeer(pm).getOpinion(content, user);
		} catch (APIException e) {}
		if (o!=null)
			return new TrustRecommendation(content,type,o.getValue(),null,user,null);
		else
		{
			PearsonSimilarityHandler simHandler = new PearsonSimilarityDBPeer(pm);
			Set<Long> N = simHandler.getNeighbourhood(content,user, 943);
			PearsonPredictor predictor = new PearsonPredictor(simHandler,(ContentRatingResolver)cp);
			TrustRecommendation prediction = predictor.ResnickPrediction(user, content, type, N, options.getMinRating(), options.getMaxRating(), false, false,0.7);
			if (prediction == null)
			{
				Double pred = cp.getAvgRating(user, type);
				if (pred == null)
				{
					pred =  options.getMaxRating()/2;
					prediction = new TrustRecommendation(content,type,pred,null,null,null);
				}
				else
					prediction = new TrustRecommendation(content,type,pred,pred,null,null);
				

			}
			return prediction;
		}
	}
}
