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

package io.seldon.test.prediction;

import java.util.HashMap;
import java.util.Map;

import io.seldon.prediction.ContentRatingResolver;
import io.seldon.trust.impl.Recommendation;
import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import io.seldon.prediction.PersonalisedRatingCreator;
import io.seldon.trust.impl.Trust;

public class WeightedMedianTest extends BasePredictionTest {

	
	@Before
    public void setup() {
		
    }
	
	@Test
	public void singleRating()
	{
		Map<Long,Double> weights = new HashMap<>();
		weights.put(1L, 0.5); // just one weight
		PersonalisedRatingCreator prc = new PersonalisedRatingCreator(new WMDummyResolver());
		Recommendation rec = prc.weightedMedianPrediction(1L, 1L, Trust.TYPE_GENERAL, weights, 0.0);
		Assert.assertNotNull(rec);
		Assert.assertEquals(rec.getPrediction(), 1.0D);
	}
	
	
	@Test
	public void twoRatings()
	{
		Map<Long,Double> weights = new HashMap<>();
		weights.put(1L, 0.5); 
		weights.put(2L, 0.5); 
		PersonalisedRatingCreator prc = new PersonalisedRatingCreator(new WMDummyResolver());
		Recommendation rec = prc.weightedMedianPrediction(1L, 1L, Trust.TYPE_GENERAL, weights, 0.0);
		Assert.assertNotNull(rec);
		Assert.assertEquals(rec.getPrediction(), 1.0D);
	}
	
	
	public static class WMDummyResolver implements ContentRatingResolver {

		@Override
		public Double getAvgContentRating(long c, int type) {
			return 1D;
		}

		@Override
		public Double getAvgRating(long m, int type) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Double getAvgRating(long m1, long m2, int type) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public int getNumberSharedOpinions(long u1, long u2, int type) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public Double getRating(long m, long contentId) {
			return 1.0D;
		}

		@Override
		public void setResolveStrategy(ResolveStrategy strategy) {
			// TODO Auto-generated method stub
			
		}

	}
	
}
