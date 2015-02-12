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

public class WeightedRating implements Comparable<WeightedRating> {

	public long user;
	public double weight;
	public double rating;
	
	public WeightedRating(long user,double weight, double rating) {
		this.user = user;
		this.weight = weight;
		this.rating = rating;
	}


	@Override
	public int compareTo(WeightedRating o) {
		if (this.rating > o.rating)
			return 1;
		else if (this.rating < o.rating)
			return -1;
		else
			return 0;
	}
	
}
	
