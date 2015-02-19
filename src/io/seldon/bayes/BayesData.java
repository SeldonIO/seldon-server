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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Provide a bean to store all naive bayes data to imporve algorithm speed
 * @author Clive
 *
 */
public class BayesData {

	Map<String,Double> pruMap = new ConcurrentHashMap<>();
	Map<String,Double> priMap = new ConcurrentHashMap<>();
	Map<Integer,Double> prMap = new ConcurrentHashMap<>();
	
	public BayesData(BayesDataProvider provider)
	{
		provider.fillPrGivenU(pruMap);
		provider.fillPrGivenI(priMap);
		provider.fillPr(prMap);
	}
	
	Double getPr_U(long user,int rating)
	{
		return pruMap.get(""+user+":"+rating);
	}
	
	Double getPr_I(long content,int rating)
	{
		return priMap.get(""+content+":"+rating);
	}
	
	Double getPr(int rating)
	{
		return prMap.get(rating);
	}
	
}
