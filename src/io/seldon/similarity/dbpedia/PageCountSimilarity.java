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

package io.seldon.similarity.dbpedia;

/**
 * Provide various distributional semantic similarity algorithms based on web search engine hits
 * @author rummble
 *
 */
public class PageCountSimilarity {


	/**
	 * As discussed in Danushka Bollegala, Yutaka Matsuo, Mitsuru Ishizuka 
	 * "Measuring semantic similarity between words using web search engines" 2007
	 * @param hitsA
	 * @param hitsB
	 * @param hitsAB
	 * @return a value between 0 and 1 - where 1 is perfect similarity
	 */
	public static double getJaccardSimilarity(int hitsA,int hitsB,int hitsAB)
	{
		if (hitsAB < 5 || hitsB == 0 || hitsA == 0 || hitsAB == 0)
			return 0;
		else
			return hitsAB/(double)(hitsA+hitsB-hitsAB);
	}
	
	/**
	 * Pointwise mutual information similarity. As discussed in Danushka Bollegala, Yutaka Matsuo, Mitsuru Ishizuka 
	 * "Measuring semantic similarity between words using web search engines" 2007
	 * @param hitsA
	 * @param hitsB
	 * @param hitsAB
	 * @param numDocs
	 * @return a value between 0 and 1 - where 1 is perfect similarity
	 */
	public static double getPMISimilarity(int hitsA,int hitsB,int hitsAB,int numDocs)
	{
		if (hitsAB < 5 || hitsB == 0 || hitsA == 0 || hitsAB == 0)
			return 0;
		else
		{
			float aVal = hitsA/(float)numDocs;
			float bVal = hitsB/(float)numDocs;
			float abVal = hitsAB/(float)numDocs;
			return log2(abVal/(aVal*bVal));
		}
	}
	
	private static double log2(double num)
	{
		return (Math.log(num)/Math.log(2));
	} 
	
	
	/**
	 * As discussed in Rudi L. Cilibrasi and Paul M.B. Vitanyi, "The Google Similarity Distance"
	 * @param hitsA
	 * @param hitsB
	 * @param hitsAB
	 * @param numDocs
	 * @return a value from 0 to infinity
	 */
	public static double getGoogleDistance(int hitsA,int hitsB,int hitsAB,int numDocs)
	{
		if (hitsA == 0 || hitsB == 0 || hitsAB == 0)
			return Double.MAX_VALUE;
		double logA = Math.log(hitsA);
		double logB = Math.log(hitsB);
		double logAB = Math.log(hitsAB);
		double n = Math.max(logA, logB) - logAB;
		double d = Math.log(numDocs) - Math.min(logA, logB);
		return n/d;
	}
	
	
}
