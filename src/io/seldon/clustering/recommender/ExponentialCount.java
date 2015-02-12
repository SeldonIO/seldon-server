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

package io.seldon.clustering.recommender;

import java.io.Serializable;

import org.apache.log4j.Logger;

/**
 * Based on mahout @link https://issues.apache.org/jira/browse/MAHOUT-634
 * @author rummble
 *
 */
public class ExponentialCount implements Serializable {

	private static Logger logger = Logger.getLogger(ExponentialCount.class.getName());
	
	double alpha;
	double count;
	long lastT;
	
	public ExponentialCount(double alpha,double initialValue,long time)
	{
		this.alpha = alpha;
		this.count = initialValue;
		this.lastT = time;
	}
	
	public synchronized double increment(double incr,long time)
	{
		if (time < lastT)
			time = lastT;
		double pi = Math.exp(-(time - lastT) / alpha);
	    count = incr + pi * count;
	    lastT = time;
	    return count;
	}
	
	public synchronized double get(long time)
	{
		return increment(0,time);
	}
	
	public static void main(String[] args)
	{
		long time = System.currentTimeMillis()/1000;
		ExponentialCount c = new ExponentialCount(7200,1,time);
		for(int i=0;i<7200;i++)
			c.increment(1, time);
		double val = c.get(time);
		System.out.println("count is "+val);
	}
	
}
