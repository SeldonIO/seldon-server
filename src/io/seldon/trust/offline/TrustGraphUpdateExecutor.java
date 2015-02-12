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

package io.seldon.trust.offline;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import io.seldon.db.jdo.JDOFactory;

public class TrustGraphUpdateExecutor {

	private static Logger logger = Logger.getLogger( TrustGraphUpdateExecutor.class.getName() );
	
	private static TrustGraphUpdateExecutor theExecutor;
	private final ThreadPoolExecutor executor;
	private boolean failing = false;
	public static void initialise()
	{
		theExecutor = new TrustGraphUpdateExecutor();
	}
	
	private TrustGraphUpdateExecutor()
	{
		// NB: LinkedBlockingQueue has no bounds but will force only corePoolSize
		// Threads to be run. So MaxPoolSize is ignored.
		//BlockingQueue channel = new LinkedBlockingQueue();
		
		// Fixed length queue
		BlockingQueue<Runnable> channel = new ArrayBlockingQueue<Runnable>(1000);
		 executor = new ThreadPoolExecutor(2,4,60,TimeUnit.SECONDS,channel)
		 {
			 protected void beforeExecute(Thread t, Runnable r) {
			     super.beforeExecute(t, r);
			     /*
			     if (r instanceof RummbleTweet)
			     {
			    	 RummbleTweet rt = (RummbleTweet) r;
			    	 logger.info("Starting  " + rt.getId() + " " + rt.getTweet());
			     }
			     */
			     
			 }
			 
			 protected void afterExecute(Runnable r,Throwable t) 
			 {
				 super.afterExecute(r,t);
				 if (t != null)
				 {
					logger.error("Caught exception running trust update:",t);
				 }
				 JDOFactory.cleanupPM();
			 }
		 };
	}
	
	public static TrustGraphUpdateExecutor getInstance()
	{
		return theExecutor;
	}
	
	public void queueTrustUpdate(TrustUpdateJob job)
	{
		logger.info("Queue size is " + executor.getQueue().size());
		try
		{
			executor.execute(job);
			failing = false;
		}
		catch (RejectedExecutionException ex)
		{
			if (!failing)
			{
				logger.warn("Tweet execution rejected. Queue size is " + executor.getQueue().size());
				failing = true;
			}
		}	
	}
	
}
