/*
 * Seldon -- open source prediction engine
 * =======================================
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 **********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at       
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ********************************************************************************************** 
*/
package io.seldon.stream.itemsim;

import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import org.junit.Test;

public class JaccardSimilarityTest {

	@Test
	public void concurrencyTest()
	{
		final StreamingJaccardSimilarity j = new StreamingJaccardSimilarity(6, 100, 0); 
		
		Timer reloadTimer = new Timer(true);
		reloadTimer.scheduleAtFixedRate(new TimerTask() {
			   public void run()  
			   {
				   List<JaccardSimilarity> res = j.getSimilarity(System.currentTimeMillis()/1000);
				   System.out.println("Results size "+res.size());
				   for(int i=0;i<3;i++)
					   System.out.println(res.get(i));
				   
			   }
		   }, 2000, 2000);
		
		long t1 = System.currentTimeMillis()/1000;
		long endTime = t1+20;
		Random r = new Random();
		do
		{
			long t = System.currentTimeMillis()/1000;
			int itemId = r.nextInt(200);
			int userId = r.nextInt(9999);
			j.add(itemId, userId, t);
		}while((System.currentTimeMillis()/1000)<= endTime);
		
		reloadTimer.cancel();
	}
	
}
