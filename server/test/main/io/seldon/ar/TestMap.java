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
package io.seldon.ar;

import io.seldon.recommendation.RecommendationUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Ignore;
import org.junit.Test;

public class TestMap {

	private void getStats()
	{
		final int mb = 1024*1024;
		//Getting the runtime reference from system
        Runtime runtime = Runtime.getRuntime();
         
        System.out.println("##### Heap utilization statistics [MB] #####");
         
        //Print used memory
        System.out.println("Used Memory:"
            + (runtime.totalMemory() - runtime.freeMemory()) / mb);
 
        //Print free memory
        System.out.println("Free Memory:"
            + runtime.freeMemory() / mb);
         
        //Print total available memory
        System.out.println("Total Memory:" + runtime.totalMemory() / mb);
 
        //Print Maximum available memory
        System.out.println("Max Memory:" + runtime.maxMemory() / mb);
	}
	
	public Map<Long,Double> getTopK(Map<Long,Double> map,int k)
	{
		ValueComparator bvc =  new ValueComparator(map);
        TreeMap<Long,Double> sorted_map = new TreeMap<Long,Double>(bvc);
        sorted_map.putAll(map);
        int i = 0;
        /*
        Map<String,Double> r = new HashMap<>(k);
        for(Map.Entry<String, Double> e : sorted_map.entrySet())
        {
        	if (++i > k)
        		break;
        	else
        		r.put(e.getKey(), e.getValue());
        }
        return r;
        */
        return sorted_map;
	}
	
	public <T extends Comparable<? super T>> List<List<T>> binPowSet(
			List<T> A){
		List<List<T>> ans= new ArrayList<List<T>>();
		int ansSize = (int)Math.pow(2, A.size());
		for(int i= 0;i< ansSize;++i){
			String bin= Integer.toBinaryString(i); //convert to binary
			while(bin.length() < A.size()) bin = "0" + bin; //pad with 0's
			ArrayList<T> thisComb = new ArrayList<T>(); //place to put one combination
			for(int j= 0;j< A.size();++j){
				if(bin.charAt(j) == '1')thisComb.add(A.get(j));
			}
			Collections.sort(thisComb); //sort it for easy checking
			ans.add(thisComb); //put this set in the answer list
		}
		return ans;
	}
	
	@Test @Ignore
	public void testPowerSet()
	{
		System.gc();
		getStats();
		long start = System.currentTimeMillis();
		for(int i=0;i<1000000;i++)
		{
			List<Integer> l = new ArrayList<Integer>();
			l.add(1); l.add(2); l.add(3); l.add(4);
			List<List<Integer>> p = binPowSet(l);
		}
		long end = System.currentTimeMillis();
		getStats();
		System.out.println("Time "+(end-start));
		System.gc();
		getStats();

	}
	
	@Test
	public void test()
	{
		System.gc();
		getStats();
		long start = System.currentTimeMillis();
		for(int i=0;i<1000000;i++)
		{
			HashMap<Long,Double> map = new HashMap<Long,Double>();
			map.put(1L,99.5);
			map.put(2L,67.4);
			map.put(3L,100.3);
			map.put(4L,65.3);
			//Map<Long,Double> sorted_map = getTopK(map, 2);
			Map<Long,Double> sorted_map = RecommendationUtils.rescaleScoresToOne(map, 2);
		}
		long end = System.currentTimeMillis();
		getStats();
		System.out.println("Time "+(end-start));
		System.gc();
		getStats();
	}
	
	public static class ValueComparator implements Comparator<Long> {

	    Map<Long, Double> base;
	    public ValueComparator(Map<Long, Double> base) {
	        this.base = base;
	    }

	    // Note: this comparator imposes orderings that are inconsistent with equals.    
	    public int compare(Long a, Long b) {
	        if (base.get(a) >= base.get(b)) {
	            return -1;
	        } else {
	            return 1;
	        } // returning 0 would merge keys
	    }
	}
}
