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

package io.seldon.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import io.seldon.trust.impl.RankedItem;

public class CollectionTools {

	public static <K, V extends Comparable<V>> List<K> sortMapAndLimitToList(Map<K, V> map,int k)
	{
		return sortMapAndLimitToList(map,k,true);
	}
	
	public static <K> Double avergeMapValue(Map<K,Double> map)
	{
		if (map.size() > 0)
		{
			double sum = 0;
			for(Double d : map.values())
				sum = sum + d;
			return sum/map.size();
		}
		else
			return null;
	}
	
	public static <K, V extends Comparable<V>> List<RankedItem<K>> sortMapAndLimitToRankedList(Map<K, V> map,int k,boolean keepHighest) {
		List<Entry<K, V>> sorted = sortByValue(map);
		if (keepHighest)
			Collections.reverse(sorted);
		List<RankedItem<K>> res = new ArrayList<>();
		int count = 0;
		int rank = 1;
		V bestScore = null;
		for(Map.Entry<K, V> e : sorted)
		{
			if (count>=k)
				break;
			else
			{
				if (bestScore == null)
					bestScore = e.getValue();
				if (!bestScore.equals(e.getValue()))
					rank = count+1;
				res.add(new RankedItem<>(e.getKey(),rank));
			}
			count++;
		}
		return res;
	}
	
	/**
	 * Generic method to sort a map by value and then return the top k keys
	 * @param <K>
	 * @param <V>
	 * @param map
	 * @param k
	 * @return
	 */
	public static <K, V extends Comparable<V>> List<K> sortMapAndLimitToList(Map<K, V> map,int k,boolean keepHighest) {
		List<Entry<K, V>> sorted = sortByValue(map);
		if (keepHighest)
			Collections.reverse(sorted);
		List<K> res = new ArrayList<>();
		int count = 0;
		for(Map.Entry<K, V> e : sorted)
		{
			if (count>=k)
				break;
			else
				res.add(e.getKey());
			count++;
		}
		return res;
	}
	public static <K, V extends Comparable<V>> Map<K,V> sortMapAndLimit(Map<K, V> map,int k) {
		return sortMapAndLimit(map,k,true);
	}
	
	public static <K, V extends Comparable<V>> Map<K,V> sortMapAndLimit(Map<K, V> map,int k,boolean keepHighest) {
		List<Entry<K, V>> sorted = sortByValue(map);
		if (keepHighest)
			Collections.reverse(sorted);
		int count = 0;
		Map<K,V> res = new HashMap<>();
		for(Map.Entry<K, V> e : sorted)
		{
			if (count>=k)
				break;
			else
				res.put(e.getKey(),e.getValue());
			count++;
		}
		return res;
	}
	
	/**
	 * Generic method to sort a map by value
	 * @param <K>
	 * @param <V>
	 * @param map
	 * @return
	 */
	public static <K, V extends Comparable<V>> List<Entry<K, V>> sortByValue(Map<K, V> map) {
        List<Entry<K, V>> entries = new ArrayList<>(map.entrySet());
        Collections.sort(entries, new ByValue<K, V>());
        return entries;
	}

	private static class ByValue<K, V extends Comparable<V>> implements Comparator<Entry<K, V>> {
        public int compare(Entry<K, V> o1, Entry<K, V> o2) {
                return o1.getValue().compareTo(o2.getValue());
        }
	}
	
	public static String join( int[] list, String delim )
    {
    return join( list, delim, false );
    }

  public static String join( int[] list, String delim, boolean printNull )
    {
    StringBuffer buffer = new StringBuffer();
    int count = 0;

    for( Object s : list )
      {
      if( count != 0 )
        buffer.append( delim );

      if( printNull || s != null )
        buffer.append( s );

      count++;
      }

    return buffer.toString();
    }

  public static String join( String delim, String... strings )
    {
    return join( delim, false, strings );
    }

  public static String join( String delim, boolean printNull, String... strings )
    {
    return join( strings, delim, printNull );
    }

  /**
   * This method joins the values in the given list with the delim String value.
   *
   * @param list
   * @param delim
   * @return a String
   */
  public static String join( Object[] list, String delim )
    {
    return join( list, delim, false );
    }

  public static String join( Object[] list, String delim, boolean printNull )
    {
    StringBuffer buffer = new StringBuffer();
    int count = 0;

    for( Object s : list )
      {
      if( count != 0 )
        buffer.append( delim );

      if( printNull || s != null )
        buffer.append( s );

      count++;
      }

    return buffer.toString();
    }

  /**
   * This method joins each value in the collection with a tab character as the delimiter.
   *
   * @param collection
   * @return a String
   */
  public static String join( Collection collection )
    {
    return join( collection, "\t" );
    }

  /**
   * This method joins each valuein the collection with the given delimiter.
   *
   * @param collection
   * @param delim
   * @return a String
   */
  public static String join( Collection collection, String delim )
    {
    return join( collection, delim, false );
    }

  public static String join( Collection collection, String delim, boolean printNull )
    {
    StringBuffer buffer = new StringBuffer();

    join( buffer, collection, delim, printNull );

    return buffer.toString();
    }

  /**
   * This method joins each value in the collection with the given delimiter. All results are appended to the
   * given {@link StringBuffer} instance.
   *
   * @param buffer
   * @param collection
   * @param delim
   */
  public static void join( StringBuffer buffer, Collection collection, String delim )
    {
    join( buffer, collection, delim, false );
    }

  public static void join( StringBuffer buffer, Collection collection, String delim, boolean printNull )
    {
    int count = 0;

    for( Object s : collection )
      {
      if( count != 0 )
        buffer.append( delim );

      if( printNull || s != null )
        buffer.append( s );

      count++;
      }
    }
}
