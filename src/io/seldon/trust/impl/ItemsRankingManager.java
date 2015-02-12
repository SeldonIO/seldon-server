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

package io.seldon.trust.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

import java.util.concurrent.ConcurrentMap;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;

/**
 * Manager for the global ranked item lists.
 * A list with items ranked by hits and a list with items ranked by date will be stored.
 * These lists will be used in conjunction with the relevance item list (specific for each user)
 * to obtain the final result.
 *
 * A possible improvement would be to cache the itemsByHits and itemByDate output for a given input
 *
 * @author rummble
 *
 */

public class ItemsRankingManager {

	private static Logger logger = Logger.getLogger(ItemsRankingManager.class.getName());
	
	private final static int TIME_WEIGHT = 1;
	private final static int HIT_WEIGHT = 4;
	private final static int RELEVANCE_WEIGHT = 4;
	private final static int MAP_SIZE = 1000;
	
	
	//Popularity Items Map
	private final static ConcurrentMap<String,ConcurrentMap<Long,MutableInt>> itemsByHits = new ConcurrentHashMap<String,ConcurrentMap<Long,MutableInt>>();
	//Recent Items Map
	private final static ConcurrentMap<String,ConcurrentMap<Long,Long>> itemsByDate = new ConcurrentHashMap<String,ConcurrentMap<Long, Long>>();
	private static ItemsRankingManager instance = null;
	
	private ItemsRankingManager() {};
	
	public static ItemsRankingManager getInstance() {
		if(instance == null) { instance = new ItemsRankingManager(); }
		return instance;
	}
	
	//ItemByHits
	private ConcurrentMap<Long,MutableInt> itemsByHits(String consumer) {
		ConcurrentMap<Long,MutableInt> map = itemsByHits.get(consumer);
		if(map == null) {
			map = new ConcurrentLinkedHashMap.Builder<Long,MutableInt>().maximumWeightedCapacity(MAP_SIZE).build();
			itemsByHits.putIfAbsent(consumer, map);
			map = itemsByHits.get(consumer);
		}
		return map;
	}
	
	//ItemsByDate
	private ConcurrentMap<Long,Long> itemsByDate(String consumer) {
		ConcurrentMap<Long,Long> map = itemsByDate.get(consumer);
		if(map == null) {
			map = new ConcurrentLinkedHashMap.Builder<Long,Long>().maximumWeightedCapacity(MAP_SIZE).build();
			itemsByDate.putIfAbsent(consumer, map);
			map = itemsByDate.get(consumer);
		}
		return map;
	}
	
	//to call when a new Item has been added
	public void addItem(String consumer, Long item, Date date) {
		itemsByHits(consumer).putIfAbsent(item, new MutableInt());
		if(date == null) { date = new Date(); }
		itemsByDate(consumer).putIfAbsent(item, date.getTime());
	}
	
	//to call when an action is executed
	public void Hit(String consumer, Long itemId) {
		try {
			//if the addItem is not called
			itemsByDate(consumer).putIfAbsent(itemId, new Date().getTime());
			itemsByHits(consumer).get(itemId).inc();
		}
		//if the addItem method is called before this exception should not occur
		catch(Exception e) {
			MutableInt one = new MutableInt();
			one.inc();
			itemsByHits(consumer).putIfAbsent(itemId, one);
		}
	}
	
	//sort the input by popularity
	public List<RankedItem<Long>> getRankedItemsByHits(String consumer, List<Long> items) {
		List<ScoreItem> list = new ArrayList<ScoreItem>();
		List<RankedItem<Long>> result = new ArrayList<RankedItem<Long>>();
		if(items != null) {
			for(Long item : items) {
				long hits;
				try {
					hits = itemsByHits(consumer).get(item).get();
					logger.debug("MostPopular item:"+item+" hits:"+hits);
				}
				catch(Exception e) {
					hits = 0;
				}
				list.add(new ScoreItem(item,hits));
			}
			Collections.sort(list);
			int pos=1;
			for(ScoreItem item : list) {
				result.add(new RankedItem(item.getItemId(),pos++));
			}
		}
		return result;
	}
	
	//sort the input by date
	public List<RankedItem<Long>> getRankedItemsByDate(String consumer, List<Long> items) {
		List<ScoreItem> list = new ArrayList<ScoreItem>();
		List<RankedItem<Long>> result = new ArrayList<RankedItem<Long>>();
		if(items != null) {
			for(Long item : items) {
				long time;
				try {
					time = itemsByDate(consumer).get(item);
				}
				catch(Exception e) {
					time = 0;
				}
				list.add(new ScoreItem(item,time));
			}
			Collections.sort(list);
			int pos = 1;
			for(ScoreItem item : list) {
				result.add(new RankedItem<Long>(item.getItemId(),pos++));
			}
		}
		return result;
	}
	
	//return the most recent items
	public List<Long> getItemsSortedByDate(String consumer, int limit) {
		ConcurrentMap<Long,Long> items = itemsByDate(consumer);
		List<ScoreItem> list = new ArrayList<ScoreItem>();
		if(items == null || items.isEmpty()) { return null; }
		for(Long item : items.keySet()) { list.add(new ScoreItem(item,items.get(item))); }
		Collections.sort(list);
		if(list.size() > limit) { list = list.subList(0, limit); }
		return scoreListToItems(list);
	}
	
	//return the most popular items
	public List<Long> getItemsSortedByPopularity(String consumer, int limit) {
		ConcurrentMap<Long,MutableInt> items = itemsByHits(consumer);
		List<ScoreItem> list = new ArrayList<ScoreItem>();
		if(items == null || items.isEmpty()) { return null; }
		for(Long item : items.keySet()) { list.add(new ScoreItem(item,(long)items.get(item).get())); }
		Collections.sort(list);
		if(list.size() > limit) { list = list.subList(0, limit); }
		return scoreListToItems(list);
	}
	
	
	//combine the popularity, relevance and time factors to rank the input item set
	public List<RankedItem<Long>> getCombinedList(List<Long> itemsToRank, List<RankedItem<Long>> itemsByHits, List<RankedItem<Long>> itemsByDate, List<RankedItem<Long>> itemsByRelevance) {
		HashMap<Long,Integer> items = new HashMap<Long,Integer>();
		List<RankedItem<Long>> rankedItems = new ArrayList<RankedItem<Long>>();
		if(itemsToRank == null || itemsToRank.isEmpty()) { return rankedItems; }
		int ndate = 0;
		int nhits = 0;
		int nrelevance = 0;
		if(itemsByDate != null && !itemsByDate.isEmpty()) { ndate = itemsByDate.size() * TIME_WEIGHT; }
		if(itemsByHits != null && !itemsByHits.isEmpty()) { nhits = itemsByHits.size() * HIT_WEIGHT; }
		if(itemsByRelevance != null && !itemsByRelevance.isEmpty()) { nrelevance = itemsByRelevance.size() * RELEVANCE_WEIGHT; }
		int ntot = ndate + nhits + nrelevance;
		//init
		for(Long item : itemsToRank) {
			items.put(item, ntot);
		}
		//itemsByDate
		if(itemsByDate != null) {
			for(RankedItem<Long> item : itemsByDate) {
				try {
					int pos = items.get(item.getItemId());
					items.put(item.getItemId(), pos - (ndate - (item.getPos() * TIME_WEIGHT)));
				}
				catch(Exception e) { //item skipped since not in the itemsToRank list
				}
			}
		}
		//itemsByHits
		if(itemsByHits != null) {
			for(RankedItem<Long> item : itemsByHits) {
				try {
					int pos = items.get(item.getItemId());
					items.put(item.getItemId(), pos - (nhits - (item.getPos() * HIT_WEIGHT)));
				}
				catch(Exception e) { //item skipped since not in the itemsToRank list
				}
			}
		}
		//itemsByRelevance
		if(itemsByRelevance != null) {
			for(RankedItem<Long> item : itemsByRelevance) {
				try {
					int pos = items.get(item.getItemId());
					items.put(item.getItemId(), pos - (nrelevance - (item.getPos() * RELEVANCE_WEIGHT)));
				}
				catch(Exception e) { //item skipped since not in the itemsToRank list
				}
			}
		}
		//rebuild the list
		for(Long item : items.keySet()) {
			rankedItems.add(new RankedItem(item,items.get(item)));
		}
		
		Collections.sort(rankedItems);
		return rankedItems;
	}
	
	public List<RankedItem<Long>> getCombinedRankedList(String consumer, List<Long> itemsToRank, List<Long> itemsByRelevance) {
		Date start = new Date();
		if(itemsToRank == null || itemsToRank.isEmpty()) { return new ArrayList<RankedItem<Long>>(); }
		List<RankedItem<Long>> listByHits = getRankedItemsByHits(consumer, itemsToRank);
		List<RankedItem<Long>> listByDate = getRankedItemsByDate(consumer, itemsToRank);
		List<RankedItem<Long>> res = getCombinedList(itemsToRank, listByHits, itemsToRankedList(itemsByRelevance), listByDate);
		logger.info("ItemRankingManager.getCombinedList: " + ( new Date().getTime() - start.getTime()) + " ms.");
		return res;
	}
	
	public List<RankedItem<Long>> getCombinedRankedListWithHits(String consumer, List<Long> itemsToRank, List<Long> itemsByRelevance) {
		Date start = new Date();
		if(itemsToRank == null || itemsToRank.isEmpty()) { return new ArrayList<RankedItem<Long>>(); }
		List<RankedItem<Long>> listByHits = getRankedItemsByHits(consumer, itemsToRank);
		List<RankedItem<Long>> res = getCombinedList(itemsToRank, listByHits, itemsToRankedList(itemsByRelevance), null);
		logger.info("ItemRankingManager.getCombinedList: " + ( new Date().getTime() - start.getTime()) + " ms.");
		return res;
	}
	
	public List<Long> getItemsByHits(String consumer, List<Long> itemsToRank) {
		return rankListToItems(getRankedItemsByHits(consumer,itemsToRank));
	}
	
	public List<Long> getItemsByDate(String consumer, List<Long> itemsToRank) {
		return rankListToItems(getRankedItemsByDate(consumer,itemsToRank));
	}
	
	public List<Long> getCombinedList(String consumer, List<Long> itemsToRank, List<Long> itemsByRelevance) {
		return rankListToItems(getCombinedRankedList(consumer,itemsToRank, itemsByRelevance));
	}
	
	public List<Long> getCombinedListWithHits(String consumer, List<Long> itemsToRank, List<Long> itemsByRelevance) {
		return rankListToItems(getCombinedRankedListWithHits(consumer,itemsToRank, itemsByRelevance));
	}
	
	public List<Long> rankListToItems(List<RankedItem<Long>> items) {
		List<Long> res = new ArrayList<Long>();
		if(items == null) { return res; }
		for(RankedItem<Long> i : items) { res.add(i.getItemId()); }
		return res;
	}
	
	public List<Long> scoreListToItems(List<ScoreItem> items) {
		List<Long> res = new ArrayList<Long>();
		if(items == null) { return res; }
		for(ScoreItem i : items) { res.add(i.getItemId()); }
		return res;
	}
	
	public List<RankedItem<Long>> itemsToRankedList(List<Long> items) {
		List<RankedItem<Long>> res = new ArrayList<RankedItem<Long>>();
		if(items == null) { return res; }
		int pos = 1;
		for(Long i : items) { res.add(new RankedItem(i,pos++)); }
		return res;
	}
}

class MutableInt {
	  int value = 0;
	  public void inc () { ++value; }
	  public int get () { return value; }
}
