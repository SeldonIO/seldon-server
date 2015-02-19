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
import java.util.List;

public class RankedItem<T> implements Comparable<RankedItem<T>> {

	private T itemId;
	private Integer pos;
	
	public RankedItem(T itemId,Integer pos) {
		this.itemId = itemId;
		this.pos = pos;
	}

	
	
	public T getItemId() {
		return itemId;
	}

	public void setItemId(T itemId) {
		this.itemId = itemId;
	}

	public Integer getPos() {
		return pos;
	}

	public void setPos(Integer pos) {
		this.pos = pos;
	}
	
	public void add(RankedItem other)
	{
		this.pos = this.pos + other.pos;
	}

	@Override
	public int compareTo(RankedItem<T> item2) {
		if(this.pos < item2.getPos()) { return -1; }
		else if(this.pos > item2.getPos()) { return 1; }
		else return 0;
	}
	
	public static <K> List<RankedItem<K>> createFromList(List<K> list)
	{
		List<RankedItem<K>> res = new ArrayList<>();
		int pos = 1;
		for(K i : list)
			res.add(new RankedItem<>(i,pos++));
		return res;
	}

}
