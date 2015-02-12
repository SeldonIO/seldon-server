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

package io.seldon.api.resource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ListBean extends ResourceBean {
	
	protected long size;
	protected long requested;
	protected List<ResourceBean> list;
	
	public ListBean() {
		list = new ArrayList<ResourceBean>();
	}
	
	public long getSize() {
		return size;
	}
	
	public void setSize(long size) {
		this.size = size;
	}
	
	public long getRequested() {
		return requested;
	}
	
	public void setRequested(long requested) {
		this.requested = requested;
	}

	public List<ResourceBean> getList() {
		return list;
	}

	public void setList(List<ResourceBean> list) {
		this.list = list;
		if(list == null) { size = 0; }
		else { size = list.size(); }
	}

	public void addBean(ResourceBean bean) {
		list.add(bean);
		size++;
	}

    public <T extends ResourceBean> void  addAll(List<T> list){
        this.list.addAll(list);
        size += list.size();
    }
	
	public void sort()
	{
		Collections.sort(list);
	}

	@Override
	public String toKey() {
		return this.hashCode() + "";
	}
	
	public ResourceBean getElement(String key) {
		for(ResourceBean b : list) {
			if(b.toKey().equals(key)) { return b; }
		}
		return null;
	}
}
