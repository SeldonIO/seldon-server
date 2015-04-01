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
package io.seldon.client.beans;

import java.util.ArrayList;
import java.util.List;

public class ItemsBean extends ListBean {
    private static final long serialVersionUID = -6608037593819056647L;

    protected List<ItemBean> list = new ArrayList<ItemBean>();

	public List<ItemBean> getList() {
		return list;
	}

	public void setList(List<ItemBean> list) {
		this.list = list;
	}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ItemsBean)) return false;

        ItemsBean itemsBean = (ItemsBean) o;

        return !(list != null ? !list.equals(itemsBean.list) : itemsBean.list != null);
    }

    @Override
    public int hashCode() {
        return list != null ? list.hashCode() : 0;
    }
}
