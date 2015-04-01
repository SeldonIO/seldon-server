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

public class DimensionsBean extends ListBean {
    private static final long serialVersionUID = 6224086063138214207L;

    protected List<DimensionBean> list = new ArrayList<DimensionBean>();

	public List<DimensionBean> getList() {
		return list;
	}

	public void setList(List<DimensionBean> list) {
		this.list = list;
	}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DimensionsBean)) return false;

        DimensionsBean that = (DimensionsBean) o;

        return !(list != null ? !list.equals(that.list) : that.list != null);
    }

    @Override
    public int hashCode() {
        return list != null ? list.hashCode() : 0;
    }
}
