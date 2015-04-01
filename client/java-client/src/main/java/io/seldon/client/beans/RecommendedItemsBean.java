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

/**
 * Created by: marc on 30/10/2012 at 17:13
 */
public class RecommendedItemsBean extends GenericListBean<ItemBean> {
    private static final long serialVersionUID = 7159749397498904201L;

    @Override
    public String toString() {
        return "RecommendedItemsBean{" +
                "list=" + list +
                ", size=" + size +
                ", requested=" + requested +
                "} " + super.toString();
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) return true;
        if (!(o instanceof RecommendedItemsBean)) return false;
        if (!super.equals(o)) return false;

        RecommendedItemsBean that = (RecommendedItemsBean) o;

        return !(list != null ? !list.equals(that.list) : that.list != null);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (list != null ? list.hashCode() : 0);
        return result;
    }
}
