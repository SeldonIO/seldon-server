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
import java.util.List;

/**
 * @author firemanphil
 *         Date: 07/05/15
 *         Time: 14:10
 */
public class ItemRecommendationsBean extends ResourceBean {

    List<ItemBean> recommendedItems = new ArrayList<>();
    String cohort ="-";

   public ItemRecommendationsBean(){}

    public ItemRecommendationsBean(List<ItemBean> recommendedItems, String cohort){
        this.recommendedItems = recommendedItems;
        this.cohort = cohort;
    }


    public List<ItemBean> getRecommendedItems() {
        return recommendedItems;
    }

    public void setRecommendedItems(List<ItemBean> recommendedItems) {
        this.recommendedItems = recommendedItems;
    }

    public String getCohort() {
        return cohort;
    }

    public void setCohort(String cohort) {
        this.cohort = cohort;
    }

    @Override
    public String toKey() {
        return String.valueOf(this.hashCode());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ItemRecommendationsBean)) return false;

        ItemRecommendationsBean that = (ItemRecommendationsBean) o;

        if (recommendedItems != null ? !recommendedItems.equals(that.recommendedItems) : that.recommendedItems != null)
            return false;
        return !(cohort != null ? !cohort.equals(that.cohort) : that.cohort != null);

    }

    @Override
    public int hashCode() {
        int result = recommendedItems != null ? recommendedItems.hashCode() : 0;
        result = 31 * result + (cohort != null ? cohort.hashCode() : 0);
        return result;
    }
}
