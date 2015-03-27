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

package io.seldon.clustering.recommender;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Results from an @ItemRecommendationAlgorithm recommendation call.
 *
 * @author firemanphil
 *         Date: 09/10/2014
 *         Time: 15:10
 */
public class ItemRecommendationResultSet implements Serializable {

    private final List<ItemRecommendationResult> results;
    private final String recommenderName;

    public ItemRecommendationResultSet(String recommenderName){
        this.recommenderName = recommenderName;
        this.results = Collections.emptyList();
    }

    public ItemRecommendationResultSet(List<ItemRecommendationResult> results, String recommenderName) {
        this.recommenderName = recommenderName;
        if(results==null) results = Collections.emptyList();
        this.results = results;
    }

    public List<ItemRecommendationResult> getResults(){
        return new ArrayList<>(results);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ItemRecommendationResultSet)) return false;

        ItemRecommendationResultSet that = (ItemRecommendationResultSet) o;

        if (!results.equals(that.results)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return results.hashCode();
    }

    public String getRecommenderName() {
        return recommenderName;
    }

    public static class ItemRecommendationResult implements Comparable<ItemRecommendationResult>,Serializable {
        public ItemRecommendationResult(Long item, Float score) {
            this.item = item;
            this.score = score;
        }

        @Override
        public int compareTo(ItemRecommendationResult o) {
            return this.score.compareTo(o.score);
        }

        public final Long item;
        public final Float score;

        @Override
        public int hashCode(){
            return item.intValue();
        }

        @Override
        public boolean equals(Object other){
            if(other == null)
                return false;
            if(other instanceof ItemRecommendationResult){
                ItemRecommendationResult otherResult = (ItemRecommendationResult) other;
                if (otherResult.item ==null || item == null){
                    return item == otherResult.item;
                }
                return otherResult.item.equals(item);
            }
            return false;
        }

        @Override
        public String toString(){
            return "{item:"+item+",score:"+score+"}";
        }
    }
}
