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
package io.seldon.client.algorithm;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by: marc on 25/11/2011 at 10:43
 */
public class AlgorithmOptions {

    private List<CFAlgorithm.CF_SORTER> sorters = new LinkedList<CFAlgorithm.CF_SORTER>();
    private List<CFAlgorithm.CF_PREDICTOR> predictors = new LinkedList<CFAlgorithm.CF_PREDICTOR>();
    private List<CFAlgorithm.CF_RECOMMENDER> recommenders = new LinkedList<CFAlgorithm.CF_RECOMMENDER>();
    private List<CFAlgorithm.CF_ITEM_COMPARATOR> itemComparators = new LinkedList<CFAlgorithm.CF_ITEM_COMPARATOR>();

    private CFAlgorithm.CF_STRATEGY sorterStrategy;
    private CFAlgorithm.CF_STRATEGY predictorStrategy;
    private CFAlgorithm.CF_STRATEGY recommenderStrategy;
    private CFAlgorithm.CF_STRATEGY itemComparatorStrategy;

    private CFAlgorithm.CF_POSTPROCESSING postprocessingType;

    private Double longTermClusterWeight = null;
    private Double shortTermClusterWeight = null;
    
    private final String optionSeparator = ",";
    private final String parameterSeparator = ":";
    private final String valueSeparator = "|";
    
    
    
    
    public List<CFAlgorithm.CF_SORTER> getSorters() {
		return sorters;
	}

	public List<CFAlgorithm.CF_PREDICTOR> getPredictors() {
		return predictors;
	}

	public List<CFAlgorithm.CF_RECOMMENDER> getRecommenders() {
		return recommenders;
	}

	public List<CFAlgorithm.CF_ITEM_COMPARATOR> getItemComparators() {
		return itemComparators;
	}

	public CFAlgorithm.CF_STRATEGY getSorterStrategy() {
		return sorterStrategy;
	}

	public CFAlgorithm.CF_STRATEGY getPredictorStrategy() {
		return predictorStrategy;
	}

	public CFAlgorithm.CF_STRATEGY getRecommenderStrategy() {
		return recommenderStrategy;
	}

	public CFAlgorithm.CF_STRATEGY getItemComparatorStrategy() {
		return itemComparatorStrategy;
	}

	public CFAlgorithm.CF_POSTPROCESSING getPostprocessingType() {
		return postprocessingType;
	}

	public Double getLongTermClusterWeight() {
		return longTermClusterWeight;
	}

	public Double getShortTermClusterWeight() {
		return shortTermClusterWeight;
	}

	public AlgorithmOptions withLongTermClusterWeight(double weight) {
    	this.longTermClusterWeight = weight;
    	return this;
    }
    
    public AlgorithmOptions withShortTermClusterWeight(double weight) {
    	this.shortTermClusterWeight = weight;
    	return this;
    }
    
    public AlgorithmOptions withSorter(CFAlgorithm.CF_SORTER sorter) {
        sorters.add(sorter);
        return this;
    }

    public AlgorithmOptions withSorterStrategy(CFAlgorithm.CF_STRATEGY strategy) {
        sorterStrategy = strategy;
        return this;
    }

    public AlgorithmOptions withPredictor(CFAlgorithm.CF_PREDICTOR predictor) {
        predictors.add(predictor);
        return this;
    }

    public AlgorithmOptions withPredictorStrategy(CFAlgorithm.CF_STRATEGY strategy) {
        predictorStrategy = strategy;
        return this;
    }

    public AlgorithmOptions withRecommender(CFAlgorithm.CF_RECOMMENDER recommender) {
        recommenders.add(recommender);
        return this;
    }

    public AlgorithmOptions withRecommenderStrategy(CFAlgorithm.CF_STRATEGY strategy) {
        recommenderStrategy = strategy;
        return this;
    }

    public AlgorithmOptions withItemComparator(CFAlgorithm.CF_ITEM_COMPARATOR comparator) {
        itemComparators.add(comparator);
        return this;
    }

    public AlgorithmOptions withItemComparatorStrategy(CFAlgorithm.CF_STRATEGY strategy) {
        itemComparatorStrategy = strategy;
        return this;
    }

    public AlgorithmOptions withPostprocessingType(CFAlgorithm.CF_POSTPROCESSING postprocessing) {
        postprocessingType = postprocessing;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(serialiseOptionList("sorters", sorters));
        stringBuilder.append(serialiseOptionList("item_comparators", itemComparators));
        stringBuilder.append(serialiseOptionList("recommenders", recommenders));
        stringBuilder.append(serialiseOptionList("predictors", predictors));
        stringBuilder.append(serialiseStrategy("sorter_strategy", sorterStrategy));
        stringBuilder.append(serialiseStrategy("recommender_strategy", recommenderStrategy));
        stringBuilder.append(serialiseStrategy("predictor_strategy", predictorStrategy));
        stringBuilder.append(serialiseStrategy("item_comparator_strategy", itemComparatorStrategy));
        stringBuilder.append(serialisePostprocessingType("postprocessing", postprocessingType));
        stringBuilder.append(serialiseNumericParameter("long_term_cluster_weight", this.longTermClusterWeight));
        stringBuilder.append(serialiseNumericParameter("short_term_cluster_weight", this.shortTermClusterWeight));
        if (stringBuilder.length() > 0) {
            // knock out the initial separator
            return stringBuilder.substring(1);
        } else {
            return "";
        }
    }

    private String serialiseStrategy(String strategyName, CFAlgorithm.CF_STRATEGY strategy) {
        return serialiseStrategyName(strategyName, (strategy == null) ? null : strategy.toString());
    }

    private String serialisePostprocessingType(String strategyName, CFAlgorithm.CF_POSTPROCESSING postprocessing) {
        return serialiseStrategyName(strategyName, (postprocessing == null) ? null : postprocessing.toString());
    }

    private String serialiseStrategyName(String strategyName, String strategy) {
        if (strategy != null) {
            return optionSeparator + strategyName + this.parameterSeparator + strategy;
        } else {
            return "";
        }
    }
    
    private String serialiseNumericParameter(String name,Double val)
    {
    	if (val != null)
    		return optionSeparator + name + this.parameterSeparator + val;
    	else
    		return "";
    }

    private <T> String serialiseOptionList(String optionName, List<T> options) {
        StringBuilder stringBuilder = new StringBuilder();
        if (options.size() > 0) {
            stringBuilder.append(optionSeparator);
            stringBuilder.append(optionName).append(this.parameterSeparator);
            for (T entry : options) {
                stringBuilder.append(entry);
                stringBuilder.append(this.valueSeparator);
            }
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        }
        return stringBuilder.toString();
    }

}
