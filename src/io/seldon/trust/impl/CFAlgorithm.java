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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import io.seldon.sv.SemanticVectorsManager;

/**
 * @author rummble
 *
 */
public class CFAlgorithm implements Cloneable,Serializable {




    public enum CF_RECOMMENDER {
		RELEVANCE, 
		TRUST_ITEMBASED,
		BEST_PREDICTION, 
		SEMANTIC_VECTORS_USER_KNN, 
		SEMANTIC_VECTORS_ITEM_KNN,
		MAHOUT_ITEMBASED, 
		GRAPHLAB_PMF, 
		MAHOUT_ALS, 
		MAHOUT_FPGROWTH,
		MOST_POPULAR,
		MOST_POPULAR_ITEM_CATEGORY,
		CLUSTER_COUNTS,
		CLUSTER_COUNTS_SIGNIFICANT,		
		CLUSTER_COUNTS_DYNAMIC,
		CLUSTER_COUNTS_GLOBAL,
		CLUSTER_COUNTS_ITEM_CATEGORY,
		SEMANTIC_VECTORS_SORT,
		SEMANTIC_VECTORS_RECENT_TAGS,
		CLUSTER_COUNTS_FOR_ITEM,
		CLUSTER_COUNTS_FOR_ITEM_SIGNIFICANT,
		RECENT_ITEMS,
		SOCIAL_PREDICT,
		USER_TAG_COUNT,
		ELPH,
		SIMILAR_ITEMS,
		RECENT_SIMILAR_ITEMS,
		ITEM_SIMILARITY_RECOMENDER,
		TAG_CLUSTER_COUNTS,
		R_RECENT_ITEMS,
        MATRIX_FACTOR,
        RECENT_MATRIX_FACTOR,
        TOPIC_MODEL,
        RECENT_TOPIC_MODEL,
        SEMANTIC_VECTORS}
	public enum CF_PREDICTOR { RESNICK, 
		RESNICK_ITEM,
		RESNICK_SARIC, 
		WEIGHTED_MEDIAN, 
		WEIGHTED_MEAN, 
		SEMANTIC_VECTORS, 
		MAHOUT_ALS, 
		GRAPHLAB_PMF,
		USER_AVG, 
		ITEM_AVG, 
		NAIVE_BAYES, 
		MID_RATING, 
		MAX_RATING }
	public enum CF_SORTER { 
		RELEVANCE, 
		TAG_SIMILARITY, 
		DEMOGRAPHICS, 
		MAHOUT_FPGROWTH,
		SEMANTIC_VECTORS,
		CLUSTER_COUNTS,
		CLUSTER_COUNTS_DYNAMIC,
		NOOP,
		MOST_POPULAR_MEMBASED,
		MOST_POPULAR_WEIGHTED_MEMBASED,
		MOST_RECENT_MEMBASED,
		MOST_POP_RECENT_MEMBASED,
		WEB_SIMILARITY,
		COOCCURRENCE,
		STORM_TRUST}
	public enum CF_ITEM_COMPARATOR { 
		SEMANTIC_VECTORS, 
		TRUST_ITEM, 
		MAHOUT_ITEM }
	public enum CF_STRATEGY {
		FIRST_SUCCESSFUL,
		ORDERED,
		WEIGHTED,
		RANK_SUM,
		ADD_MISSING
	}
	public enum CF_POSTPROCESSING {
		NONE,
		REORDER_BY_POPULARITY,
		TIME_HITS,
		HITS,
		HITS_WEIGHTED,
		WEB_SIMILARITY,
		ADD_MISSING
	}
	
	public enum CF_CLUSTER_ALGORITHM {
		NONE,
		LDA_USER,
		LDA_ITEM,
		DIMENSION
	}
	
	private static Logger logger = Logger.getLogger(CFAlgorithm.class.getName());
	

	private List<CF_ITEM_COMPARATOR> itemComparators = new ArrayList<CF_ITEM_COMPARATOR>();
	private CF_STRATEGY itemComparatorStrategy = CF_STRATEGY.FIRST_SUCCESSFUL;

	private List<CF_RECOMMENDER> recommenders = new ArrayList<CF_RECOMMENDER>();
	private CF_STRATEGY recommenderStrategy = CF_STRATEGY.FIRST_SUCCESSFUL;

	private List<CF_PREDICTOR> predictors = new ArrayList<CF_PREDICTOR>();
	private CF_STRATEGY predictorStrategy = CF_STRATEGY.FIRST_SUCCESSFUL;

	private List<CF_SORTER> sorters  = new ArrayList<CF_SORTER>();
	private CF_STRATEGY sorterStrategy = CF_STRATEGY.FIRST_SUCCESSFUL;

	private CF_POSTPROCESSING postprocessing =  CF_POSTPROCESSING.NONE;
	
	Map<CF_RECOMMENDER,Double> recommendationWeightMap = new HashMap<CF_RECOMMENDER,Double>();
	int maxRecommendersToUse = 2;
	
	private Date date; // base algorithm to run as if from this date
	private Date recommendAfter; // only provide recommendations for things created after this date

    // ~ Fields #2: originally in RummbleLabsClient ~

    private String name;
    private String recTag;
    private double minRating = 0;
    private double maxRating = 10;
    private int userCFLimit = 50;
    private int transactionActionType = 0; // the action_type that defines transactions (purchases, page views)

    // Recommendation parameters
    private int recommendK = 50;
    private double recommendMinTrust = 0.7;
    private int recommendationCachingTimeSecs = 0;

    // Prediction parameters
    private int predictK = 50;
    private int predictNeighbours = 500;
    private double predictMinTrust = 0.7;

    //Semantic Vector parameters
    private int minNumTxsForSV = 5;
    private int txHistorySizeForSV = 1;
    private int recentArticlesForSV = 1000;
    private boolean ignorePerfectSVMatches = true;
    private String svPrefix = SemanticVectorsManager.SV_TEXT_NEW_LOC_PATTERN;
    
    //topic models
    int minNumTagsForTopicWeights = 4;
    
    //Cluster parameters
    double longTermWeight = 1.0D;
    double shortTermWeight = 1.0D;
    double decayRateSecs = 3600;
    String categoryDim = "category";
    
    int numRecentActions = 0;
    
    int tagAttrId = 9;
    String tagTable = "varchar";
    int tagUserHistory = 1;
    boolean tagClusterCountsActive = true;
    
    //Ranking parameters
    private boolean rankingRemoveHistory = true; // remove recent items for a user from items to rank
    
    //Recommendation remove ignore recommendations
    boolean removeIgnoredRecommendations = false;
    
    float recommendationDiversity = 1.0f; // should be a value >= 1.0 A value of 1.0 is no diversity.
    
    CF_CLUSTER_ALGORITHM clusterAlgorithm = CF_CLUSTER_ALGORITHM.NONE;
    int minNumberItemsForValidClusterResult = 0;
    boolean useBucketCluster = false; // add cluster counts for users not in any cluster to a single "bucket" cluster.
    
    
    boolean allowFullSocialPredictSearch = false;
    
    int userTagAttrId = 9;
    
    String abTestingKey = null;
    
    int similarUsersMetric = 2;
    
    // ~ END field ~

	public CFAlgorithm() {
	}

    // accessors

	
	
	public List<CF_RECOMMENDER> getRecommenders() {
		return recommenders;
	}

	
	

	public int getSimilarUsersMetric() {
		return similarUsersMetric;
	}

	public void setSimilarUsersMetric(int similarUsersMetric) {
		this.similarUsersMetric = similarUsersMetric;
	}

	public boolean isTagClusterCountsActive() {
		return tagClusterCountsActive;
	}

	public void setTagClusterCountsActive(boolean tagClusterCountsActive) {
		this.tagClusterCountsActive = tagClusterCountsActive;
	}

	public int getTagUserHistory() {
		return tagUserHistory;
	}

	public void setTagUserHistory(int tagUserHistory) {
		this.tagUserHistory = tagUserHistory;
	}

	public int getTagAttrId() {
		return tagAttrId;
	}

	public void setTagAttrId(int tagAttrId) {
		this.tagAttrId = tagAttrId;
	}
	
	public String getTagTable() {
		return tagTable;
	}

	public void setTagTable(String tagTable) {
		this.tagTable = tagTable;
	}

	public String getCategoryDim() {
		return categoryDim;
	}

	public void setCategoryDim(String categoryDim) {
		this.categoryDim = categoryDim;
	}

	public boolean isUseBucketCluster() {
		return useBucketCluster;
	}

	public void setUseBucketCluster(boolean useBucketCluster) {
		this.useBucketCluster = useBucketCluster;
	}

	public String getAbTestingKey() {
		return abTestingKey;
	}

	public void setAbTestingKey(String abTestingKey) {
		this.abTestingKey = abTestingKey;
	}

	public int getUserTagAttrId() {
		return userTagAttrId;
	}

	public void setUserTagAttrId(int userTagAttrId) {
		this.userTagAttrId = userTagAttrId;
	}

	public boolean isIgnorePerfectSVMatches() {
		return ignorePerfectSVMatches;
	}

	public void setIgnorePerfectSVMatches(boolean ignorePerfectSVMatches) {
		this.ignorePerfectSVMatches = ignorePerfectSVMatches;
	}

	public boolean isAllowFullSocialPredictSearch() {
		return allowFullSocialPredictSearch;
	}

	public void setAllowFullSocialPredictSearch(boolean allowFullSocialPredictSearch) {
		this.allowFullSocialPredictSearch = allowFullSocialPredictSearch;
	}

	public Map<CF_RECOMMENDER, Double> getRecommendationWeightMap() {
		return recommendationWeightMap;
	}

	public void setRecommendationWeightMap(
			Map<CF_RECOMMENDER, Double> recommendationWeightMap) {
		this.recommendationWeightMap = recommendationWeightMap;
	}

	public int getMaxRecommendersToUse() {
		return maxRecommendersToUse;
	}

	public void setMaxRecommendersToUse(int maxRecommendersToUse) {
		this.maxRecommendersToUse = maxRecommendersToUse;
	}

	public int getMinNumberItemsForValidClusterResult() {
		return minNumberItemsForValidClusterResult;
	}

	public void setMinNumberItemsForValidClusterResult(
			int minNumberItemsForValidClusterResult) {
		this.minNumberItemsForValidClusterResult = minNumberItemsForValidClusterResult;
	}

	public CF_CLUSTER_ALGORITHM getClusterAlgorithm() {
		return clusterAlgorithm;
	}

	public void setClusterAlgorithm(CF_CLUSTER_ALGORITHM clusterAlgorithm) {
		this.clusterAlgorithm = clusterAlgorithm;
	}

	public int getRecentArticlesForSV() {
		return recentArticlesForSV;
	}

	public void setRecentArticlesForSV(int recentArticlesForSV) {
		this.recentArticlesForSV = recentArticlesForSV;
	}

	public boolean isRemoveIgnoredRecommendations() {
		return removeIgnoredRecommendations;
	}

	public void setRemoveIgnoredRecommendations(boolean removeIgnoredRecommendations) {
		this.removeIgnoredRecommendations = removeIgnoredRecommendations;
	}

	public void setRecommenders(List<CF_RECOMMENDER> recommenders) {
		this.recommenders = recommenders;
	}

	public CF_STRATEGY getRecommenderStrategy() {
		return recommenderStrategy;
	}

	public void setRecommenderStrategy(CF_STRATEGY recommenderStrategy) {
		this.recommenderStrategy = recommenderStrategy;
	}

	public List<CF_PREDICTOR> getPredictors() {
		return predictors;
	}

	public void setPredictors(List<CF_PREDICTOR> predictors) {
		this.predictors = predictors;
	}

	public CF_STRATEGY getPredictorStrategy() {
		return predictorStrategy;
	}

	public void setPredictorStrategy(CF_STRATEGY predictorStrategy) {
		this.predictorStrategy = predictorStrategy;
	}

	public List<CF_SORTER> getSorters() {
		return sorters;
	}

	public void setSorters(List<CF_SORTER> sorters) {
		this.sorters = sorters;
	}

	public CF_STRATEGY getSorterStrategy() {
		return sorterStrategy;
	}

	public void setSorterStrategy(CF_STRATEGY sorterStrategy) {
		this.sorterStrategy = sorterStrategy;
	}

	public List<CF_ITEM_COMPARATOR> getItemComparators() {
		return itemComparators;
	}

	public void setItemComparators(List<CF_ITEM_COMPARATOR> itemComparators) {
		this.itemComparators = itemComparators;
	}

	public CF_STRATEGY getItemComparatorStrategy() {
		return itemComparatorStrategy;
	}

	public void setItemComparatorStrategy(CF_STRATEGY itemComparatorStrategy) {
		this.itemComparatorStrategy = itemComparatorStrategy;
	}
	
    public CF_POSTPROCESSING getPostprocessing() {
		return postprocessing;
	}

	public void setPostprocessing(CF_POSTPROCESSING postprocessing) {
		this.postprocessing = postprocessing;
	}

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public Date getRecommendAfter() {
        return recommendAfter;
    }

    public void setRecommendAfter(Date recommendAfter) {
        this.recommendAfter = recommendAfter;
    }


	public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
    

    public String getRecTag() {
		return recTag;
	}

	public void setRecTag(String recTag) {
		this.recTag = recTag;
	}

	public double getMinRating() {
        return minRating;
    }

    public void setMinRating(double minRating) {
        this.minRating = minRating;
    }

    public double getMaxRating() {
        return maxRating;
    }

    public void setMaxRating(double maxRating) {
        this.maxRating = maxRating;
    }

    public int getUserCFLimit() {
        return userCFLimit;
    }

    public void setUserCFLimit(int userCFLimit) {
        this.userCFLimit = userCFLimit;
    }

    public int getTransactionActionType() {
        return transactionActionType;
    }

    public void setTransactionActionType(int transactionActionType) {
        this.transactionActionType = transactionActionType;
    }

    public int getRecommendK() {
        return recommendK;
    }

    public void setRecommendK(int recommendK) {
        this.recommendK = recommendK;
    }

    public double getRecommendMinTrust() {
        return recommendMinTrust;
    }

    public void setRecommendMinTrust(double recommendMinTrust) {
        this.recommendMinTrust = recommendMinTrust;
    }

    public int getPredictK() {
        return predictK;
    }

    public void setPredictK(int predictK) {
        this.predictK = predictK;
    }

    public int getPredictNeighbours() {
        return predictNeighbours;
    }

    public void setPredictNeighbours(int predictNeighbours) {
        this.predictNeighbours = predictNeighbours;
    }

    public double getPredictMinTrust() {
        return predictMinTrust;
    }

    public void setPredictMinTrust(double predictMinTrust) {
        this.predictMinTrust = predictMinTrust;
    }

    public int getRecommendationCachingTimeSecs() {
		return recommendationCachingTimeSecs;
	}

	public void setRecommendationCachingTimeSecs(int recommendationCachingTimeSecs) {
		this.recommendationCachingTimeSecs = recommendationCachingTimeSecs;
	}
    
	
    
    // toString, equals, clone, hashCode:

    public float getRecommendationDiversity() {
		return recommendationDiversity;
	}

	public void setRecommendationDiversity(float recommendationDiversity) {
		this.recommendationDiversity = recommendationDiversity;
	}

	public boolean isRankingRemoveHistory() {
		return rankingRemoveHistory;
	}

	public void setRankingRemoveHistory(boolean rankingRemoveHistory) {
		this.rankingRemoveHistory = rankingRemoveHistory;
	}

	public int getMinNumTxsForSV() {
		return minNumTxsForSV;
	}

	public void setMinNumTxsForSV(int minNumTxsForSV) {
		this.minNumTxsForSV = minNumTxsForSV;
	}
	

	public int getTxHistorySizeForSV() {
		return txHistorySizeForSV;
	}

	public void setTxHistorySizeForSV(int txHistorySizeForSV) {
		this.txHistorySizeForSV = txHistorySizeForSV;
	}
	
	

	public double getLongTermWeight() {
		return longTermWeight;
	}

	public void setLongTermWeight(double longTermWeight) {
		this.longTermWeight = longTermWeight;
	}

	public double getShortTermWeight() {
		return shortTermWeight;
	}

	public void setShortTermWeight(double shortTermWeight) {
		this.shortTermWeight = shortTermWeight;
	}
	
	

	public double getDecayRateSecs() {
		return decayRateSecs;
	}

	public void setDecayRateSecs(double decayRateSecs) {
		this.decayRateSecs = decayRateSecs;
	}
	
	

	public String getSvPrefix() {
		return svPrefix;
	}

	public void setSvPrefix(String svPrefix) {
		this.svPrefix = svPrefix;
	}
	
	

	public int getNumRecentActions() {
		return numRecentActions;
	}

    private int numRecentItems= 200;

    public int getNumRecentItems() {
        return numRecentItems;
    }

    public void setNumRecentItems(int numRecentItems) {
        this.numRecentItems = numRecentItems;
    }

	public void setNumRecentActions(int numRecentActions) {
		this.numRecentActions = numRecentActions;
	}
	
	

	public int getMinNumTagsForTopicWeights() {
		return minNumTagsForTopicWeights;
	}

	public void setMinNumTagsForTopicWeights(int minNumTagsForTopicWeights) {
		this.minNumTagsForTopicWeights = minNumTagsForTopicWeights;
	}

	public String toString() {
    StringBuilder buf = new StringBuilder();

    if (this.recTag != null)
    	buf.append(" RecTag:").append(recTag);
    else
    	buf.append(" RecTag:").append("DEFAULT");
    
    int count = 1;
    for (CF_RECOMMENDER recommender : recommenders)
    buf.append(" Recommender").append(count++).append(":").append(recommender.name());

    count = 1;
    for (CF_PREDICTOR predictor : predictors)
    buf.append(" Predictor").append(count++).append(":").append(predictor.name());

    count = 1;
    for (CF_SORTER sorter : sorters)
    buf.append(" Sorter").append(count++).append(":").append(sorter.name());

    count = 1;
    for (CF_ITEM_COMPARATOR comparator : itemComparators)
    buf.append(" Item Comparator").append(count++).append(":").append(comparator.name());
    return buf.toString();
}

    @Override
    public CFAlgorithm clone() throws CloneNotSupportedException {
        return (CFAlgorithm) super.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CFAlgorithm that = (CFAlgorithm) o;

        if (Double.compare(that.maxRating, maxRating) != 0) return false;
        if (Double.compare(that.minRating, minRating) != 0) return false;
        if (predictK != that.predictK) return false;
        if (Double.compare(that.predictMinTrust, predictMinTrust) != 0) return false;
        if (predictNeighbours != that.predictNeighbours) return false;
        if (recommendK != that.recommendK) return false;
        if (Double.compare(that.recommendMinTrust, recommendMinTrust) != 0) return false;
        if (transactionActionType != that.transactionActionType) return false;
        if (userCFLimit != that.userCFLimit) return false;
        if (date != null ? !date.equals(that.date) : that.date != null) return false;
        if (itemComparatorStrategy != that.itemComparatorStrategy) return false;
        if (itemComparators != null ? !itemComparators.equals(that.itemComparators) : that.itemComparators != null)
            return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (predictorStrategy != that.predictorStrategy) return false;
        if (predictors != null ? !predictors.equals(that.predictors) : that.predictors != null) return false;
        if (recommendAfter != null ? !recommendAfter.equals(that.recommendAfter) : that.recommendAfter != null)
            return false;
        if (recommenderStrategy != that.recommenderStrategy) return false;
        if (recommenders != null ? !recommenders.equals(that.recommenders) : that.recommenders != null) return false;
        if (sorterStrategy != that.sorterStrategy) return false;
        if (sorters != null ? !sorters.equals(that.sorters) : that.sorters != null) return false;
        if (postprocessing != that.postprocessing) return false;
        if (longTermWeight != that.longTermWeight) return false;
        if (shortTermWeight != that.shortTermWeight) return false;
        if(numRecentItems != that.numRecentItems) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = itemComparators != null ? itemComparators.hashCode() : 0;
        result = 31 * result + (itemComparatorStrategy != null ? itemComparatorStrategy.hashCode() : 0);
        result = 31 * result + (recommenders != null ? recommenders.hashCode() : 0);
        result = 31 * result + (recommenderStrategy != null ? recommenderStrategy.hashCode() : 0);
        result = 31 * result + (predictors != null ? predictors.hashCode() : 0);
        result = 31 * result + (predictorStrategy != null ? predictorStrategy.hashCode() : 0);
        result = 31 * result + (sorters != null ? sorters.hashCode() : 0);
        result = 31 * result + (sorterStrategy != null ? sorterStrategy.hashCode() : 0);
        result = 31 * result + (postprocessing != null ? postprocessing.hashCode() : 0);
        result = 31 * result + (date != null ? date.hashCode() : 0);
        result = 31 * result + (recommendAfter != null ? recommendAfter.hashCode() : 0);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        temp = minRating != +0.0d ? Double.doubleToLongBits(minRating) : 0L;
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = maxRating != +0.0d ? Double.doubleToLongBits(maxRating) : 0L;
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + userCFLimit;
        result = 31 * result + transactionActionType;
        result = 31 * result + recommendK;
        temp = recommendMinTrust != +0.0d ? Double.doubleToLongBits(recommendMinTrust) : 0L;
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + predictK;
        result = 31 * result + predictNeighbours;
        temp = predictMinTrust != +0.0d ? Double.doubleToLongBits(predictMinTrust) : 0L;
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = longTermWeight != +0.0d ? Double.doubleToLongBits(longTermWeight) : 0L;
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = shortTermWeight != +0.0d ? Double.doubleToLongBits(shortTermWeight) : 0L;
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + numRecentActions;
        return result;
    }

	public void setParameter(String field, List<String> values) {
		try {
			//check if it's a multiple value
			if(values != null && !values.isEmpty()) {
				String value = values.iterator().next();
				if("item_comparators".equals(field)) {
					List<CF_ITEM_COMPARATOR> list = new ArrayList<CF_ITEM_COMPARATOR>();
					for(String val : values) {
						list.add(CF_ITEM_COMPARATOR.valueOf(val));
					}   
					setItemComparators(list);
				}
				else if("item_comparator_strategy".equals(field)) {
					setItemComparatorStrategy(CF_STRATEGY.valueOf(value));
				}
				else if("recommenders".equals(field)) {
					List<CF_RECOMMENDER> list = new ArrayList<CF_RECOMMENDER>();
					for(String val : values) {
						list.add(CF_RECOMMENDER.valueOf(val));
					}   
					setRecommenders(list);
				}
				else if("recommender_strategy".equals(field)) {
					setRecommenderStrategy(CF_STRATEGY.valueOf(value));
				}
				else if("predictors".equals(field)) {
					List<CF_PREDICTOR> list = new ArrayList<CF_PREDICTOR>();
					for(String val : values) {
						list.add(CF_PREDICTOR.valueOf(val));
					}   
					setPredictors(list);
				}
				else if("predictor_strategy".equals(field)) {
					setPredictorStrategy(CF_STRATEGY.valueOf(value));
				}
				else if("sorters".equals(field)) {
					List<CF_SORTER> list = new ArrayList<CF_SORTER>();
					for(String val : values) {
						list.add(CF_SORTER.valueOf(val));
					}   
					setSorters(list);
				}
				else if("sorter_strategy".equals(field)) {
					setSorterStrategy(CF_STRATEGY.valueOf(value));
				}
				else if("postprocessing".equals(field)) {
					setPostprocessing(CF_POSTPROCESSING.valueOf(value));
				}
				else if ("long_term_cluster_weight".equals(field)){
					this.setLongTermWeight(Double.parseDouble(value));
				}
				else if ("short_term_cluster_weight".equals(field)){
					this.setShortTermWeight(Double.parseDouble(value));
				}
				else if ("recent_articles_sv".equals(field)){
					this.setRecentArticlesForSV(Integer.parseInt(value));
				}
				else if ("tx_history_sv".equals(field)){
					this.setTxHistorySizeForSV(Integer.parseInt(value));
				}
				else if ("tag_attr_id".equals(field)){
					this.setTagAttrId(Integer.parseInt(value));
				}
				else if ("sv_prefix".equals(field)){
					this.setSvPrefix(value);
				}
				else if ("num_recent_actions".equals(field)){
					this.setNumRecentActions(Integer.parseInt(value));
				}
				else {
                    final String message = "Field : " + field + " not recognized";
                    logger.error(message, new Exception(message));
				}
			}
		}
		catch(Exception e) {
			logger.error("Not able to process the field : " + field + " with value: " + values, e);
		}
	}
	
	
	public String toLogSorter() {
		String res = "";
		//CF_SORTER
		if(sorters!=null && sorters.size()>0) {
			for(CF_SORTER s : sorters) {
				res += s.name() + "|";
			}
			res = res.substring(0,res.length()-1);
		}
		//CF_STRATEGY
		if(sorterStrategy!=null) 
			res +=";" + sorterStrategy.name();
		//CF_POSTPROCESSING
		if(postprocessing!=null)
			res +=";" + postprocessing.name();
		return res;
	}
    
}
