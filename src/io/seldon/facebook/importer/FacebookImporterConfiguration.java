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

package io.seldon.facebook.importer;

/**
 * Created by: marc on 11/01/2012 at 11:37
 */
public class FacebookImporterConfiguration {

    private Boolean facebookEnabled = true;

    // By default, we only import the user's likes.
    // ~ user
    private Boolean demographics = false;
    private Boolean likes = true;
    private Boolean groups = false;
    // ~ friends
    private Boolean friends = false;
    private Boolean friendDemographics = false;
    private Boolean friendLikes = false;
    private Boolean friendGroups = false;

   // ~ limits
    private int likesLimit = 100;
    private int distantLikesLimit = 25;
    private int friendLimit = 100;

    // ~ filtering
    private String searchPeerUrl;
    private String searchPeerUser;
    private String searchPeerPass;
    private String wikipediaLanguages;
    private boolean dbpediaHitsFilter;
    private boolean wikipediaFilter;

    // member get member notify
    boolean postImportProcess = false;
    
    // This is to enable the new faster fb importer Oct2013
    boolean importerSpeedup = false;
    
    boolean noDbPersist = false; // this is to stop persisting likes etc to db
    
    boolean memcachePersist = false; // this for putting data into memcache, like the user and friends likes 
   
    private int queueRunnerMaxThreads = 15; // Maximum number of threads in pool for FacebookQueueRunner
    
    private String memcacheServerlist = "UNKOWN";
    
    public FacebookImporterConfiguration() {
    }

    public FacebookImporterConfiguration(Boolean demographics, Boolean likes, Boolean groups) {
        this.likes = likes;
        this.groups = groups;
        this.demographics = demographics;

        // ~ don't import friends:
        this.friends = false;
    }

    public FacebookImporterConfiguration(Boolean demographics, Boolean likes, Boolean groups,
                                         Boolean friendDemographics, Boolean friendLikes, Boolean friendGroups) {
        this.demographics = demographics;
        this.likes = likes;
        this.groups = groups;
        this.friends = true;
        this.friendDemographics = friendDemographics;
        this.friendLikes = friendLikes;
        this.friendGroups = friendGroups;
    }

    public Boolean getFacebookEnabled() {
        return facebookEnabled;
    }

    public void setFacebookEnabled(Boolean facebookEnabled) {
        this.facebookEnabled = facebookEnabled;
    }

    public Boolean isDemographics() {
        return demographics;
    }

    public void setDemographics(Boolean demographics) {
        this.demographics = demographics;
    }

    public Boolean isLikes() {
        return likes;
    }

    public void setLikes(Boolean likes) {
        this.likes = likes;
    }

    public Boolean isGroups() {
        return groups;
    }

    public void setGroups(Boolean groups) {
        this.groups = groups;
    }

    public Boolean isFriends() {
        return friends;
    }

    public void setFriends(Boolean friends) {
        this.friends = friends;
    }

    public Boolean isfDemographics() {
        return friendDemographics;
    }

    public void setFriendDemographics(Boolean friendDemographics) {
        this.friendDemographics = friendDemographics;
    }

    public Boolean isfLikes() {
        return friendLikes;
    }

    public void setFriendLikes(Boolean friendLikes) {
        this.friendLikes = friendLikes;
    }

    public Boolean isfGroups() {
        return friendGroups;
    }

    public void setFriendGroups(Boolean friendGroups) {
        this.friendGroups = friendGroups;
    }

    public int getLikesLimit() {
        return likesLimit;
    }

    public void setLikesLimit(int likesLimit) {
        this.likesLimit = likesLimit;
    }

    public int getFriendLimit() {
        return friendLimit;
    }

    public void setFriendLimit(int friendLimit) {
        this.friendLimit = friendLimit;
    }

    public int getDistantLikesLimit() {
        return distantLikesLimit;
    }

    public void setDistantLikesLimit(int distantLikesLimit) {
        this.distantLikesLimit = distantLikesLimit;
    }

    public String getSearchPeerUrl() {
        return searchPeerUrl;
    }

    public void setSearchPeerUrl(String searchPeerUrl) {
        this.searchPeerUrl = searchPeerUrl;
    }

    public String getSearchPeerUser() {
        return searchPeerUser;
    }

    public void setSearchPeerUser(String searchPeerUser) {
        this.searchPeerUser = searchPeerUser;
    }

    public String getSearchPeerPass() {
        return searchPeerPass;
    }

    public void setSearchPeerPass(String searchPeerPass) {
        this.searchPeerPass = searchPeerPass;
    }

    public String getWikipediaLanguages() {
        return wikipediaLanguages;
    }

    public void setWikipediaLanguages(String wikipediaLanguages) {
        this.wikipediaLanguages = wikipediaLanguages;
    }

    public boolean hasDbpediaHitsFilter() {
        return dbpediaHitsFilter;
    }

    public void setDbpediaHitsFilter(boolean dbpediaHitsFilter) {
        this.dbpediaHitsFilter = dbpediaHitsFilter;
    }

    public boolean hasWikipediaFilter() {
        return wikipediaFilter;
    }

    public void setWikipediaFilter(boolean wikipediaFilter) {
        this.wikipediaFilter = wikipediaFilter;
    }

    
    
    public boolean isPostImportProcess() {
		return postImportProcess;
	}

	public void setPostImportProcess(
			boolean postImportSendToUserSimilarityQ) {
		this.postImportProcess = postImportSendToUserSimilarityQ;
	}
	
	public boolean isImporterSpeedup() {
	    return importerSpeedup;
	}
	
	public void setImporterSpeedup(boolean importerSpeedup) {
	    this.importerSpeedup = importerSpeedup;
	}
	
	public boolean isNoDbPersist() {
	    return noDbPersist;
	}

	public void setNoDbPersist(boolean noDbPersist) {
	    this.noDbPersist = noDbPersist;
	}
	
	public boolean isMemcachePersist() {
	    return memcachePersist;
	}
	
	public void setMemcachePersist(boolean memcachePersist) {
	    this.memcachePersist = memcachePersist;
	}

	public int getQueueRunnerMaxThreads() {
        return queueRunnerMaxThreads;
    }

    public void setQueueRunnerMaxThreads(int queueRunnerMaxThreads) {
        this.queueRunnerMaxThreads = queueRunnerMaxThreads;
    }

    public String getMemcacheServerlist() {
        return memcacheServerlist;
    }

    public void setMemcacheServerlist(String memcacheServerlist) {
        this.memcacheServerlist = memcacheServerlist;
    }

    @Override
    public String toString() {
        return "FacebookImporterConfiguration{" +
                "facebookEnabled=" + facebookEnabled +
                ", demographics=" + demographics +
                ", likes=" + likes +
                ", groups=" + groups +
                ", friends=" + friends +
                ", friendDemographics=" + friendDemographics +
                ", friendLikes=" + friendLikes +
                ", friendGroups=" + friendGroups +
                ", likesLimit=" + likesLimit +
                ", distantLikesLimit=" + distantLikesLimit +
                ", friendLimit=" + friendLimit +
                ", sendUserToUserSimQ="+postImportProcess+
                '}';
    }
}
