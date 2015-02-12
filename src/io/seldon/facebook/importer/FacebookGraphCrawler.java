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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.seldon.facebook.client.ThrottledFacebookClient;
import org.apache.log4j.Logger;

import com.restfb.Connection;
import com.restfb.FacebookClient;
import com.restfb.Parameter;
import com.restfb.exception.FacebookException;
import com.restfb.types.CategorizedFacebookType;
import com.restfb.types.Comment;
import com.restfb.types.NamedFacebookType;
import com.restfb.types.Post;
import com.restfb.types.Post.Comments;
import com.restfb.types.User;
import io.seldon.facebook.FacebookSimpleUser;
import io.seldon.facebook.FacebookUserGraph;
import io.seldon.facebook.Like;
import io.seldon.facebook.exception.FacebookUserRetrievalException;

public class FacebookGraphCrawler {

    private static final Logger logger = Logger.getLogger(FacebookGraphCrawler.class);
    public static final int LIKES_LIMIT = 100;
    public static final int DISTANT_LIKES_LIMIT = 25;
    public static final int FRIENDS_LIMIT = 100;
    public static final int FEED_LIMIT = 500;
    private FacebookClient fbClient;
    private Set<String> wallFriendIds;

    public FacebookGraphCrawler(String fbToken) {
        fbClient = new ThrottledFacebookClient(fbToken);
        // Don't throw facebook exceptions during construction      
        try {
            wallFriendIds = safeRecentFriendIds(); // time sensitive
        } catch (FacebookException e) {
            logger.warn("Problem retrieving wall friend ids: " + e.getMessage() + " for token " + fbToken, e);
            wallFriendIds = new HashSet<String>();
        }
    }

    public Map<String, Like> fetchLikes(String fbUserId) {
        return fetchLikes(fbUserId, LIKES_LIMIT);
    }

    public Map<String, Like> fetchLikes(String fbUserId, int limit) {
        logger.info("FB importing likes for fbUser=" + fbUserId);
        String fbConn = fbUserId + "/likes";
        Connection<Like> likes = fbClient.fetchConnection(fbConn, Like.class, Parameter.with("limit", limit));
        Map<String, Like> likesMap = new HashMap<String, Like>();

        for (Like like : likes.getData()) {
            likesMap.put(like.getId(), like);
        }
        return likesMap;
    }

    public User fetchUser(String fbUserId) {
        User fbUser = this.fbClient.fetchObject(fbUserId, User.class, Parameter.with("locale", "en_US"));
        final String name = fbUser == null ? "<null>" : fbUser.getName();
        logger.info("FB fetched user name = " + name);
        // demographics were previously introspected here; no longer necessary.
        return fbUser;
    }

    public Map<String, FacebookUserGraph> fetchFriends(String facebookId, Boolean demographics, Boolean likes, Boolean groups) {
        return fetchFriends(facebookId, demographics, likes, groups, LIKES_LIMIT, DISTANT_LIKES_LIMIT, FRIENDS_LIMIT);
    }

    /**
     * Retrieve a set of close friends derived from recent wall activity.
     *
     * @return a {@link Set} of Facebook IDs
     */
    private Set<String> safeRecentFriendIds() {
        try {
            return recentFriendIds();
        } catch (Exception e) {
            logger.warn("Could not fetch recent friend IDs: " + e.getMessage(), e);
            return new HashSet<String>();
        }
    }

    private Set<String> recentFriendIds() {
        Connection<Post> myFeed = fbClient.fetchConnection("me/feed", Post.class, Parameter.with("limit", FEED_LIMIT));
        Set<String> friends = new HashSet<String>();
        final List<Post> data = myFeed.getData();
        for (Post post : data) {
            final CategorizedFacebookType from = post.getFrom();
            final List<NamedFacebookType> to = post.getTo();
            Comments commentList = post.getComments();
            if (commentList != null)
            {
            	final List<Comment> comments = commentList.getData();
            	for (Comment comment : comments) {
            		final NamedFacebookType commenter = comment.getFrom();
            		// LABS-802: commenter maybe null. (This guard is replicated below for likes and recipients.)
                	if (commenter != null) {
                		friends.add(commenter.getId());
                	}
            	}
            }
            final Post.Likes likes = post.getLikes();
            if (likes != null) {
                final List<NamedFacebookType> likers = likes.getData();
                for (NamedFacebookType like : likers) {
                    if (like != null) {
                        friends.add(like.getId());
                    }
                }
            }
            friends.add(from.getId());
            for (NamedFacebookType namedFacebookType : to) {
                if (namedFacebookType != null) {
                    friends.add(namedFacebookType.getId());
                }
            }
        }
        return friends;
    }

    public static final String TOP_FRIENDS_BY_MUTUAL_FRIENDS_QUERY = "SELECT uid "+
            "FROM user"+
            " WHERE uid IN (SELECT uid2 FROM friend WHERE uid1 = me())"+
            " ORDER BY mutual_friend_count DESC";
    
    /**
     * Return a {@link List} of all recent contacts combined with a limited set of remaining friends.
     *
     * @param allFriends a {@link List} of all the user's friends
     * @param friendsLimit the number of friends to draw from the remaining (not recently contacted) pool
     * @return the friend sample
     */  
    private List<User> sampleFriends(List<User> allFriends, int friendsLimit){
    	List<FacebookSimpleUser> mutualFriends = fbClient.executeQuery(TOP_FRIENDS_BY_MUTUAL_FRIENDS_QUERY, FacebookSimpleUser.class, new Parameter[0]);
        List<User> recentFriends = new LinkedList<User>();
        List<User> remainingMutualFriends = new LinkedList<User>();
             
        for(FacebookSimpleUser mutualCand : mutualFriends){
        	for(User candidate : allFriends){       	
        		if(candidate.getId().equals(mutualCand.getId())){
        			remainingMutualFriends.add(candidate);
        		}
        	}
    	}
                
    	for(User candidate : allFriends){
    		if (wallFriendIds.contains(candidate.getId())) {
    			recentFriends.add(candidate);	
    			remainingMutualFriends.remove(candidate);
    		}
    	}
    	   		
    	List<User> merged = new LinkedList<User>(recentFriends);
    	final int count = remainingMutualFriends.size();
    	merged.addAll(remainingMutualFriends.subList(0, friendsLimit < count ? friendsLimit : count));
    	   
    	return merged;   	 	
    }

    public Map<String, FacebookUserGraph> fetchFriends(String facebookId, Boolean demographics,
                                                       Boolean likes, Boolean groups,
                                                       int likesLimit, int distantLikesLimit, int friendsLimit) {
        logger.info("FB fetchFriends for facebookId=" + facebookId);
        String fbConn = "me/friends";
        Connection<User> fbFriends;
        if (demographics) {
            fbFriends = fbClient.fetchConnection(fbConn, User.class,
                    //Parameter.with("limit", friendsLimit),
                    Parameter.with("locale", "en_US"),
                    Parameter.with("fields", "id, name, location, gender, locale, birthday, hometown, " +
                            "religion, timezone, work, languages, political, sports, meeting_for, education, " +
                            "interested_in, relationship_status,favorite_teams, favorite_athletes"));
        } else {
            fbFriends = fbClient.fetchConnection(fbConn, User.class,
                    //Parameter.with("limit", friendsLimit),
                    Parameter.with("locale", "en_US"));
        }

        List<User> friendSample = sampleFriends(fbFriends.getData(), friendsLimit);
        final int friendCount = friendSample.size();

        Map<String, FacebookUserGraph> friends = new HashMap<String, FacebookUserGraph>();

        for (User fbFriend : friendSample) {
            try {
                FacebookUserGraph friend;
                if (likes || demographics || groups) {
                    int limit = distantLikesLimit;
                    if (wallFriendIds.contains(fbFriend.getId())) {
                        logger.info("FB Close friend: " + fbFriend.getId() + "; importing up to " + likesLimit + " likes.");
                        limit = likesLimit;
                    }
                    try {
                        friend = fetchUserGraph(fbFriend, demographics, likes, groups, limit);
                        final Map<String, Like> friendLikes = friend.getLikes();
                        final int likeCount = friendLikes == null ? 0 : friendLikes.size();
                        logger.info("FB friend: " + fbFriend.getId() + "; imported " + likeCount + " likes.");
                    } catch (FacebookUserRetrievalException e) {
                        logger.info("Failed to retrieve user graph for " + e.getUserId() + " - creating placeholder.");
                        friend = new FacebookUserGraph(fbFriend);
                    }
                } else {
                    friend = new FacebookUserGraph(fbFriend);
                }
                friends.put(fbFriend.getId(), friend);
            } catch (Exception e) {
                logger.error("Error in fetching friend: " + fbFriend, e);
            }
        }

        logger.info("FB Friends - User " + facebookId + " - " + friendCount + " friends.");
        return friends;
    }

    @SuppressWarnings("unused")
    private FacebookUserGraph fetchUserGraph(User fbFriend, Boolean demographics, Boolean likes,
                                             Boolean groups, int likesLimit) throws FacebookUserRetrievalException {
        final String userId = fbFriend.getId();
        // this will fetch the demographics:
        User user = fetchUser(userId);
        if ( user == null ) {
            throw new FacebookUserRetrievalException(userId);
        }
        // now explicitly fetch the likes...
        Map<String, Like> likesMap = null;
        if (likes) {
            likesMap = fetchLikes(userId, likesLimit);
        }
        // Add: groups...
        return new FacebookUserGraph(user, likesMap);
    }

}
