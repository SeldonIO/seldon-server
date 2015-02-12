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

package io.seldon.facebook.user;

import com.restfb.DefaultFacebookClient;
import com.restfb.Facebook;
import com.restfb.FacebookClient;
import com.restfb.exception.FacebookResponseStatusException;
import io.seldon.api.logging.FacebookCallLogger;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * Interactions between a user and his friends.
 * Class used to retrieve the data from the FB api.
 * @author dylanlentini
 *         Date: 28/10/2013
 *         Time: 12:45
 */
public class FacebookInteractionsGraphStatusPhotoLike {


    private static final String GET_STATUS = "SELECT status_id, time, source, message FROM status WHERE uid = me() AND like_info.like_count < 100 limit 50";
    private static final String GET_PHOTO = "SELECT object_id, created, caption FROM photo WHERE owner = me() AND like_info.like_count < 100 limit 50";
    private static final String GET_STATUS_LIKE = "SELECT user_id,object_id FROM like WHERE object_id IN (SELECT status_id FROM #status_query)";
    private static final String GET_PHOTO_LIKE = "SELECT user_id,object_id FROM like WHERE object_id IN (SELECT object_id FROM #photo_query)";
    private static final String GET_PERMISSIONS = "SELECT user_status, user_photos FROM permissions WHERE uid = me()";
    private static final Map<String, String> queries = new HashMap<String, String>();
    private static final Logger logger = Logger.getLogger(FacebookInteractionsGraphStatusPhotoLike.class);
    
    static 
    {
        queries.put("status_query", GET_STATUS);
        queries.put("photo_query", GET_PHOTO);
        queries.put("user_status_like_query", GET_STATUS_LIKE);
        queries.put("user_photo_like_query", GET_PHOTO_LIKE);
        queries.put("permissions_query", GET_PERMISSIONS);
    }

    
    public final List<FacebookLike> userStatusLikeList;
    public final List<FacebookLike> userPhotoLikeList;
    public final List<FacebookPermissions> permissions;

    private FacebookInteractionsGraphStatusPhotoLike(List<FacebookLike> likesUserStatus, List<FacebookLike> likesUserPhoto, List<FacebookPermissions> userPermissions)
    {
        this.userStatusLikeList = likesUserStatus;
        this.userPhotoLikeList = likesUserPhoto;
        this.permissions = userPermissions;
    }

    public static FacebookInteractionsGraphStatusPhotoLike build(String fbToken, FacebookCallLogger fbCallLogger)
    {
        logger.info("Retrieving all facebook friend interactions with token " + fbToken);
        FacebookClient facebookApiClient = new DefaultFacebookClient(fbToken);
        MultiQueryResult result = null;

        List<FacebookLike> likesUserStatus = null;
        List<FacebookLike> likesUserPhoto = null;
        List<FacebookPermissions> userPermissions = null;

        try 
        {
            for (int x=0; x<5; x++)
            {
                fbCallLogger.fbCallPerformed();
            }

            result = facebookApiClient.executeFqlMultiquery(queries, MultiQueryResult.class);
            
            likesUserStatus = result.getFacebookUserStatusLike();
            likesUserPhoto = result.getFacebookUserPhotoLike();
            userPermissions = result.getFacebookPermissions();
        } 
        catch (FacebookResponseStatusException frse) 
        {
            // this error code means that we requested TOO MUCH DATAS
            if (frse.getErrorCode() != null && frse.getErrorCode() == 1) 
            {
                logger.info("Failed to get facebook connections via multiquery call fbToken "+ fbToken);
            } 
            else 
            {
                logger.error("Problem when retrieving facebook connections for fbToken "+ fbToken, frse);
                throw frse;
            }
        }
        
        return new FacebookInteractionsGraphStatusPhotoLike(likesUserStatus, likesUserPhoto, userPermissions);
    }
    
    
    public static class MultiQueryResult {

    	@Facebook("status_query")
        List<FacebookStatus> status_query;
        @Facebook("photo_query")
        List<FacebookPhoto> photo_query;
        @Facebook("user_status_like_query")
        List<FacebookLike> user_status_like_query;
        @Facebook("user_photo_like_query")
        List<FacebookLike> user_photo_like_query;
        @Facebook("permissions_query")
        List<FacebookPermissions> permissions_query;
        
        public List<FacebookStatus> getFacebookStatus()
        {
        	return status_query;
        }
        
        public List<FacebookPhoto> getFacebookPhoto()
        {
        	return photo_query;
        }
        
        public List<FacebookLike> getFacebookUserStatusLike()
        {
        	return user_status_like_query;
        }
        
        public List<FacebookLike> getFacebookUserPhotoLike()
        {
        	return user_photo_like_query;
        }
        
        public List<FacebookPermissions> getFacebookPermissions()
        {
        	return permissions_query;
        }
    }

}
