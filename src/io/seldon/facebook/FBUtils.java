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

package io.seldon.facebook;

import io.seldon.api.Constants;
import io.seldon.api.Util;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.UserBean;
import io.seldon.general.UserAttribute;
import io.seldon.general.UserAttributeValueVo;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;

import java.sql.Timestamp;

public class FBUtils {

	public static boolean isFacebookActive(ConsumerBean c, long userId) {
		String memcacheKey = MemCacheKeys.getFBUserKey(c.getShort_name(), userId);
		Boolean res = (Boolean) MemCachePeer.get(memcacheKey);
		if (res == null)
		{
			res = false;
			try 
			{
				UserAttributeValueVo<Boolean> value = Util.getUserAttributePeer(c).getScalarUserAttributeValueForUser(userId, FBConstants.FB);
				res = value.getValue();
				MemCachePeer.put(memcacheKey, res,Constants.CACHING_TIME);
			}
			catch(Exception e) {}
		}
		return res;
	}
	
	public static void enableFacebook(ConsumerBean c, long userId) { 
		UserAttribute attr = Util.getUserAttributePeer(c).findUserAttributeByName(FBConstants.FB);
		Util.getUserAttributePeer(c).addUserAttributeValue(userId, attr.getAttributeId(), true, attr.getType());
	}
	
	public static void disableFacebook(ConsumerBean c, long userId) { 
		UserAttribute attr = Util.getUserAttributePeer(c).findUserAttributeByName(FBConstants.FB);
		Util.getUserAttributePeer(c).addUserAttributeValue(userId, attr.getAttributeId(), false, attr.getType());
	}

    public static boolean hasBeenImported(ConsumerBean c, UserBean user){
        String fbFinishImport = user.getAttributesName().get(FBConstants.FB_FINISH_IMPORT);
        if(fbFinishImport==null || fbFinishImport.isEmpty()) return false;
        Timestamp end = Timestamp.valueOf(fbFinishImport);
        String fbStartImport = user.getAttributesName().get(FBConstants.FB_START_IMPORT);
        Timestamp start = Timestamp.valueOf(fbStartImport);
        // if the import took less than 10 seconds then we can be confident that it failed.
        return (end.getTime() - start.getTime()) > 10000;
    }
}
