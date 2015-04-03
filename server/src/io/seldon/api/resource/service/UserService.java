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

package io.seldon.api.resource.service;

import io.seldon.api.APIException;
import io.seldon.api.Constants;
import io.seldon.api.Util;
import io.seldon.api.caching.ClientIdCacheStore;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.DemographicBean;
import io.seldon.api.resource.ListBean;
import io.seldon.api.resource.UserBean;
import io.seldon.general.User;
import io.seldon.memcache.MemCacheKeys;
import io.seldon.memcache.MemCachePeer;

import java.util.Collection;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author claudio
 */

@Service
public class UserService {
	
	@Autowired
	private ClientIdCacheStore idCache;
	
    private static Logger logger = Logger.getLogger(UserService.class.getName());
    public static final int USERNAME_MAX_LENGTH = 25;

    public static UserBean getUser(ConsumerBean c, String uid, boolean full) throws APIException {
        UserBean bean = getUserSafe(c, uid, full);
		if(bean==null) throw new APIException(APIException.USER_NOT_FOUND);
		return bean;
	}

    public static UserBean getUserSafe(ConsumerBean c, String uid, boolean full) {
        String userBeanKey = MemCacheKeys.getUserBeanKey(c.getShort_name(), uid, full);
        UserBean bean = (UserBean) MemCachePeer.get(userBeanKey);
        if(bean == null) {
            User u = Util.getUserPeer(c).getUser(uid);
            if(u==null) { return null;}
            bean = new UserBean(u,full,c);
            if(Constants.CACHING) MemCachePeer.put(userBeanKey, bean,Constants.USERBEAN_CACHING_TIME);
        }
        return bean;
    }
	
	//Does not retrieve inactive users
	public static ListBean getUsers(ConsumerBean c, int limit, boolean full) throws APIException {
		ListBean bean = (ListBean) MemCachePeer.get(MemCacheKeys.getUsersBeanKey(c.getShort_name(), full));
		bean = Util.getLimitedBean(bean, limit);
		if(bean == null) {
			bean = new ListBean();
			Collection<User> res = Util.getUserPeer(c).getRecentUsers(limit);
			for(User u : res) { bean.addBean(new UserBean(u,full,c)); }
			if(Constants.CACHING) MemCachePeer.put(MemCacheKeys.getUsersBeanKey(c.getShort_name(),full), bean,Constants.CACHING_TIME);
		}
		return bean;
	}
	
	//Does not retrieve inactive users

	public static ListBean getUsersByName(ConsumerBean c, int limit, boolean full, String name) throws APIException {
			ListBean bean = (ListBean) MemCachePeer.get(MemCacheKeys.getUsersBeanKey(c.getShort_name(), full, name));
			bean = Util.getLimitedBean(bean, limit);
			if(bean == null) {
				bean = new ListBean();
				Collection<User> res = Util.getUserPeer(c).getUserByName(name,limit);
				for(User u : res) { bean.addBean(new UserBean(u,full,c)); }
				if(Constants.CACHING) MemCachePeer.put(MemCacheKeys.getUsersBeanKey(c.getShort_name(),full, name), bean,Constants.CACHING_TIME);
			}
			return bean;
	}


    public Long getInternalUserId(ConsumerBean c, String id) throws APIException {
        return getInternalUserId(c.getShort_name(), id);
    }


    public Long getInternalUserId(String consumerShortName, String id) throws APIException {
		 Long res = null;
		 res = getInternalUserIdInternal(consumerShortName, id);
         if (res == null)
		 {
	        logger.warn("getInternalUserId(" + id + "): USER NOT FOUND");
      		throw new APIException(APIException.USER_NOT_FOUND);

		 }
		return res;
	}

    public Long getInternalUserIdInternal(String consumerShortName, String id) {
        Long res = null;
        res = idCache.getInternalUserId(consumerShortName, id);
        if (res == null)
        {
            String userInternalId = MemCacheKeys.getUserInternalId(consumerShortName, id);
            res = (Long)MemCachePeer.get(userInternalId);
            if(res==null) {
                User u = Util.getUserPeer(consumerShortName).getUser(id);
                if(u!=null) {
                    res = u.getUserId();
                    idCache.putUserId(consumerShortName, id, res);
                    //MemCachePeer.put(userInternalId, res);
                    cacheInternalUserId(consumerShortName, id, res);
                }
            }
        }
        return res;
    }

    /**
     * Cache a user's internal ID, keyed by client ID
     * @param consumerBean -
     * @param clientId the client user ID (cache key)
     * @param internalId the internal user ID (cache value)
     */
    public static void cacheInternalUserId(String consumerBean, String clientId, Long internalId) {
        String clientIdKey = MemCacheKeys.getUserInternalId(consumerBean, clientId);
        MemCachePeer.put(clientIdKey, internalId,Constants.CACHING_TIME);
    }

    public String getClientUserId(String consumer, Long id){
        if(id == null) { throw new APIException(APIException.USER_NOT_FOUND); }
        String res = null;
        res = idCache.getExternalUserId(consumer, id);
        if (res == null)
            res = (String)MemCachePeer.get(MemCacheKeys.getUserClientId(consumer, id));
        if(res==null) {
            User u = Util.getUserPeer(consumer).getUser(id);
            if(u!=null) {
                res = u.getClientUserId();
                // users may be inferred, hence have no client id
                if ( res != null ) {
                    idCache.putUserId(consumer, res, id);
                    cacheClientUserId(consumer, id, res);
                }
            }
            else {
                logger.warn("getClientUserId(" + id + "): USER NOT FOUND");
                throw new APIException(APIException.USER_NOT_FOUND);
            }
        }
        return res;
    }

	public String getClientUserId(ConsumerBean c, Long id) throws APIException {
        return getClientUserId(c.getShort_name(), id);
	}

    /**
     * Cache a user's client item ID keyed by internal user ID
     * @param consumerBean -
     * @param internalId the internal user ID (cache key)
     * @param clientId  the client user ID (cache value)
     */
    public static void cacheClientUserId(ConsumerBean consumerBean, Long internalId, String clientId) {
        cacheClientUserId(consumerBean.getShort_name(), internalId, clientId);
    }

    public static void cacheClientUserId(String consumer, Long internalId, String clientId){
        final String internalIdKey = MemCacheKeys.getUserClientId(consumer, internalId);
        MemCachePeer.put(internalIdKey, clientId,Constants.CACHING_TIME);
    }

    /**
     * Bidirectionally cache the supplied user
     * (see {@link UserService#cacheClientUserId(io.seldon.api.resource.ConsumerBean, Long, String)}
     * and {@link UserService#cacheInternalUserId(String, String, Long)}).
     * @param consumerBean -
     * @param internalId user ID
     * @param clientId client ID
     */
    public static void cacheUser(ConsumerBean consumerBean, Long internalId, String clientId) {
        cacheClientUserId(consumerBean, internalId, clientId);
        cacheInternalUserId(consumerBean.getShort_name(), clientId, internalId);
    }

    public User addUser(ConsumerBean c,UserBean bean) {
		//check if the user is already in the system
		if(bean.getId()!=null) {
			Long id = getInternalUserIdInternal(c.getShort_name(),bean.getId());
			if(id!=null) throw new APIException(APIException.USER_DUPLICATED);
		}

		User u = bean.createUser(c);
        truncateUsername(u);
		Util.getUserPeer(c).persistUser(u);
		long userId = u.getUserId();
		
		//attributes
		if(bean.getAttributesName() != null && bean.getAttributesName().size()>0) {
			Util.getUserPeer(c).addUserAttributeNames(userId,bean.getType(),bean.getAttributesName(),c);
		} else if(bean.getAttributes() != null && bean.getAttributes().size()>0) {
			Util.getUserPeer(c).addUserAttribute(userId, bean.getType(), bean.getAttributes(),c);
		}
        cacheUser(c, bean, userId);
        return u;
	}

    private void cacheUser(ConsumerBean c, UserBean bean, long userId) {
        idCache.putUserId(c.getShort_name(), bean.getId(), userId);
        cacheInternalUserId(c.getShort_name(), bean.getId(), userId);
        MemCachePeer.put(MemCacheKeys.getUserBeanKey(c.getShort_name(),
                bean.getId(), bean.getAttributesName() != null), bean, Constants.USERBEAN_CACHING_TIME);
    }

    public User updateUser(ConsumerBean c,UserBean bean,boolean async) {
        UserBean user = null;
        Long userId;
        boolean fullUpdate = (bean.getAttributes()!=null && bean.getAttributes().size() > 0)
                || (bean.getAttributesName()!=null && bean.getAttributesName().size() > 0);

        userId = getInternalUserIdInternal(c.getShort_name(), bean.getId());
        if(userId==null){
            try {
            		return addUser(c, bean);
            } catch (APIException additionException) {
                if ( additionException.getError_id() == APIException.USER_DUPLICATED) {
                    throw new APIException(APIException.CONCURRENT_USER_UPDATE);
                } else {
                    throw additionException;
                }
            }
        }
        user = locateUser(c, bean.getId(), userId, fullUpdate);

        if(user==null){
            // something strange has happened
            logger.error("Found user id in cache but couldn't find the actual user object in caches or DB");
            throw new APIException(APIException.GENERIC_ERROR);
        }

        UserBean merged = mergeInUpdate(user, bean);
        if(merged == null){
            return User.fromUserBean(user, userId);
        }
//        truncateUsername(user);
//        return Util.getUserPeer(c).persistUser(user);
        if (merged.getAttributesName() != null &&  merged.getAttributesName().size() > 0) {
            Util.getUserPeer(c).addUserAttributeNames(userId, merged.getType(), merged.getAttributesName(), c);
            merged.getAttributesName().putAll(merged.getAttributesName());
        } else if (merged.getAttributes() != null && merged.getAttributes().size() > 0) {
            Util.getUserPeer(c).addUserAttribute(userId, merged.getType(),merged.getAttributes(), c);
        }
        String userBeanKeyTrue = MemCacheKeys.getUserBeanKey(c.getShort_name(), bean.getId(), true);
        String userBeanKeyFalse = MemCacheKeys.getUserBeanKey(c.getShort_name(), bean.getId(), false);

        if(fullUpdate) {
            MemCachePeer.put(userBeanKeyTrue, merged, Constants.CACHING_TIME);
            MemCachePeer.delete(userBeanKeyFalse);
        } else {
            //delete memcache entries
            MemCachePeer.delete(userBeanKeyTrue);
            MemCachePeer.delete(userBeanKeyFalse);
        }
        return User.fromUserBean(merged, userId);
    }

    private static UserBean mergeInUpdate(UserBean previous, UserBean update) {
        boolean activeDifferent = previous.isActive() != update.isActive();
        boolean typeDifferent = previous.getType() != update.getType();
        boolean usernameDifferent = !previous.getUsername().equals(update.getUsername());

        Map<Integer,Integer> previousAttributes = previous.getAttributes();
        Map<Integer, Integer> updateAttributes = update.getAttributes();
        boolean attrsDifferent;
        if(previousAttributes==null){
            attrsDifferent = updateAttributes!=null;
        } else {
           attrsDifferent = !previousAttributes.equals(updateAttributes);
        }
        boolean attrsNameDifferent;

        Map<String, String> previousAttributesName = previous.getAttributesName();
        Map<String, String> updateAttributesName = update.getAttributesName();
        if(previousAttributesName==null){
            attrsNameDifferent = updateAttributesName!=null;
        } else {
            attrsNameDifferent = !previousAttributesName.equals(updateAttributesName);
        }
        if(activeDifferent || typeDifferent || usernameDifferent || attrsDifferent || attrsNameDifferent){
            previous.setActive(update.isActive());
            previous.setType(update.getType());
            previous.setUsername(update.getUsername());
            previous.setAttributes(updateAttributes);
            previous.setAttributesName(updateAttributesName);
            return previous;
        } else return null;
    }

    private static UserBean locateUser(ConsumerBean c, String clientUserId, Long userId, boolean fullRequired) {
        UserBean user = null;
        String userBeanKey = MemCacheKeys.getUserBeanKey(c.getShort_name(), clientUserId, true);
        user = (UserBean) MemCachePeer.get(userBeanKey);
        if(user==null){
            if(!fullRequired){
                userBeanKey = MemCacheKeys.getUserBeanKey(c.getShort_name(), clientUserId, false);
                user = (UserBean) MemCachePeer.get(userBeanKey);
            }
            if(user==null){
                User dbUser = Util.getUserPeer(c).getUser(userId);
                if(dbUser==null){
                    return null;// can't find it anywhere so get out of here
                }
                user = new UserBean(dbUser, fullRequired, c);
            }

        }
        return user;
    }

    private static void truncateUsername(User user) {
        final String username = user.getUsername();
        final String truncated = StringUtils.left(username, USERNAME_MAX_LENGTH);
        user.setUsername(truncated);
    }

    public static DemographicBean getDemographic(ConsumerBean c, int demographic) {;
		DemographicBean bean = (DemographicBean)MemCachePeer.get(MemCacheKeys.getDemographicBeanKey(c.getShort_name(), demographic));
		if(bean == null) {
			bean = new DemographicBean();
			bean.setDemoId(demographic);
			Integer[] attr = Util.getUserPeer(c).getAttributes(demographic);
			String[] attrName = Util.getUserPeer(c).getAttributesNames(demographic);
			bean.setAttr(attr[0]);
			bean.setVal(attr[1]);
            // TODO address this properly:
            if ( attrName == null ) {
                bean.setAttrName("<all>");
                bean.setValName("<all>");
            } else {
                bean.setAttrName(attrName[0]);
                bean.setValName(attrName[1]);
            }
			MemCachePeer.put(MemCacheKeys.getDemographicBeanKey(c.getShort_name(), demographic),bean,Constants.CACHING_TIME);
		}
		return bean;
	}

	public static DemographicBean getDemographic(ConsumerBean c,String attrName,String valName) throws APIException {
		DemographicBean bean = (DemographicBean)MemCachePeer.get(MemCacheKeys.getDemographicBeanKey(c.getShort_name(),attrName,valName));
		if(bean == null) {
			int demoId = Util.getUserPeer(c).getDemographic(attrName,valName);
			if(demoId != Constants.DEFAULT_DEMOGRAPHIC) {
				bean = new DemographicBean();
				bean.setAttrName(attrName);
				bean.setValName(valName);
				Integer[] attr = Util.getUserPeer(c).getAttributes(demoId);
				bean.setDemoId(demoId);
				bean.setAttr(attr[0]);
				bean.setVal(attr[1]);
				MemCachePeer.put(MemCacheKeys.getDemographicBeanKey(c.getShort_name(),attrName,valName),bean,Constants.CACHING_TIME);
			}
			else return null;
		}
		return bean;
	}
    
}
