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

package io.seldon.general.jdo;

import javax.jdo.Extent;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;
import java.util.*;

import io.seldon.api.Constants;
import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.general.User;
import io.seldon.general.UserAttribute;
import io.seldon.general.UserAttributePeer;
import io.seldon.general.UserAttributeValueVo;
import io.seldon.general.exception.UnknownUserAttributeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by: marc on 08/08/2011 at 15:43
 */
public class SqlUserAttributePeer implements UserAttributePeer {

    private static final List<String> scalarAttributeTypes = Arrays.asList(
            Constants.TYPE_VARCHAR, Constants.TYPE_BOOLEAN, Constants.TYPE_BIGINT,
            Constants.TYPE_DOUBLE, Constants.TYPE_BOOLEAN, Constants.TYPE_DATETIME
    );

    private static final List<String> compositeAttributeTypes = Arrays.asList(
            Constants.TYPE_ENUM
    );

    private static final Logger logger = LoggerFactory.getLogger(SqlUserAttributePeer.class);
    private final PersistenceManager persistenceManager;

    public SqlUserAttributePeer(PersistenceManager persistenceManager) {
        this.persistenceManager = persistenceManager;
    }

    public Collection<UserAttribute> getAll() {
        Extent<UserAttribute> extent = persistenceManager.getExtent(UserAttribute.class);
        Query query = persistenceManager.newQuery(extent);
        query.setOrdering("attributeId ascending");

        @SuppressWarnings({"unchecked", "UnnecessaryLocalVariable"})
        Collection<UserAttribute> attributes = (Collection<UserAttribute>) query.execute();
        return attributes;
    }

    public Boolean addAttribute(final UserAttribute userAttribute) {
        boolean success = true;

        try {
            TransactionPeer.runTransaction(new Transaction(persistenceManager) {
                public void process() {
                    persistenceManager.makePersistent(userAttribute);
                }
            });
        } catch (DatabaseException e) {
            logger.error("Failed to add user attribute.", e);
            success = false;
        }

        return success;
    }

    public Boolean addAttribute(String attributeName, String attributeType) {
        UserAttribute attribute = new UserAttribute(attributeName, attributeType);
        return addAttribute(attribute);
    }

    public void delete(final UserAttribute attribute) throws DatabaseException {
        TransactionPeer.runTransaction(new Transaction(persistenceManager) {
            public void process() {
                persistenceManager.deletePersistent(attribute);
            }
        });
    }

    public Map<Integer, UserAttributeValueVo> getAttributesForUser(User user) {
        return getAttributesForUser(user.getUserId());
    }

    @SuppressWarnings({"unchecked"})
    public Map<Integer, UserAttributeValueVo> getAttributesForUser(Long userId) {
        Map<Integer, UserAttributeValueVo> userAttributeValueMap = new HashMap<>();

        // (1) Deal with scalar types -- each table provides {attr_id,value}
        for (String type : scalarAttributeTypes) {
            Collection<Object[]> items = findScalarAttributeValuesForUser(userId, type);

            for (Object[] item : items) {
                Integer attributeId = (Integer) item[0];
                Object value = item[1];

                userAttributeValueMap.put(attributeId, new UserAttributeValueVo(userId, attributeId, value, type));
            }
        }

        // (2) Deal with composite types -- here we have {attr_id, value_id}
        for (String type : compositeAttributeTypes) {
            Collection<Object[]> items = findCompositeAttributeValuesForUser(userId, type);

            for (Object[] item : items) {
                Integer attributeId = (Integer) item[0];
                Integer valueId = (Integer) item[1];

                userAttributeValueMap.put(attributeId, new UserAttributeValueVo(userId, attributeId, valueId, type));
            }
        }

        return userAttributeValueMap;
    }

    public Map<Integer, Integer> getCompositeAttributesForUser(User user) {
        return getCompositeAttributesForUser(user.getUserId());
    }

    public Map<Integer, Integer> getCompositeAttributesForUser(Long userId) {
        Map<Integer, Integer> attributeValueMap = new HashMap<>();
        for (String compositeType : compositeAttributeTypes) {
            Collection<Object[]> items = findCompositeAttributeValuesForUser(userId, compositeType);
            for (Object[] item : items) {
                Integer attributeId = (Integer) item[0];
                Integer valueId = (Integer) item[1];
                attributeValueMap.put(attributeId, valueId);
            }
        }
        return attributeValueMap;
    }

    public Map<String, String> getCompositeAttributeNamesForUser(User user) {
        return getCompositeAttributeNamesForUser(user.getUserId());
    }

    public Map<String, String> getCompositeAttributeNamesForUser(Long userId) {
        Map<String, String> attributeValueNameMap = new HashMap<>();
        for (String compositeType : compositeAttributeTypes) {
            Collection<Object[]> items = findCompositeAttributeValueNamesForUser(userId, compositeType);
            for (Object[] item : items) {
                String attributeName = (String) item[0];
                String valueName = (String) item[1];
                attributeValueNameMap.put(attributeName, valueName);
            }
        }
        return attributeValueNameMap;
    }

    public UserAttributeValueVo getScalarUserAttributeValueForUser(Long userId, Integer attributeId) throws UnknownUserAttributeException {
        String typeQueryString = "select type from user_attr where attr_id = ?";
        Query typeQuery = persistenceManager.newQuery("javax.jdo.query.SQL", typeQueryString);

        @SuppressWarnings({"unchecked"})
        Collection<String> typeNames = (Collection<String>) typeQuery.execute(attributeId);

        if (typeNames.size() == 0) {
            throw new UnknownUserAttributeException("Attribute with ID: " + attributeId + " not found.");
        }

        String typeName = typeNames.iterator().next();
        return findScalarAttributeValueForUser(userId, attributeId, typeName);
    }

    public UserAttributeValueVo getScalarUserAttributeValueForUser(Long userId, String attributeName) throws UnknownUserAttributeException {
        String queryString = "select attr_id, type from user_attr where name = ?";
        Query query = persistenceManager.newQuery("javax.jdo.query.SQL", queryString);
        @SuppressWarnings({"unchecked"})
        Collection<Object[]> attributeInformationRows = (Collection<Object[]>) query.execute(attributeName);

        if (attributeInformationRows.size() == 0) {
            throw new UnknownUserAttributeException("Attribute named: " + attributeName + " not found.");
        }

        Object[] attributeInformation = attributeInformationRows.iterator().next();
        Integer attributeId = (Integer) attributeInformation[0];
        String typeName = (String) attributeInformation[1];

        return findScalarAttributeValueForUser(userId, attributeId, typeName);
    }

    public UserAttributeValueVo getScalarUserAttributeValueForUser(User user, String attributeName) throws UnknownUserAttributeException {
        return getScalarUserAttributeValueForUser(user.getUserId(), attributeName);
    }

    public UserAttribute findUserAttributeByName(String name) {
        Extent<UserAttribute> extent = persistenceManager.getExtent(UserAttribute.class);
        Query query = persistenceManager.newQuery(extent);
        query.setFilter("name == attrName");
        query.declareParameters("String attrName");

        @SuppressWarnings({"unchecked", "UnnecessaryLocalVariable"})
        Collection<UserAttribute> attributes = (Collection<UserAttribute>) query.execute(name);
        Iterator<UserAttribute> iterator = attributes.iterator();
        return iterator.hasNext() ? iterator.next() : null;
    }

    public UserAttribute findByNameAndType(String name, Integer type) {
        Extent<UserAttribute> extent = persistenceManager.getExtent(UserAttribute.class);
        Query query = persistenceManager.newQuery(extent);
        query.setFilter("name == attrName && type == type");
        query.declareParameters("String attrName, Integer type");

        @SuppressWarnings("unchecked")
        Collection<UserAttribute> attributes = (Collection<UserAttribute>) query.execute(name, type);
        Iterator<UserAttribute> iterator = attributes.iterator();
        return iterator.hasNext() ? iterator.next() : null;
    }

    public void saveUserAttributeValue(UserAttributeValueVo<?> userAttributeValue) {
        Long userId = userAttributeValue.getUserId();
        Integer attributeId = userAttributeValue.getAttributeId();
        String type = userAttributeValue.getType();
        Object value = userAttributeValue.getValue();
        addUserAttributeValue(userId, attributeId, value, type);
    }

    public void addUserAttributeValue(final Long userId, final Integer attributeId, final Object value, String type) {
        final StringBuilder queryBuilder = new StringBuilder();
        queryBuilder
                .append("insert into user_map_").append(type)
                .append(" (user_id, attr_id, value) values(?, ?, ?)");

        try {
            TransactionPeer.runTransaction(new Transaction(persistenceManager) {
                public void process() {
                    Query insertQuery = persistenceManager.newQuery("javax.jdo.query.SQL", queryBuilder.toString());
                    insertQuery.execute(userId, attributeId, value);
                    insertQuery.closeAll();
                }
            });
        } catch (DatabaseException e) {
            //logger.error("Not able to add value " + value + " for attribute " + name + " for item " + i.getItemId(), e);
        }

    }
    
    @SuppressWarnings("unchecked")
	public void addUserSafeAttributeValue(Long userId,String attributeName, Object value, String type, Integer linkType, Boolean demographic) {
    	if(value == null) return;
    	logger.info("AddUSerAttribute: Adding attribute "+attributeName+" with value'"+value+"' for user:"+userId);
    	UserAttributeValueVo<String> uav = null;
    	//VARCHAR value check
    	if(type == "VARCHAR") { 
    		if(value != null & value.toString().length()>Constants.VARCHAR_SIZE) { value = value.toString().substring(0, Constants.VARCHAR_SIZE); }
    	}
		try {
			uav = getScalarUserAttributeValueForUser(userId, attributeName);
		} catch (UnknownUserAttributeException e) {
			// add the attribute if missing
			addAttribute(new UserAttribute(attributeName, type, linkType, demographic));
		}
		//add the attribute value for the user if missing
		if(uav == null) {
			UserAttribute userAttribute = findUserAttributeByName(attributeName);
			addUserAttributeValue(userId, userAttribute.getAttributeId(),value,type);
		}
    }

    

    public UserAttributeValueVo getScalarUserAttributeValueForUser(User user, Integer attributeId) throws UnknownUserAttributeException {
        return getScalarUserAttributeValueForUser(user.getUserId(), attributeId);
    }

    // TODO Refactor these:

    @SuppressWarnings({"unchecked"})
    private UserAttributeValueVo findScalarAttributeValueForUser(Long userId, Integer attributeId, String typeName) {
        StringBuilder queryBuilder = new StringBuilder();
                queryBuilder.append("select value from user_map_").append(typeName).append(" where user_id = ? and attr_id = ?");
        Query valueQuery = persistenceManager.newQuery("javax.jdo.query.SQL", queryBuilder.toString());
        Collection<Object> values = (Collection<Object>) valueQuery.execute(userId, attributeId);
        Iterator<Object> iterator = values.iterator();
        if (!iterator.hasNext()) {
            return null;
        }
        Object value = iterator.next();
        return new UserAttributeValueVo(userId, attributeId, value, typeName);
    }

    // TODO these will need to be amended if we start using POS for scalar user attributes

    @SuppressWarnings({"unchecked"})
    private Collection<Object[]> findScalarAttributeValuesForUser(Long userId, String type) {
        StringBuilder queryStringBuilder = new StringBuilder();
        queryStringBuilder.append("select attr_id, value from user_map_").append(type).append(" where user_id = ?");
        Query query = persistenceManager.newQuery("javax.jdo.query.SQL", queryStringBuilder.toString());
        return (Collection<Object[]>) query.execute(userId);
    }

    @SuppressWarnings({"unchecked"})
    private Collection<Object[]> findCompositeAttributeValuesForUser(Long userId, String type) {
        StringBuilder queryStringBuilder = new StringBuilder();
        queryStringBuilder.append("select attr_id, value_id from user_map_").append(type).append(" where user_id = ?");
        Query query = persistenceManager.newQuery("javax.jdo.query.SQL", queryStringBuilder.toString());
        return (Collection<Object[]>) query.execute(userId);
    }

    @SuppressWarnings({"unchecked"})
    private Collection<Object[]> findCompositeAttributeValueNamesForUser(Long userId, String type) {
        StringBuilder queryStringBuilder = new StringBuilder();
        queryStringBuilder
                .append("select ua.name,uae.value_name from user_map_")
                .append(type).append(" ume ")
                .append("natural join user_attr ua ")
                .append("natural join user_attr_enum uae ")
                .append("where user_id = ?");
        Query query = persistenceManager.newQuery("javax.jdo.query.SQL", queryStringBuilder.toString());
        return (Collection<Object[]>) query.execute(userId);
    }
    
    
	public Map<String,String> getUserAttributesName(long userid) {
		Map<String,String> attributes = new HashMap<>();
		String sql = "SELECT a.name attr_name, CASE WHEN imi.value IS NOT NULL THEN cast(imi.value as char) WHEN imd.value IS NOT NULL THEN cast(imd.value as char) WHEN imb.value IS NOT NULL THEN cast(imb.value as char) WHEN imboo.value IS NOT NULL and imboo.value=0 THEN 'false' WHEN imboo.value IS NOT NULL and imboo.value>0 THEN 'true' WHEN imt.value IS NOT NULL THEN imt.value WHEN imdt.value IS NOT NULL THEN cast(imdt.value as char) WHEN imv.value IS NOT NULL THEN imv.value WHEN e.value_name IS NOT NULL THEN e.value_name END value_id FROM  users u INNER JOIN user_attr a ON u.user_id=? LEFT JOIN user_map_int imi ON u.user_id=imi.user_id AND a.attr_id=imi.attr_id LEFT JOIN user_map_double imd ON u.user_id=imd.user_id AND a.attr_id=imd.attr_id LEFT JOIN user_map_enum ime ON u.user_id=ime.user_id AND a.attr_id=ime.attr_id LEFT JOIN user_map_bigint imb ON u.user_id=imb.user_id AND a.attr_id=imb.attr_id LEFT JOIN user_map_boolean imboo ON u.user_id=imboo.user_id AND a.attr_id=imboo.attr_id LEFT JOIN user_map_text imt ON u.user_id=imt.user_id AND a.attr_id=imt.attr_id LEFT JOIN user_map_datetime imdt ON u.user_id=imdt.user_id AND a.attr_id=imdt.attr_id LEFT JOIN user_map_varchar imv ON u.user_id=imv.user_id AND a.attr_id=imv.attr_id LEFT JOIN user_attr_enum e ON ime.attr_id=e.attr_id AND ime.value_id=e.value_id";
		Query query = persistenceManager.newQuery( "javax.jdo.query.SQL", sql );
		Collection<Object[]> c = (Collection<Object[]>) query.execute(userid);
		for(Object[] array : c) {
			if(array!=null && array[1]!=null) {
				attributes.put((String)array[0],(String)array[1]);
			}
		}
		return attributes;
	}

	public Map<Integer,Integer> getUserAttributes(long userId) {
		Map<Integer,Integer> attributes = new HashMap<>();
		Query query = persistenceManager.newQuery( "javax.jdo.query.SQL", "select a.attr_id,e.value_id from users u inner join user_attr a on u.user_id=? inner join user_map_enum ime on u.user_id=ime.user_id and a.attr_id=ime.attr_id inner join user_attr_enum e on ime.attr_id=e.attr_id and ime.value_id=e.value_id" );
		Collection<Object[]> c = (Collection<Object[]>) query.execute(userId);
		for(Object[] array : c) {
			attributes.put((Integer)array[0],(Integer)array[1]);
		}
		return attributes;
	}
}