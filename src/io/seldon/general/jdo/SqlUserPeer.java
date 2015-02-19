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

import javax.jdo.PersistenceManager;
import javax.jdo.Query;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import io.seldon.api.APIException;
import io.seldon.api.Constants;
import io.seldon.api.Util;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.service.UserService;
import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.SQLErrorPeer;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.general.*;
import org.apache.log4j.Logger;

public class SqlUserPeer extends UserPeer {

	private PersistenceManager pm;
	private static Logger logger = Logger.getLogger(SqlUserPeer.class.getName());


	public SqlUserPeer(PersistenceManager pm) {
		this.pm = pm;
	}

	public Collection<User> getRecentUsers(int limit)
	{
		Query query = pm.newQuery( User.class, " active==true " );
		query.setOrdering("userId desc");
		query.setRange(0, limit);
		Collection<User> c = (Collection<User>) query.execute();
		return c;
	}

	public Collection<User> getActiveUsers(int limit)
	{
		Query query = pm.newQuery( User.class, "active" );
		query.setOrdering("userId desc");
		query.setRange(0, limit);
		Collection<User> c = (Collection<User>) query.execute();
		return c;
	}

	public User getUser(long id) {
		User u = null;
		Query query = pm.newQuery( User.class, "userId == i" );
		query.declareParameters( "java.lang.Long i" );
		Collection<User> c = (Collection<User>) query.execute(id);
		if(!c.isEmpty()) {
			u = c.iterator().next();
		}
		return u;
	}

	public User getUser(String id) {
		User u = null;
		Query query = pm.newQuery( User.class, "clientUserId == i" );
		query.declareParameters( "java.lang.String i" );
		Collection<User> c = (Collection<User>) query.execute(id);
		if(!c.isEmpty()) {
			u = c.iterator().next();
		}
		return u;
	}

	public Collection<UserDimension> getUserDimensions(long id) {
		Query query = pm.newQuery( UserDimension.class, "userId == i" );
		query.declareParameters( "java.lang.Long i" );
		Collection<UserDimension> c = (Collection<UserDimension>) query.execute(id);
		return c;
	}

	public Double getUserAvgRating(long id,int dimension)  {
		if(dimension == Constants.DEFAULT_DIMENSION) {
			return getUser(id).getAvgRating();
		}
		else {
			Query query = pm.newQuery("javax.jdo.query.SQL", "select avg(value) from opinions natural join users natural join item_map_enum where user_id=? and attr_id=? and value_id=?" );
			Integer[] attr;
			try {
				attr = Util.getItemPeer(pm).getAttributes(dimension);
				Collection<Double> c = (Collection<Double>) query.execute(id, attr[0],attr[1]);
				return c.iterator().next();
			} catch (APIException e) {
				return getUser(id).getAvgRating();
			}

		}
	}

	public void updateUserStat(long id) {
		Query query = pm.newQuery("javax.jdo.query.SQL","update (select user_id,avg(value) a,stddev(value)s from opinions where user_id=?) t natural join users set avgrating=a,stddevrating=s;");
		query.execute(id);
		query.closeAll();
	}

    @Override
    public User saveOrUpdate(final User user) {
        // (1) look up user by id
        Long userId = user.getUserId();
        if (userId != null) {
            final User retrievedUser = getUser(userId);
            if (retrievedUser != null) {
                // update -- this is a little tricky; prioritise non-null fields in retrievedUser that are
                // null in the submitted user
                try {
                    TransactionPeer.runTransaction(new Transaction(pm) {
                        public void process() {
                            JdoPeerUtil.updateRetrievedItem(User.class, user, retrievedUser);
                        }
                    });
                } catch (DatabaseException e) {
                    logger.error("Failed to update user with id:" + user.getClientUserId(), e);
                    processDatabaseException(e);
                }
                logger.info("Retrieved user has been modified: " + retrievedUser + "; persisting it.");
                return persistUser(retrievedUser);
            } else {
                // we'll allow the id to be changed since it doesn't actually exist in the DB....
                return persistUser(user);
            }
        } else {
            return persistUser(user);
        }
    }

	@Override
	public User persistUser(final User u) throws APIException {
		try {
			TransactionPeer.runTransaction(new Transaction(pm) {
			    public void process()
			    {
			    	pm.makePersistent(u);
			    }});
		} catch (DatabaseException e)
		{
			logger.error("Failed to Add User with id:" + u.getClientUserId(),e);
            processDatabaseException(e);
        }

		return u;
	}

    private void processDatabaseException(DatabaseException e) throws APIException {
        if ( e.getPlaytxtErrNum() == SQLErrorPeer.SQL_DUPLICATE_KEY ) {
            throw new APIException(APIException.USER_DUPLICATED);
        } else {
            throw new APIException(APIException.INCORRECT_FIELD);
        }
    }

    // TODO quick and dirty -- given that user_map_* isn't mapped, this is less of a headache
    @Override
    public Collection<User> findUsersWithAttributeValuePair(Integer attributeId, Object value, String type) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("select user_id from user_map_").append(type).append(" where attr_id = ? and value = ?");
        Query query = pm.newQuery("javax.jdo.query.SQL", stringBuilder.toString());

        Collection<User> users = new LinkedList<>();
        @SuppressWarnings({"unchecked"})
        Collection<Object> rows = (Collection<Object>) query.execute(attributeId, value);
        for (Object id : rows) {
            User user = getUser((Long) id);
            users.add(user);
        }

        return users;
    }

	public Collection<User> getUserByName(String name, int limit) {
		Query query = pm.newQuery( User.class, "username.matches('(?i).*"+name+".*')");
		query.setRange(0, limit);
		Collection<User> c = (Collection<User>) query.execute();
		return c;
	}

	public Integer[] getAttributes(int demographic) {
		Integer[] res = null;
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select attr_id,value_id from demographic where demo_id=?");
		Collection<Object[]> c = (Collection<Object[]>) query.execute(demographic);
		if(!c.isEmpty()) {
			Object[] ores = (Object[])c.iterator().next();
			res = new Integer[2];
			res[0] = (Integer)ores[0];
			res[1] = (Integer)ores[1];
		}
		return res;
	}

	public String[] getAttributesNames(int demographic) {
		String[] res = null;
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select name,value_name from demographic natural join user_attr_enum natural join user_attr where demo_id=?");
		Collection<Object[]> c = (Collection<Object[]>) query.execute(demographic);
		if(!c.isEmpty()) {
			Object[] ores = (Object[])c.iterator().next();
			res = new String[2];
			res[0] = (String)ores[0];
			res[1] = (String)ores[1];
		}
		return res;
	}

	
	@Override
	public boolean addUserAttribute(long userId, int userType, Map<Integer, Integer> attributes,ConsumerBean c) throws APIException {
		boolean res = true;
		return res;
	}


    @Override
    public boolean addUserAttributeNames(final long userId, final int typeId, Map<String, String> attributes, ConsumerBean c) throws APIException {
        boolean res = true;
        Map<String, String> failedMappings = new HashMap<>();
        UserAttributePeer userAttributePeer = Util.getUserAttributePeer(c);
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            String name = entry.getKey();
            String value = entry.getValue();
            final String final_value;
            final UserAttribute attribute = userAttributePeer.findByNameAndType(name, typeId);
            if (attribute == null) {
                final String failureReason = "Could not find an attribute named '" + name + "' for type: " + typeId;
                failedMappings.put(name, failureReason);
                continue;
            }
            String type = attribute.getType();
            final String sql;
            if (!type.equals(Constants.TYPE_ENUM)) {
                String table = "USER_MAP_" + type.toUpperCase();
                // ~~ BEG kludge to deal with truncation ~~
                int valueLength = value.length();
                logger.info("Value length pre-truncation: " + valueLength);
                if (type.equals(Constants.TYPE_VARCHAR)) {
                    int max = 255;
                    int upper = (max > valueLength) ? valueLength : max;
                    value = value.substring(0, upper);
                } else if (type.equals(Constants.TYPE_TEXT)) {
                    int max = 65535;
                    int upper = (max > valueLength) ? valueLength : max;
                    value = value.substring(0, upper);
                }
                logger.info("Value length post truncation step: " + value.length());
                // ~~ END kludge to deal with truncation ~~
                sql = "insert into " + table + " (user_id,attr_id,value) select user_id,attr_id,value1 from (select " + userId + " as user_id,attr_id, case when type = 'VARCHAR' then val when type = 'TEXT' then val when type = 'INT' then CAST(val as SIGNED) when type = 'BIGINT' then CAST(val as SIGNED) when type = 'ENUM' then CAST(val as SIGNED) when type = 'DOUBLE' then CAST(val as DECIMAL) when type = 'BOOLEAN' then CAST(val as BINARY) when type = 'DATETIME' then CAST(val as BINARY) else null end value1, type from (select ? as val) a, user_attr where name = '" + name + "') a where value1 is not null on duplicate key update value=value1";
            } else {
            	//check if the enumeration exists, if not it's created
            	if(!validateDemographic(name,value,c)) {
                    final String err = "Not possible to create the ENUM value" + value + " for the attribute " + name;
                    failedMappings.put(name, err);
                    logger.error(err + " for item " + userId);
            		continue;
            	}
            	else {
            		sql = "insert into user_map_enum (user_id,attr_id,value_id) select " + userId + ",a.attr_id,e.value_id  from user_attr a inner join user_attr_enum e on a.name='" + name + "' and a.attr_id=e.attr_id and e.value_name=? on duplicate key update value_id=e.value_id";
            	}
            }
            //verify content validity
            boolean valid = true;
            try {
                if (type.equals(Constants.TYPE_BIGINT)) {
                    Long.parseLong(value);
                } else if (type.equals(Constants.TYPE_INT)) {
                    Integer.parseInt(value);
                } else if (type.equals(Constants.TYPE_DOUBLE)) {
                    Double.parseDouble(value);
                } else if (type.equals(Constants.TYPE_BOOLEAN)) {
//                        Boolean.parseBoolean(value);
                    // TODO fix this temporary workaround
                    if (!value.matches("^\\s*[10]\\s*$")) {
                        final boolean enabled = Boolean.parseBoolean(value);
                        value = enabled ? "1" : "0";
                    }
                } else if (type.equals(Constants.TYPE_DATETIME)) {
//						Date.parse(type);
                } else if (type.equals(Constants.TYPE_ENUM)) {
                    //TODO
                }
            } catch (Exception e) {
                valid = false;
                logger.error("Not able to add value: " + value + " for attribute " + name + " for user " + userId,e);
                failedMappings.put(name, "Incompatible value format (" + value + ")");
            }
            //query
            if (valid) {
                try {
                	final_value = value;
                    TransactionPeer.runTransaction(new Transaction(pm) {
                        public void process() {
                            Query query = pm.newQuery("javax.jdo.query.SQL", sql);
                            query.execute(final_value);
                            query.closeAll();
                        }
                    });
                } catch (DatabaseException e) {
                    logger.error("Not able to add value" + value + " for attribute " + name + " for user " + userId, e);
                    failedMappings.put(name, "Problem persisting attribute with value: " + value);
                }
            }
        }
        if (!failedMappings.isEmpty()) {
            APIException exception = new APIException(APIException.INCOMPLETE_ATTRIBUTE_ADDITION);
            exception.setFailureMap(failedMappings);
            throw exception;
        }
        return res;
    }

    /*
	//check if a dimension (attr name - value name) exists. if not creates the entities
    private boolean validateDimension(final int itemType,final String name,final String value,ConsumerBean c) {
		if(ItemService.getDimension(c,name,value) == null) {
			//create the dimension
			final String addAttrEnum = "insert into item_attr_enum (attr_id,value_id,value_name,amount) select a.attr_id,max(e.value_id)+1,?,0 from item_attr a inner join item_attr_enum e on a.name = ? and a.attr_id=e.attr_id";
			final String addDim = "insert into dimension (item_type,attr_id,value_id,trustnetwork) select ?,a.attr_id,e.value_id,false from item_attr a inner join item_attr_enum e on a.name = ? and e.value_name=? and a.attr_id=e.attr_id";
			try {
                TransactionPeer.runTransaction(new Transaction(pm) {
                    public void process() {
                        Query query = pm.newQuery("javax.jdo.query.SQL", addAttrEnum);
                        query.execute(value,name);
                        query.closeAll();
                        query = pm.newQuery("javax.jdo.query.SQL", addDim);
                        query.execute(itemType,name,value);
                        query.closeAll();
                    }
                });
            } catch (DatabaseException e) {
                logger.error("Not able to create dimension for value: " + value + " for attribute " + name + " with item type " + itemType, e);
                return false;
            }
		}
		return true;
	}
     */
    
	//check if a demographic (attr name - value name) exists. if not creates the entities
    private boolean validateDemographic(final String name,final String value,ConsumerBean c) {
		if(UserService.getDemographic(c,name, value) == null) {
			//create the demographic
			final String addAttrEnum = "insert into user_attr_enum (attr_id,value_id,value_name,amount) select a.attr_id,max(e.value_id)+1,?,0 from user_attr a inner join user_attr_enum e on a.name = ? and a.attr_id=e.attr_id";
			final String addDemo = "insert into demographic (attr_id,value_id) select a.attr_id,e.value_id from user_attr a inner join user_attr_enum e on a.name = ? and e.value_name=? and a.attr_id=e.attr_id";
			try {
                TransactionPeer.runTransaction(new Transaction(pm) {
                    public void process() {
                        Query query = pm.newQuery("javax.jdo.query.SQL", addAttrEnum);
                        query.execute(value,name);
                        query.closeAll();
                        query = pm.newQuery("javax.jdo.query.SQL", addDemo);
                        query.execute(name,value);
                        query.closeAll();
                    }
                });
            } catch (DatabaseException e) {
                logger.error("Not able to create demographic for value: " + value + " for attribute " + name, e);
                return false;
            }
		}
		return true;
	}

	public int getDemographic(String attrName, String valName) {
		int res = Constants.DEFAULT_DEMOGRAPHIC;
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select demo_id from demographic d inner join user_attr_enum e on d.attr_id=e.attr_id and d.value_id=e.value_id inner join user_attr a on e.attr_id=a.attr_id where lcase(trim(a.name))=lcase(trim(?)) and lcase(trim(e.value_name))=lcase(trim(?))");
		Collection<Integer> c = (Collection<Integer>) query.execute(attrName,valName);
		if(!c.isEmpty()) {
			res = (Integer)c.iterator().next();
		}
		return res;
	}
    
}
