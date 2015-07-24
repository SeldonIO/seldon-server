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

import io.seldon.api.APIException;
import io.seldon.api.Constants;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.DimensionBean;
import io.seldon.api.resource.service.ItemService;
import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.SQLErrorPeer;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.general.Dimension;
import io.seldon.general.Item;
import io.seldon.general.ItemAttr;
import io.seldon.general.ItemDemographic;
import io.seldon.general.ItemPeer;
import io.seldon.general.ItemType;
import io.seldon.util.CollectionTools;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class SqlItemPeer extends ItemPeer {

	private PersistenceManager pm;
	private static Logger logger = Logger.getLogger(SqlItemPeer.class.getName());

	public SqlItemPeer(PersistenceManager pm) {
		this.pm = pm;
	}

	//ADD FURTHER DIMENSION FILTER (ATTR_ID&&VAL_ID<>NULL)
	public Collection<Item> getItems(int limit,int dimension,ConsumerBean cb) {
		Integer type = null;
		//check if there is a filter over item_type
		if(dimension != Constants.DEFAULT_DIMENSION) {
			DimensionBean d = ItemService.getDimension(cb, dimension);
			type = d.getItemType();
		}
		Query query = pm.newQuery( Item.class, "" );
		if(type != null) { query.setFilter("type == " + type); }
		query.setOrdering("itemId desc");
		query.setRange(0, limit);
		Collection<Item> c = (Collection<Item>) query.execute();
		return c;
	}

	@Override
	public Collection<Item> getItems(int skip,int limit) {
		Query query = pm.newQuery( Item.class, "" );
		query.setRange(skip, limit);
		Collection<Item> c = (Collection<Item>) query.execute();
		return c;
	}

	//ADD FURTHER DIMENSION FILTER (ATTR_ID&&VAL_ID<>NULL)
	public Collection<Item> getRecentItems(int limit, int dimension,ConsumerBean cb) {
		Integer type = null;
		//check if there is a filter over item_type
		if(dimension != Constants.DEFAULT_DIMENSION) {
			DimensionBean d = ItemService.getDimension(cb, dimension);
			type = d.getItemType();
		}
		Query query = pm.newQuery( Item.class, "" );
		if(type != null) { query.setFilter("type == " + type); }
		query.setOrdering("itemId desc");
		query.setRange(0, limit);
		Collection<Item> c = (Collection<Item>) query.execute();
		return c;
	}

	//ADD FURTHER DIMENSION FILTER (ATTR_ID&&VAL_ID<>NULL)
	public Collection<Item> getAlphabeticItems(int limit,int dimension,ConsumerBean cb) {
		Integer type = null;
		//check if there is a filter over item_type
		if(dimension != Constants.DEFAULT_DIMENSION) {
			DimensionBean d = ItemService.getDimension(cb, dimension);
			type = d.getItemType();
		}
		Query query = pm.newQuery( Item.class, "" );
		if(type != null) { query.setFilter("type == " + type); }
		query.setOrdering("name asc,itemId desc");
		query.setRange(0, limit);
		Collection<Item> c = (Collection<Item>) query.execute();
		return c;
	}

	public Map<String,String> getItemAttributesName(long userid) {
		Map<String,String> attributes = new HashMap<>();
		String sql = "SELECT a.name attr_name, CASE WHEN imi.value IS NOT NULL THEN cast(imi.value as char) WHEN imd.value IS NOT NULL THEN cast(imd.value as char) WHEN imb.value IS NOT NULL THEN cast(imb.value as char) WHEN imboo.value IS NOT NULL THEN cast(imboo.value as char) WHEN imt.value IS NOT NULL THEN imt.value WHEN imdt.value IS NOT NULL THEN cast(imdt.value as char) WHEN imv.value IS NOT NULL THEN imv.value WHEN e.value_name IS NOT NULL THEN e.value_name END value_id FROM  items i INNER JOIN item_attr a ON i.item_id=? and i.type=a.item_type LEFT JOIN item_map_int imi ON i.item_id=imi.item_id AND a.attr_id=imi.attr_id LEFT JOIN item_map_double imd ON i.item_id=imd.item_id AND a.attr_id=imd.attr_id LEFT JOIN item_map_enum ime ON i.item_id=ime.item_id AND a.attr_id=ime.attr_id LEFT JOIN item_map_bigint imb ON i.item_id=imb.item_id AND a.attr_id=imb.attr_id LEFT JOIN item_map_boolean imboo ON i.item_id=imboo.item_id AND a.attr_id=imboo.attr_id LEFT JOIN item_map_text imt ON i.item_id=imt.item_id AND a.attr_id=imt.attr_id LEFT JOIN item_map_datetime imdt ON i.item_id=imdt.item_id AND a.attr_id=imdt.attr_id LEFT JOIN item_map_varchar imv ON i.item_id=imv.item_id AND a.attr_id=imv.attr_id LEFT JOIN item_attr_enum e ON ime.attr_id =e.attr_id AND ime.value_id=e.value_id";
		Query query = pm.newQuery( "javax.jdo.query.SQL", sql );
		Collection<Object[]> c = (Collection<Object[]>) query.execute(userid);
		for(Object[] array : c) {
			if(array!=null && array[1]!=null) {
				String currentValue = attributes.get((String)array[0]);
				String newValue = (String)array[1];
				if(currentValue!=null && !currentValue.isEmpty()) { newValue = currentValue + "," + newValue; }
				attributes.put((String)array[0],newValue);
			}
		}
		return attributes;
	}

	public Map<Integer,Integer> getItemAttributes(long itemId) {
		Map<Integer,Integer> attributes = new HashMap<>();
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select a.attr_id,e.value_id from items i inner join item_attr a on i.item_id=? inner join item_map_enum ime on i.item_id=ime.item_id and a.attr_id=ime.attr_id inner join item_attr_enum e on ime.attr_id=e.attr_id and ime.value_id=e.value_id" );
		Collection<Object[]> c = (Collection<Object[]>) query.execute(itemId);
		for(Object[] array : c) {
			attributes.put((Integer)array[0],(Integer)array[1]);
		}
		return attributes;
	}

	//useful in particular for multiple values (tags)
	public Collection<String> getItemAttributesNameByAttrName(long itemId,String attrName) {
		String sql = "SELECT CASE WHEN imi.value IS NOT NULL THEN cast(imi.value as char) WHEN imd.value IS NOT NULL THEN cast(imd.value as char) WHEN imb.value IS NOT NULL THEN cast(imb.value as char) WHEN imboo.value IS NOT NULL THEN cast(imboo.value as char) WHEN imt.value IS NOT NULL THEN imt.value WHEN imdt.value IS NOT NULL THEN cast(imdt.value as char) WHEN imv.value IS NOT NULL THEN imv.value WHEN e.value_name IS NOT NULL THEN e.value_name END value_id FROM  items i INNER JOIN item_attr a ON i.item_id=? and a.name=? LEFT JOIN item_map_int imi ON i.item_id=imi.item_id AND a.attr_id=imi.attr_id LEFT JOIN item_map_double imd ON i.item_id=imd.item_id AND a.attr_id=imd.attr_id LEFT JOIN item_map_enum ime ON i.item_id=ime.item_id AND a.attr_id=ime.attr_id LEFT JOIN item_map_bigint imb ON i.item_id=imb.item_id AND a.attr_id=imb.attr_id LEFT JOIN item_map_boolean imboo ON i.item_id=imboo.item_id AND a.attr_id=imboo.attr_id LEFT JOIN item_map_text imt ON i.item_id=imt.item_id AND a.attr_id=imt.attr_id LEFT JOIN item_map_datetime imdt ON i.item_id=imdt.item_id AND a.attr_id=imdt.attr_id LEFT JOIN item_map_varchar imv ON i.item_id=imv.item_id AND a.attr_id=imv.attr_id LEFT JOIN item_attr_enum e ON ime.attr_id =e.attr_id AND ime.value_id=e.value_id order by imv.pos";
		Query query = pm.newQuery( "javax.jdo.query.SQL", sql );
		return (Collection<String>) query.execute(itemId,attrName);
	}

	public Item getItem(long id) {
		Item i = null;
		Query query = pm.newQuery( Item.class, "itemId == i" );
		query.declareParameters( "java.lang.Long i" );
		Collection<Item> c = (Collection<Item>) query.execute(id);
		if(!c.isEmpty()) {
			i = c.iterator().next();
		}
		return i;
	}

	public Item getItem(String id) {
		Item i = null;
		Query query = pm.newQuery( Item.class, "clientItemId == i" );
		query.declareParameters( "java.lang.String i" );
		Collection<Item> c = (Collection<Item>) query.execute(id);
		if(!c.isEmpty()) {
			i = c.iterator().next();
		}
		return i;
	}

	public Integer[] getAttributes(int dimension) {
		Integer[] res = null;
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select attr_id,value_id from dimension where dim_id=?");
		Collection<Object[]> c = (Collection<Object[]>) query.execute(dimension);
		if(!c.isEmpty()) {
			Object[] ores = (Object[])c.iterator().next();
			res = new Integer[2];
			res[0] = (Integer)ores[0];
			res[1] = (Integer)ores[1];
		}
		return res;
	}

	public String[] getAttributesNames(int dimension) {
		String[] res = null;
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select a.name,value_name from dimension d inner join item_attr_enum e on d.attr_id = e.attr_id and d.value_id=e.value_id inner join item_attr a on e.attr_id=a.attr_id where dim_id=?");
		Collection<Object[]> c = (Collection<Object[]>) query.execute(dimension);
		if(!c.isEmpty()) {
			Object[] ores = (Object[])c.iterator().next();
			res = new String[2];
			res[0] = (String)ores[0];
			res[1] = (String)ores[1];
		}
		return res;
	}

	public int getDimension(int attr,int val) {
		int res = Constants.DEFAULT_DIMENSION;
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select dim_id from dimension where attr_id=? and value_id=?");
		Collection<Integer> c = (Collection<Integer>) query.execute(attr,val);
		if(!c.isEmpty()) {
			res = (Integer)c.iterator().next();
		}
		return res;
	}

	public int getDimension(String attrName, String valName) {
		int res = Constants.DEFAULT_DIMENSION;
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select dim_id from dimension d inner join item_attr_enum e on d.attr_id=e.attr_id and d.value_id=e.value_id inner join item_attr a on e.attr_id=a.attr_id where lcase(trim(a.name))=lcase(trim(?)) and lcase(trim(e.value_name))=lcase(trim(?))");
		Collection<Integer> c = (Collection<Integer>) query.execute(attrName,valName);
		if(!c.isEmpty()) {
			res = (Integer)c.iterator().next();
		}
		return res;
	}

	public double getItemAvgRating(long id,int dimension) {
		if(dimension == Constants.DEFAULT_DIMENSION) {
			return getItem(id).getAvgRating();
		}
		//an item is only in one dimension
		else {
			return getItem(id).getAvgRating();
		}
	}


	@Override
	public Collection<Dimension> getDimensions() {
		Collection<Dimension> res = new ArrayList<>();
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select d.dim_id,d.item_type,d.attr_id,d.value_id,a.name,e.value_name,d.trustnetwork from dimension d inner join item_attr_enum e on d.attr_id=e.attr_id and d.value_id=e.value_id inner join item_attr a on e.attr_id=a.attr_id union all select dim_id,d.item_type,attr_id,value_id,'itemType',name,trustnetwork from dimension d inner join item_type t on d.item_type=t.type_id and d.attr_id is null and d.value_id is null");
		Collection<Object[]> c = (Collection<Object[]>) query.execute();
		for(Object[] ores : c) {
			Dimension d = new Dimension();
			d.setDimId((Integer)ores[0]);
			d.setItemType((Integer)ores[1]);
			d.setAttr((Integer)ores[2]);
			d.setVal((Integer)ores[3]);
			d.setAttrName((String)ores[4]);
			d.setValName((String)ores[5]);
			Integer trustNetwork = (Integer)ores[6];
			if(trustNetwork!= null && trustNetwork >0) {
				d.setTrustNetwork(true);
			}
			else {
				d.setTrustNetwork(false);
			}
			res.add(d);
		}
		return res;
	}

    @Override
    public Item saveOrUpdate(final Item item, ConsumerBean consumerBean) {
        // (1) look up item by id
        Long itemId = item.getItemId();
        if (itemId != null) {
            final Item retrievedItem = getItem(itemId);
            if (retrievedItem != null) {
                // update -- this is a little tricky; prioritise non-null fields in retrievedItem that are
                // null in the submitted item
                try {
                    TransactionPeer.runTransaction(new Transaction(pm) {
						public void process() {
							JdoPeerUtil.updateRetrievedItem(Item.class, item, retrievedItem);
						}
					});
                } catch (DatabaseException e) {
                    logger.error("Failed to update item with id:" + item.getClientItemId(), e);
                    processDatabaseException(e);
                }
                logger.info("Retrieved item has been modified: " + retrievedItem + "; persisting it.");
                return addItem(retrievedItem, consumerBean);
            } else {
                // we'll allow the id to be changed since it doesn't actually exist in the DB....
                return addItem(item, consumerBean);
            }
        } else {
            return addItem(item, consumerBean);
        }
    }

    @Override
	public Item addItem(final Item i,ConsumerBean c) throws APIException {
		try {
			TransactionPeer.runTransaction(new Transaction(pm) {
			    public void process()
			    {
			    	pm.makePersistent(i);
			    }});
		} catch (DatabaseException e)
		{
			logger.error("Failed to Add Item with id:" + i.getClientItemId(),e);
			processDatabaseException(e);
		}

		return i;
	}

        private void processDatabaseException(DatabaseException e) throws APIException {
        if ( e.getPlaytxtErrNum() == SQLErrorPeer.SQL_DUPLICATE_KEY ) {
            throw new APIException(APIException.ITEM_DUPLICATED);
        } else {
            throw new APIException(APIException.INCORRECT_FIELD);
        }
    }


	@Override
	public boolean addItemAttribute(long itemId, int itemType, Map<Integer, Integer> attributes,ConsumerBean c) throws APIException {
		boolean res = true;
		return res;
	}

	@Override
	public boolean addItemAttributeNames(long itemId, int itemType, Map<String, String> attributes, ConsumerBean c) throws APIException {
		boolean res = true;
        Map<String, String> failedMappings = new HashMap<>();
		for(Map.Entry<String, String> entry : attributes.entrySet()) {
            String name = entry.getKey();
            String value = entry.getValue();
            final String final_value;
            String type = ItemService.getAttrType(c, itemType, name);
            if (type == null) {
                final String failureReason = "Could not find an attribute named '" + name + "' for type: " +itemType;
                logger.error(failureReason + " for item " + itemId);
                failedMappings.put(name, failureReason);
                continue;
            }
            // TODO problem fetching type
            final String sql;
            if (!type.equals(Constants.TYPE_ENUM)) {
                String table = "ITEM_MAP_" + type.toUpperCase();
                // ~~ BEG kludge to deal with truncation ~~
                int valueLength = value.length();
                //logger.info("Value length pre-truncation: " + valueLength);
                if (type.equals(Constants.TYPE_VARCHAR)) {
                    int max = 255;
                    int upper = (max > valueLength) ? valueLength : max;
                    value = value.substring(0, upper);
                } else if (type.equals(Constants.TYPE_TEXT)) {
                    int max = 65535;
                    int upper = (max > valueLength) ? valueLength : max;
                    value = value.substring(0, upper);
                }
                //logger.info("Value length post truncation step: " + value.length());
                if ( value.length() < valueLength ) {
                    logger.info("Truncated value attribute " + name + ", item " + itemId);
                }
                // ~~ END kludge to deal with truncation ~~
                sql = "insert into " + table + " (item_id,attr_id,value) select item_id,attr_id,value1 from (select " + itemId + " as item_id,attr_id, case when type = 'VARCHAR' then val when type = 'TEXT' then val when type = 'INT' then CAST(val as SIGNED) when type = 'BIGINT' then CAST(val as SIGNED) when type = 'ENUM' then CAST(val as SIGNED) when type = 'DOUBLE' then CAST(val as DECIMAL) when type = 'BOOLEAN' then CAST(val as BINARY) when type = 'DATETIME' then CAST(val as BINARY) else null end value1, type from (select ? as val) a, item_attr where name = '" + name + "') a where value1 is not null on duplicate key update value=value1";
            } else {
            	//check if the enumeration exists, if not it's created
            	if(!validateDimension(itemType,name,value,c)) {
                    final String err = "Not possible to create the ENUM value" + value + " for the attribute " + name;
                    failedMappings.put(name, err);
                    logger.error(err + " for item " + itemId);
            		continue;
            	}
            	else {
            		sql = "insert into item_map_enum (item_id,attr_id,value_id) select " + itemId + ",a.attr_id,e.value_id value_id1  from item_attr a inner join item_attr_enum e on a.item_type=" + itemType + " and a.name='" + name + "' and a.attr_id=e.attr_id and e.value_name=? on duplicate key update value_id=e.value_id";
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
                    // this is a NOOP
                    //Boolean.parseBoolean(type);
                } else if (type.equals(Constants.TYPE_DATETIME)) {
                    // TODO 
//					Date.parse(type);
                } else if (type.equals(Constants.TYPE_ENUM)) {
                    //TODO
                }
            } catch (Exception e) {
                valid = false;
                logger.error("Not able to add value: " + value + " for attribute " + name + " for item " + itemId, e);
                failedMappings.put(name, "Incompatible value format (" + value + ")");
            }

            //query
            if (valid) {
            	final_value = value;
                try {
                    TransactionPeer.runTransaction(new Transaction(pm) {
                        public void process() {
                            Query query = pm.newQuery("javax.jdo.query.SQL", sql);
                            query.execute(final_value);
                            query.closeAll();
                        }
                    });
                } catch (DatabaseException e) {
                    logger.error("Not able to add value: " + value + " for attribute " + name + " for item " + itemId, e);
                    failedMappings.put(name, "Problem persisting attribute with value: " + value);
                }
            }
        }
        if ( ! failedMappings.isEmpty()) {
            APIException exception = new APIException(APIException.INCOMPLETE_ATTRIBUTE_ADDITION);
            exception.setFailureMap(failedMappings);
            throw exception;
        }
		return res;
	}

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

	@Override
	public long getMinItemId(Date after, Integer dimension,ConsumerBean cb) {
		String typeFilter = "";
		if(dimension != null && dimension != Constants.DEFAULT_DIMENSION) {
			DimensionBean d = ItemService.getDimension(cb, dimension);
			Integer type = d.getItemType();
			if(type!=null) { typeFilter = " and type ="+ type; }
		}
		Query query = pm.newQuery( Item.class, "lastOp >= d" + typeFilter );
		query.declareParameters("java.util.Date d");
		query.setRange(0, 1);
		query.setOrdering("lastOp ASC,itemId ASC");
		Collection<Item> c = (Collection<Item>) query.execute(after);
		if (c.size() >= 1)
			return c.iterator().next().getItemId();
		else
			return 0L;
	}
	
	@Override
	public Collection<Item> getItemsByName(String name, int limit, int dimension,ConsumerBean cb) {
		String typeFilter = "";
		if(dimension != Constants.DEFAULT_DIMENSION) {
			DimensionBean d = ItemService.getDimension(cb, dimension);
			Integer type = d.getItemType();
			if(type!=null) { typeFilter = " && type =="+ type; }
		}
		Query query = pm.newQuery( Item.class, "name.matches('(?i).*"+name+".*')" + typeFilter );
		query.setRange(0, limit);
		Collection<Item> c = (Collection<Item>) query.execute();
		return c;
	}

	@Override
	public ItemAttr getItemAttr(int itemType, String attrName) {
		ItemAttr a = null;
		Query query = pm.newQuery( ItemAttr.class, "name == a" );
		query.declareParameters( "java.lang.String a" );
		Collection<ItemAttr> c = (Collection<ItemAttr>) query.execute(attrName);
		if(!c.isEmpty()) {
			a = c.iterator().next();
		}
		return a;
	}

	@Override
	public ItemType getItemType(String name) {
		ItemType t = null;
		Query query = pm.newQuery( ItemType.class, "name == a" );
		query.declareParameters( "java.lang.String a" );
		Collection<ItemType> c = (Collection<ItemType>) query.execute(name);
		if(!c.isEmpty()) {
			t = c.iterator().next();
		}
		return t;
	}

	@Override
	public ItemType getItemType(int typeId) {
		ItemType t = null;
		Query query = pm.newQuery( ItemType.class, "typeId == a" );
		query.declareParameters( "java.lang.Integer a" );
		Collection<ItemType> c = (Collection<ItemType>) query.execute(typeId);
		if(!c.isEmpty()) {
			t = c.iterator().next();
		}
		return t;
	}

	@Override
	public Collection<Integer> getItemDimensions(long itemId) {
		Collection<Integer> res = new ArrayList<>();
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select dim_id from item_map_enum e inner join dimension d on e.attr_id=d.attr_id and e.value_id=d.value_id where item_id="+itemId+" order by dim_id;");
		return (Collection<Integer>) query.execute();
	}
	
	
	public Integer getItemCluster(long itemId) {
		Collection<Integer> res = new ArrayList<>();
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select cluster_id from item_clusters where item_id="+itemId);
		query.setResultClass(Integer.class);
		query.setUnique(true);
		return (Integer) query.execute();
	}


	public int getDimensionItemType(int dimension) {
		int res = Constants.DEFAULT_ITEM_TYPE;
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select item_type from dimension where dim_id=?");
		Collection<Integer> c = (Collection<Integer>) query.execute(dimension);
		if(!c.isEmpty()) {
			res = (Integer)c.iterator().next();
		}
		return res;
	}

	public Integer getDimensionByItemType(int itemtype) {
		Integer res = null;
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select dim_id from dimension where item_type=? and attr_id is null and value_id is null");
		Collection<Integer> c = (Collection<Integer>) query.execute(itemtype);
		if(!c.isEmpty()) {
			res = (Integer)c.iterator().next();
		}
		return res;
	}

	public Collection<ItemDemographic> getItemDemographics(long itemId) {
		Query query = pm.newQuery( ItemDemographic.class, "itemId == i" );
		query.declareParameters( "java.lang.Long i" );
		Collection<ItemDemographic> c = (Collection<ItemDemographic>) query.execute(itemId);
		return c;
	}

	@Override
	public Collection<Item> getItemsFromUserActions(long userId, String actionType, int limit) {
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select item_id,name,first_op,last_op,popular,type,client_item_id from (select i.* from actions a, action_type at,items i where at.name=? and at.type_id=a.type and a.user_id=? and i.item_id=a.item_id group by i.item_id order by a.date desc) i" );
		query.setClass(Item.class);
		query.setRange(0, limit);
		Collection<Item> items = (List<Item>) query.execute(actionType, userId);
		return items;
	}

	@Override
	public Collection<ItemType> getItemTypes() {
		Query query = pm.newQuery( ItemType.class, "" );
		query.setOrdering("typeId asc");
		return (Collection<ItemType>) query.execute();
	}

	@Override
	public List<String> getItemSemanticAttributes(long itemId) {
		Query query = pm.newQuery("javax.jdo.query.SQL","SELECT CASE WHEN imi.value IS NOT NULL THEN cast(imi.value as char) WHEN imd.value IS NOT NULL THEN cast(imd.value as char) WHEN imb.value IS NOT NULL THEN cast(imb.value as char) WHEN imboo.value IS NOT NULL THEN cast(imboo.value as char) WHEN imt.value IS NOT NULL THEN imt.value WHEN imdt.value IS NOT NULL THEN cast(imdt.value as char) WHEN imv.value IS NOT NULL THEN imv.value WHEN e.value_name IS NOT NULL THEN e.value_name END value_id FROM  items i INNER JOIN item_attr a ON i.item_id=? and a.semantic = true and i.type=a.item_type LEFT JOIN item_map_int imi ON i.item_id=imi.item_id AND a.attr_id=imi.attr_id LEFT JOIN item_map_double imd ON i.item_id=imd.item_id AND a.attr_id=imd.attr_id LEFT JOIN item_map_enum ime ON i.item_id=ime.item_id AND a.attr_id=ime.attr_id LEFT JOIN item_map_bigint imb ON i.item_id=imb.item_id AND a.attr_id=imb.attr_id LEFT JOIN item_map_boolean imboo ON i.item_id=imboo.item_id AND a.attr_id=imboo.attr_id LEFT JOIN item_map_text imt ON i.item_id=imt.item_id AND a.attr_id=imt.attr_id LEFT JOIN item_map_datetime imdt ON i.item_id=imdt.item_id AND a.attr_id=imdt.attr_id LEFT JOIN item_map_varchar imv ON i.item_id=imv.item_id AND a.attr_id=imv.attr_id LEFT JOIN item_attr_enum e ON ime.attr_id =e.attr_id AND ime.value_id=e.value_id order by imv.pos");
		List<String> res = new ArrayList<>();
		Collection<String> col = (Collection<String>) query.execute(itemId);
		if(col!=null && col.size()>0) { res = new ArrayList<>(col); }
		return res;

	}

	@Override
	public List<Long> getRecentItemIds(Set<Integer> dimensions, int limit, ConsumerBean c) {
		Query query;
		if (dimensions.isEmpty() || (dimensions.size() == 1 && dimensions.iterator().next() == Constants.DEFAULT_DIMENSION))
			query = pm.newQuery("javax.jdo.query.SQL","select i.item_id from items i order by i.item_id desc limit "+limit);			
		else
			query = pm.newQuery("javax.jdo.query.SQL","select i.item_id from items i natural join item_map_enum e join dimension d on (d.dim_id in ("+StringUtils.join(dimensions, ",")+") and e.attr_id=d.attr_id and e.value_id=d.value_id and i.type=d.item_type) order by i.item_id desc limit "+limit);

		query.setResultClass(Long.class);
		Collection<Long> res = (Collection<Long>) query.execute();
		List<Long> resf = new ArrayList<>(res);
		query.closeAll();
		return resf;
	}

	@Override
	public List<Long> getRecentItemIdsWithTags(int tagAttrId,Set<String> tags, int limit) {
		Query query;
		query = pm.newQuery("javax.jdo.query.SQL","select item_id,value from item_map_varchar where attr_id=? order by item_id desc limit "+limit);			

		Collection<Object[]> results = (Collection<Object[]>) query.execute(tagAttrId);
		List<Long> resf = new ArrayList<>();
		for(Object[] r : results)
		{
			Long itemId = (Long) r[0];
			String itemTags = (String) r[1];
			String[] parts = itemTags.split(",");
			for(int i=0;i<parts.length;i++)
				if (tags.contains(parts[i].toLowerCase().trim()))
				{
					resf.add(itemId);
					break;
				}
		}
		query.closeAll();
		return resf;
	}

	
	
	@Override
	public Map<Long, List<String>> getRecentItemTags(Set<Long> ids, int attrId,String table) {
		Map<Long,List<String>> res = new HashMap<>();
		if (ids != null && ids.size() > 0)
		{
			String idStr = CollectionTools.join(ids, ",");
			Query query = pm.newQuery("javax.jdo.query.SQL","select item_id,value from item_map_"+table+" where attr_id="+attrId+" and item_id in ("+idStr+")");				
			Collection<Object[]> results = (Collection<Object[]>) query.execute();

			for(Object[] r : results)
			{
				Long itemId = (Long) r[0];
				String tags = (String) r[1];
				String[] parts = tags.split(",");
				List<String> tagList = new ArrayList<>();
				for(int i=0;i<parts.length;i++)
				{
					String tag = StringUtils.trimToEmpty(parts[i]);
					if (!StringUtils.isEmpty(tag))
						tagList.add(tag);
				}
				if (tagList.size() > 0)
					res.put(itemId, tagList);
			}
			query.closeAll();
		}
		return res;
	}

	@Override
	public Integer getDimensionForAttrName(long itemId, String name) {
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select dim_id from dimension natural join item_map_enum join item_attr on (item_attr.attr_id=item_map_enum.attr_id) where item_attr.name=? and item_map_enum.item_id=?");
		query.setResultClass(Integer.class);
		query.setUnique(true);
		return (Integer) query.execute(name,itemId);
	}

	@Override
	public List<ItemAndScore> retrieveMostPopularItems(int numItems, Set<Integer> dimensions){
		Query query;
		Collection<Object[]> results;
		List<ItemAndScore> toReturn = new ArrayList<>();
		if (dimensions.isEmpty() || (dimensions.size() == 1 && dimensions.iterator().next() == Constants.DEFAULT_DIMENSION))
		{
			query = pm.newQuery("javax.jdo.query.SQL", "select p.item_id, p.score from items_recent_popularity p order by p.score desc limit ?");
			results = (Collection<Object[]>) query.execute(numItems);			
		}
		else
		{
			query = pm.newQuery("javax.jdo.query.SQL", "select i.item_id, p.score " +
					"from items i " +
					"natural join item_map_enum e " +
					"join dimension d on (d.dim_id in ("+StringUtils.join(dimensions, ",")+") and e.attr_id=d.attr_id and e.value_id=d.value_id and i.type=d.item_type) " +
					"natural join items_recent_popularity p ORDER BY p.score desc LIMIT ?");

			results = (Collection<Object[]>) query.execute(numItems);
		}
		for(Object[] r : results)
		{
			Long itemId = (Long) r[0];
			Double score =  ((Float)r[1]).doubleValue();
			toReturn.add(new ItemAndScore(itemId, score));
		}

		return toReturn;
	}


	public static class ItemAndScore implements Serializable {
		public final Long item;
		public final Double score;

		public ItemAndScore(Long item, Double score) {
			this.item = item;
			this.score = score;
		}
	}


	

}

