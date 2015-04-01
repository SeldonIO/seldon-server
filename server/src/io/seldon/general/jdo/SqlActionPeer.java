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

import java.util.Collection;
import java.util.List;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.api.APIException;
import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.general.Action;
import io.seldon.general.ActionPeer;
import io.seldon.general.ActionType;
import org.apache.log4j.Logger;

public class SqlActionPeer extends ActionPeer {

	private static Logger logger = Logger.getLogger(SqlActionPeer.class.getName());

	private PersistenceManager pm;

	public SqlActionPeer(PersistenceManager pm) {
		this.pm = pm;
	}

	public Collection<Action> getRecentActions(int limit)
	{
		Query query = pm.newQuery( Action.class, "" );
		query.setOrdering("actionId desc");
		query.setRange(0, limit);
		Collection<Action> c = (Collection<Action>) query.execute();
		return c;
	}

	public Collection<Action> getItemActions(long itemId, int limit) {
		Query query = pm.newQuery( Action.class, "itemId == i" );
		query.setOrdering("actionId desc");
		query.declareParameters( "java.lang.Long i");
		query.setRange(0, limit);
		Collection<Action> c = (Collection<Action>) query.execute(itemId);
		return c;
	}

	public Collection<Action> getUserActions(long userId,int limit) {
		Query query = pm.newQuery( Action.class, "userId == i" );
		query.setOrdering("actionId desc");
		query.declareParameters( "java.lang.Long i");
		query.setRange(0, limit);
		Collection<Action> c = (Collection<Action>) query.execute(userId);
		return c;
	}

	@Override
	public Collection<Action> getRecentUserActions(String clientUserId,
			int actionType, int limit) {
		Query query = pm.newQuery( Action.class, "clientUserId == i && type == t" );
		query.setOrdering("actionId desc");
		query.declareParameters( "java.lang.String i,java.lang.Integer t");
		query.setRange(0, limit);
		Collection<Action> c = (Collection<Action>) query.execute(clientUserId,actionType);
		return c;
	}
	
	@Override
	public Collection<Action> getRecentUserActions(long userId, int actionType,
			int limit) {
		Query query = pm.newQuery( Action.class, "userId == i && type == t" );
		query.setOrdering("actionId desc");
		query.declareParameters( "java.lang.Long i,java.lang.Integer t");
		query.setRange(0, limit);
		Collection<Action> c = (Collection<Action>) query.execute(userId,actionType);
		return c;
	}

	public Collection<Action> getUserItemActions(long itemId,long userId,int limit) {
		Query query = pm.newQuery( Action.class, "itemId == i &&  userId == u" );
		query.setOrdering("actionId desc");
		query.declareParameters( "java.lang.Long i,java.lang.Long u");
		query.setRange(0, limit);
		Collection<Action> c = (Collection<Action>) query.execute(itemId,userId);
		return c;
	}

    @Override
    public boolean saveOrUpdate(final Action action) {
        // (1) look up action by id
        Long actionId = action.getUserId();
        if (actionId != null) {
            final Action retrievedAction = getAction(actionId);
            if (retrievedAction != null) {
                // update -- this is a little tricky; prioritise non-null fields in retrievedAction that are
                // null in the submitted action
                try {
                    TransactionPeer.runTransaction(new Transaction(pm) {
						public void process() {
							JdoPeerUtil.updateRetrievedItem(Action.class, action, retrievedAction);
						}
					});
                } catch (DatabaseException e) {
                    logger.error("Failed to update action with id:" + action.getActionId(), e);
                    throw new APIException(APIException.INCORRECT_FIELD);
                }
                logger.info("Retrieved action has been modified: " + retrievedAction + "; persisting it.");
                return addAction(retrievedAction);
            } else {
                // we'll allow the id to be changed since it doesn't actually exist in the DB....
                return addAction(action);
            }
        } else {
            return addAction(action);
        }
    }

    public Action getAction(long actionId) {
		Action a = null;
		Query query = pm.newQuery( Action.class, "actionId == i" );
		query.declareParameters( "java.lang.Long i" );
		Collection<Action> c = (Collection<Action>) query.execute(actionId);
		if(!c.isEmpty()) {
			a = c.iterator().next();
		}
		return a;
	}

	@Override
	public boolean addAction(final Action a) {
		boolean res = true;
		try {
			TransactionPeer.runTransaction(new Transaction(pm) {
			    public void process()
			    {
			    	pm.makePersistent(a);
			    }});
		} catch (DatabaseException e)
		{
			logger.error("Failed to addAction",e);
			res=false;
		}

		return res;
	}

	@Override
	public boolean addActionType(final ActionType at) {
		boolean res = true;
		try {
			TransactionPeer.runTransaction(new Transaction(pm) {
			    public void process()
			    {
			    	pm.makePersistent(at);
			    }});
		} catch (DatabaseException e)
		{
			logger.error("Failed to addActionType",e);
			res=false;
		}

		return res;
	}

	@Override
	public ActionType getActionType(int typeId) {
		ActionType at = null;
		Query query = pm.newQuery( ActionType.class, "typeId == i" );
		query.declareParameters( "java.lang.Long i" );
		Collection<ActionType> c = (Collection<ActionType>) query.execute(typeId);
		if(!c.isEmpty()) {
			at = c.iterator().next();
		}
		return at;
	}

	@Override
	public ActionType getActionType(String name) {
		ActionType at = null;
		Query query = pm.newQuery( ActionType.class, "name == n" );
		query.declareParameters( "java.lang.String n" );
		Collection<ActionType> c = (Collection<ActionType>) query.execute(name);
		if(!c.isEmpty()) {
			at = c.iterator().next();
		}
		return at;
	}


	@Override
	public Collection<ActionType> getActionTypes() {
		Query query = pm.newQuery( ActionType.class, "" );
		query.setOrdering("typeId asc");
		return (Collection<ActionType>) query.execute();
	}

	@Override
	public List<Long> getRecentUserActions(long userId) {
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select item_id from actions where user_id=? order by action_id desc limit 20");
		query.setResultClass(Long.class);
		return (List<Long>) query.execute(userId);
	}

	


}
