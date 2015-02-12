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

import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.general.ExplicitLink;
import io.seldon.general.LinkType;
import io.seldon.general.NetworkPeer;
import org.apache.log4j.Logger;
import io.seldon.general.User;

public class SqlNetworkPeer extends NetworkPeer {

	private static Logger logger = Logger.getLogger(SqlNetworkPeer.class.getName());
	
	private PersistenceManager pm;

	public SqlNetworkPeer(PersistenceManager pm) {
		this.pm = pm;
	}
	
	@Override
	public boolean addExplicitLink(final ExplicitLink link) {
		boolean res = true;
		try {
			TransactionPeer.runTransaction(new Transaction(pm) {
				public void process() {
					pm.makePersistent(link);
				}
			});
		} catch (DatabaseException e)
		{
			logger.info("Failed to addExplicitLink, this link might be a duplicate");
			res=false;
		}
		
		return res;
	}
	
	@Override
	public ExplicitLink getExplicitLink(long u1,long u2,int type) {
		ExplicitLink l = null;
		Query query = pm.newQuery( ExplicitLink.class, "type == t && u1 == u && u2 == uu" );
		query.declareParameters( "java.lang.Integer t, java.lang.Long u, java.lang.Long uu" );
		Collection<ExplicitLink> c = (Collection<ExplicitLink>) query.execute(type,u1,u2);
		if(!c.isEmpty()) {
			l = c.iterator().next();
		}
		return l;
	}

	@Override
	public LinkType getLinkType(int id) {
		LinkType l = null;
		Query query = pm.newQuery( LinkType.class, "typeId == i" );
		query.declareParameters( "java.lang.Long i" );
		Collection<LinkType> c = (Collection<LinkType>) query.execute(id);
		if(!c.isEmpty()) {
			l = c.iterator().next();
		}
		return l;
	}

	@Override
	public LinkType getLinkType(String name) {
		LinkType l = null;
		Query query = pm.newQuery( LinkType.class, "name == i" );
		query.declareParameters( "java.lang.String i" );
		Collection<LinkType> c = (Collection<LinkType>) query.execute(name);
		if(!c.isEmpty()) {
			l = c.iterator().next();
		}
		return l;
	}

	@Override
	public boolean addLinkType(final LinkType l) {
		boolean res = true;
		try {
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	pm.makePersistent(l);
			    }});
		} catch (DatabaseException e) 
		{
			logger.error("Failed to add LinkType",e);
			res=false;
		}
		
		return res;
		
	}

	@Override
	public Collection<User> getLinkedUsers(long userId, String linkType, int limit) {
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select user_id,username,first_op,last_op,type,num_op,active,client_user_id from (select u.* from users u, links l, link_type lt where lt.name=? and l.type=lt.type_id and l.u1=? and l.u2=u.user_id) a");
		query.setClass(User.class);
		query.setRange(0, limit);
		Collection<User> users = (List<User>) query.execute(linkType,userId);
		return users;
	}
}
