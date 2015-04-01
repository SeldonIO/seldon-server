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

package io.seldon.api.jdo;

import java.util.Collection;
import java.util.Date;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;
import io.seldon.api.APIException;
import io.seldon.api.Constants;
import io.seldon.api.service.ApiLoggerServer;
import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;

/**
 * @author claudio
 */

public class TokenPeer {
	public static Token findToken(String tokenKey) throws APIException {
		try {
			PersistenceManager pm =  JDOFactory.getPersistenceManager(Constants.API_DB);
			Query query = pm.newQuery(Token.class, "tokenKey == t");
			query.declareParameters("java.lang.String t");
			Collection<Token> c = (Collection<Token>) query.execute(tokenKey);
			if(c!=null && c.size()>0) {
				return c.iterator().next();
			}
			else return null;
		}
		catch(Exception e) {
			ApiLoggerServer.log(new TokenPeer(), e);
			throw new APIException(APIException.INTERNAL_DB_ERROR);
		}
	}
	
	//check if the token is expired and update the active attribute
	public static boolean isExpired(Token t) throws APIException {
		//if token already expired or not valid anymore
		if(!t.isActive())
			return true;
		Date now = new Date();
		long timeDiff = (now.getTime() - t.time.getTime()) / 1000;
		//if token just expired
		if(timeDiff > t.expires_in) {
			invalidateToken(t);
			return true;
		}
		//if token not expired
		return false;
	}

	public static void saveToken(final Token token) throws APIException {
		try {
	    	final PersistenceManager pm =  JDOFactory.getPersistenceManager(Constants.API_DB);
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
					pm.makePersistent(token);
			    }
			});
		}
		catch (DatabaseException e) {
			ApiLoggerServer.log(new TokenPeer(), e);
			throw new APIException(APIException.INTERNAL_DB_ERROR);
		}
	}
	
	public static void invalidateToken(final Token token) throws APIException {
		try {
			final PersistenceManager pm =  JDOFactory.getPersistenceManager(Constants.API_DB);
			TransactionPeer.runTransaction(new Transaction(pm) { 
			    public void process()
			    { 
			    	token.setActive(false);
					pm.makePersistent(token);
			    }
			});
		}
		catch (DatabaseException e) {
			ApiLoggerServer.log(new TokenPeer(), e);
			throw new APIException(APIException.INTERNAL_DB_ERROR);
		}
	}

}
