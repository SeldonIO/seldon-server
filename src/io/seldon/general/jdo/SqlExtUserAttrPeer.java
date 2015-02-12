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
import io.seldon.db.jdo.DatabaseException;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.general.ExtUserAttrPeer;
import io.seldon.general.ExternalUserAttribute;
import org.apache.log4j.Logger;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;
import java.sql.SQLException;
import java.util.*;

/**
 * @author philipince
 *         Date: 14/03/2014
 *         Time: 12:33
 */
public class SqlExtUserAttrPeer implements ExtUserAttrPeer {

    private final PersistenceManager manager;
    private final Logger logger = Logger.getLogger(SqlExtUserAttrPeer.class);

    public SqlExtUserAttrPeer(PersistenceManager persistenceManager){
        this.manager = persistenceManager;
    }
    @Override
    public void addUsrAttr(final ExternalUserAttribute attr) {
        try {
            final ExternalUserAttribute attrInDb = retrieve(attr.getUid(), attr.type);
            if (attrInDb == null) {
                TransactionPeer.runTransaction(new Transaction(manager) {
                    @Override
                    public void process() throws DatabaseException, SQLException {
                        manager.makePersistent(attr);
                    }
                });
            } else {
                if(!attrInDb.getValue().equals(attr.getValue())) {
                    TransactionPeer.runTransaction(new Transaction(manager) {
                        @Override
                        public void process() throws DatabaseException, SQLException {

                            attrInDb.setValue(attr.getValue());
                        }
                    });
                };
            }
        } catch (DatabaseException e){
            logger.error("Database exception received when saving new ExternalUserAttribute :" + attr, e);
            throw new APIException(APIException.GENERIC_ERROR);
        }
    }

    @Override
    public Map<String, Boolean> retrieve(Collection<String> userIds, String type){
        Map<String, Boolean > toReturn = new HashMap<String, Boolean>();
        Query q = manager.newQuery(ExternalUserAttribute.class, ":p1.contains(uid) && type == :p2");
        List<ExternalUserAttribute> objects = (List<ExternalUserAttribute>) q.execute(new ArrayList<String>(userIds), type);
        for(ExternalUserAttribute objectArr: objects){
            toReturn.put(objectArr.uid, Boolean.parseBoolean(objectArr.value));
        }
        for(String userId : userIds){
            if(!toReturn.containsKey(userId)){
                logger.warn("Interaction with userid2 = "+userId + " exists but the external user attr does not (in the DB). Registering as app-user conversion.");
                toReturn.put(userId, true);
            }
        }
        return toReturn;
    }

    private ExternalUserAttribute retrieve(String userId, String type){
        Query q = manager.newQuery(ExternalUserAttribute.class, ":p1.equals(uid) && type == :p2");
        q.setUnique(true);
        ExternalUserAttribute userAttribute = (ExternalUserAttribute)q.execute(userId, type);
        return userAttribute;

    }
}
