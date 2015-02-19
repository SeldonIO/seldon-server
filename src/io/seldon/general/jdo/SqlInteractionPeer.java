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

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.general.InteractionEvent;
import io.seldon.api.APIException;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;
import io.seldon.general.Interaction;
import org.apache.log4j.Logger;

import io.seldon.db.jdo.DatabaseException;
import io.seldon.general.InteractionPeer;

public class SqlInteractionPeer implements InteractionPeer{
    private static Logger logger = Logger.getLogger(SqlInteractionPeer.class.getName());
    private final PersistenceManager manager;

    public SqlInteractionPeer(PersistenceManager persistenceManager){
        this.manager = persistenceManager;
    }

    @Override
    public Interaction getInteraction(String user1Id, String user2FbId, int type, int subType ) {
        Query q = manager.newQuery(Interaction.class, "user1Id == i && user2FbId == j && type == k && subType == l && parameterId == 0");
        q.declareParameters("String i, String j, int k, int l");
        q.setUnique(true);
        return (Interaction) q.executeWithArray(user1Id, user2FbId, type, subType);
    }

    @Override
    public void addInteractionEvent(final Interaction interaction, final InteractionEvent event) {
        try {
            TransactionPeer.runTransaction(new Transaction(manager) {
                @Override
                public void process() throws DatabaseException, SQLException {
                    Interaction managedInteraction = manager.getObjectById(Interaction.class, interaction.getId());
                    managedInteraction.getInteractionEvents().add(event);

                }
            });
        } catch (DatabaseException e) {
            logger.error("Database exception received when saving new interaction event:" + interaction, e);
            throw new APIException(APIException.GENERIC_ERROR);
        }


    }

    @Override
    public Set<Interaction> getInteractions(String user1Id, int type, int subType) {
        Query q = manager.newQuery(Interaction.class, "user1Id == i && type == k && subType == j");
        q.declareParameters("String i, int k, int j");
        return new HashSet<>((List<Interaction>) q.execute(user1Id, type, subType));
    }

    @Override
    public Set<Interaction> getInteractions(String user1Id, int type) {
        Query q = manager.newQuery(Interaction.class, "user1Id == i && type == k");
        q.declareParameters("String i, int k");
        return new HashSet<>((List<Interaction>) q.execute(user1Id, type));
    }

    @Override
    public Set<Interaction> getUnconvertedInteractionsByInteractedWithUsers(Collection<String> userFbIds, int type, int subType){
        Query q = manager.newQuery(Interaction.class, ":p1.contains(user2FbId) && type == :p2 && subType == :p3 && interactionEvents.isEmpty()");
        return new HashSet<>((List<Interaction>) q.execute(userFbIds, type, subType));
    }

    @Override
    public Interaction saveOrUpdateInteraction(final Interaction interaction) {
        try{
            final Interaction interactionInDb = getInteraction(interaction.getUser1Id(), interaction.getUser2FbId(),
                    interaction.getType(), interaction.getSubType());
            if(interactionInDb==null){
                TransactionPeer.runTransaction(new Transaction(manager) {
                    @Override
                    public void process() throws DatabaseException, SQLException {
                        Set<InteractionEvent> events = interaction.getInteractionEvents();
                        Interaction persistentInteraction = new Interaction(interaction.getUser1Id(), interaction.getUser2FbId(),
                                interaction.getType(), interaction.getSubType(), interaction.getDate(),
                                events == null? null : new HashSet<>(events));
                        manager.makePersistent(persistentInteraction);
                    }
                });
            } else {
                TransactionPeer.runTransaction(new Transaction(manager){
                    @Override
                    public void process() throws DatabaseException, SQLException {

                        interactionInDb.setDate(interaction.getDate());
                        interactionInDb.setCount(interactionInDb.getCount()+1);
                    }
                });
            }
            return interactionInDb == null ? interaction : interactionInDb;
        } catch (DatabaseException e){
            logger.error("Database exception received when saving new interaction :" + interaction, e);
            throw new APIException(APIException.GENERIC_ERROR);
        }
    }

}
