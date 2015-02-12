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

package io.seldon.facebook.importer.offline;

import java.util.ArrayList;
import java.util.List;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.api.resource.ConsumerBean;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.db.jdo.Transaction;
import io.seldon.db.jdo.TransactionPeer;

import io.seldon.db.jdo.DatabaseException;

public class DbUtils {

    public static class InsertWithArgs {
        private long row_count;

        public long executeSql(final ConsumerBean consumerBean, final String sql, final List<Object> args) throws DatabaseException {

            row_count = 0;

            final String consumer = consumerBean.getShort_name();
            final PersistenceManager pm = JDOFactory.getPersistenceManager(consumer);

            TransactionPeer.runTransaction(new Transaction(pm) {
                public void process() {
                    Query query = pm.newQuery("javax.jdo.query.SQL", sql);
                    row_count = (Long) query.executeWithArray(args.toArray());
                    query.closeAll();
                }
            });

            return row_count;
        }

    }

    static public void addSimilarUser(ConsumerBean consumerBean, final long u1, final long u2, final int type, final double score) throws DatabaseException {

        final String consumer = consumerBean.getShort_name();
        final PersistenceManager pm = JDOFactory.getPersistenceManager(consumer);

        TransactionPeer.runTransaction(new Transaction(pm) {
            public void process() {
                String sql = "insert into user_similarity (u1,u2,type,score) values (?,?,?,?) on duplicate key update score=" + score;
                Query query = pm.newQuery("javax.jdo.query.SQL", sql);
                List<Object> args = new ArrayList<Object>();
                args.add(u1);
                args.add(u2);
                args.add(type);
                args.add(score);
                query.executeWithArray(args.toArray());
                query.closeAll();
            }
        });

    }

}
