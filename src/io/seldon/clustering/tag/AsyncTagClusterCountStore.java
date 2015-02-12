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

package io.seldon.clustering.tag;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import io.seldon.api.caching.ActionHistoryCache;
import io.seldon.clustering.tag.jdo.JdoItemTagCache;
import io.seldon.db.jdbc.JDBCConnectionFactory;
import io.seldon.general.Action;
import org.apache.log4j.Logger;

import io.seldon.api.Constants;

public class AsyncTagClusterCountStore implements Runnable {

	public static class ClusterCount {
		public String tag;
		public long itemId;
		
		public ClusterCount(String tag, long itemId) {
			super();
			this.tag = tag;
			this.itemId = itemId;
		}
		
		
	}
	
	int actionHistorySize = 1;
	int tagAttrId = 9;
	
	
	private static Logger logger = Logger.getLogger(AsyncTagClusterCountStore.class.getName());
    private String client;
    private int timeout; 
    private LinkedBlockingQueue<ClusterCount> queue;
    private int batchSize; // batch size for sql statements
    private int maxDBRetries = 1; // max # of times to try sql statement on exception
    boolean keepRunning;
    double decay = 3600;
    Connection connection = null;
    
    
    PreparedStatement countPreparedStatement;
    private int countsAdded = 0; // actions added so far to sql statement
    private int countsAddedTotal = 0; // total actions added including counts for same cluster and item (will be different than counts added for cached version that uses db time)
     
    long lastSqlRunTime = 0;
    int badActions = 0;
    boolean useDBTime = true;
    TreeMap<String,TreeMap<Long,Double>> clusterCounts;
    
    public AsyncTagClusterCountStore(String client, int qTimeoutSecs, int batchSize, int maxQSize,int maxDBRetries,double decay,boolean useDBTime) {
        this.client = client;
        this.batchSize = batchSize;
        this.maxDBRetries = maxDBRetries;
        this.queue = new LinkedBlockingQueue<ClusterCount>(maxQSize);
        this.timeout = qTimeoutSecs;
        this.decay = decay;
        clusterCounts = new TreeMap<String,TreeMap<Long,Double>>();
        this.useDBTime = useDBTime;
        logger.info("Async tag cluster count created for client "+client+" qTimeout:"+qTimeoutSecs+" batchSize:"+batchSize+" maxQSize:"+maxQSize+" maxDBRetries:"+maxDBRetries+" decay:"+decay+" use DB Time:"+useDBTime);
    }

    public void run() {
    	keepRunning = true;
    	this.lastSqlRunTime = System.currentTimeMillis();
        while (true) 
        {
            try 
            {
                ClusterCount count = queue.poll(timeout, TimeUnit.SECONDS);
                if (count != null)
                {
                	if (useDBTime)
                		addCount(count);
                	else
                		addSQL(count);
                }
                long timeSinceLastSQLRun = (System.currentTimeMillis() - this.lastSqlRunTime)/1000;
                boolean runSQL = false;
                if ((count == null && countsAdded > 0))
                {
                	runSQL = true;
                	logger.info("Run sql as timeout on poll and actionsAdded > 0");
                }
                else if (countsAdded >= batchSize)
                {
                	runSQL = true;
                	logger.info("Run sql as batch size exceeded");
                }
                else if (timeSinceLastSQLRun > timeout && countsAdded > 0)
                {
                	runSQL = true;
                	logger.info("Run sql as time between sql runs exceeded");
                }
                if (runSQL)
                    runSQL();
                if (!keepRunning && count == null)
                	return;

            } 
            catch (InterruptedException e) {
                return;
            }
            catch (Exception e)
            {
            	logger.error("Caught exception while running ", e);
                resetState();
                logger.warn("\\-> Reset buffers.");
            }
            catch (Throwable t)
            {
                logger.error("Caught throwable while running ", t);
                resetState();
                logger.warn("\\-> Reset buffers.");
            }
        }
    }

    private void resetState() {
    	clusterCounts = new TreeMap<String,TreeMap<Long,Double>>();
        clearSQLState();
        countsAdded = 0;
        countsAddedTotal = 0;
        this.lastSqlRunTime = System.currentTimeMillis();
    }
    
    private void clearSQLState()
    {
    	try
		{
    		if (connection != null)
    		{
    			try{connection.close();}
    			catch( SQLException exception )
    			{
    				logger.error("Unable to close connection",exception);
    			}
    		}
    		if (countPreparedStatement != null)
    		{
    			try{countPreparedStatement.close();}
    			catch( SQLException exception )
    			{
    				logger.error("Unable to close action perpared statment",exception);
    			}
    		}

		}
		finally
		{
			connection = null;
			countPreparedStatement = null;
		}
		
    }
    
    private void executeBatch() throws SQLException
    {
    	if (countsAdded > 0)
    	{
    		countPreparedStatement.executeBatch();
    		countPreparedStatement.close();
			countsAdded = 0;

    		connection.commit();
    	}
    }
    
    private void rollBack()
    {
    	try
		{
			connection.rollback();
		}
		catch( SQLException re )
		{
			logger.error("Can't roll back transaction",re);
		}
    }
   
    private void runSQL() throws SQLException 
    {
    	int sqlAdded = 0;
    	int localActionsAdded = this.countsAdded;
    	if (useDBTime)
    	{
    		addSQLs();
    		sqlAdded = this.countsAdded; 
        	localActionsAdded = this.countsAddedTotal;
    	}
    	else
    	{
    		sqlAdded = this.countsAdded;
        	localActionsAdded = this.countsAdded;
    	}
    	long t1 = System.currentTimeMillis();

    	boolean success = false;
        for (int i = 0; i < this.maxDBRetries; i++)
        {
        	try
    		{
    			executeBatch();
    			success = true;
    			break;
            }
    		catch (SQLException e) {
                logger.error("Failed to run update ",e);
			}
    	}
        if (!success)
        {
        	rollBack();
        	localActionsAdded = 0;
        }
        resetState();
        long t2 = System.currentTimeMillis();
        //log q size
        float compression = localActionsAdded > 0 ? (1.0f-(sqlAdded/(float)localActionsAdded)) : 0.0f;
        logger.info("Asyn count for "+client+" at size:"+queue.size()+" actions added "+localActionsAdded+" unique sql inserts "+sqlAdded + " compression " + compression +" time to process:"+(t2-t1));
    }
    
    /**
     * Allowed operations to fill in nulls in Action
     * @param action
     */
    private void repairAction(Action action)
    {
        if (action.getTimes() == null)
                action.setTimes(1);
    }

    
    private void getConnectionIfNeeded() throws SQLException
    {
    	if (connection == null)
    	{
    		connection = JDBCConnectionFactory.get().getConnection(client);
    		connection.setAutoCommit( false );
    	}
    }
    
   

    private void addCount(ClusterCount count)
    {
    	TreeMap<Long,Double> clusterMap = clusterCounts.get(count.tag);
    	if (clusterMap == null)
    	{
    		clusterMap = new TreeMap<Long,Double>();
    		clusterMap.put(count.itemId, 1D);
    		clusterCounts.put(count.tag, clusterMap);
    		countsAdded++;
    	}
    	else
    	{
    		Double presValue = clusterMap.get(count.itemId);
    		if (presValue == null)
    		{
    			clusterMap.put(count.itemId, 1D);
    			countsAdded++;
    		}
    		else
    			clusterMap.put(count.itemId, presValue + 1);
    	}
    	this.countsAddedTotal++;
    }
    
    private int addSQLs() throws SQLException
    {
    	getConnectionIfNeeded();
		
		// Add action batch
		if (countPreparedStatement == null)
			countPreparedStatement = connection.prepareStatement("insert into tag_cluster_counts values (?,?,?,unix_timestamp()) on duplicate key update count=?+exp(-(greatest(unix_timestamp()-t,0)/?))*count,t=unix_timestamp()");
		int added = 0;
    	for(Map.Entry<String,TreeMap<Long,Double>> m : clusterCounts.entrySet())
    	{
    		for(Map.Entry<Long, Double> e : m.getValue().entrySet())
    		{
    			countPreparedStatement.setString(1, m.getKey());
    			countPreparedStatement.setLong(2, e.getKey());
    			countPreparedStatement.setDouble(3, e.getValue());
    			countPreparedStatement.setDouble(4, e.getValue());
    			countPreparedStatement.setDouble(5, decay);
    			
    			countPreparedStatement.addBatch();
    			added++;
    		}
    	}
    	logger.info("Added "+added+" sql inserts to run ");
    	clusterCounts = new TreeMap<String,TreeMap<Long,Double>>();
    	return added;
    }
    
    
    
    private synchronized void addActionBatch(ClusterCount count) throws SQLException
    {
    	countPreparedStatement.setString(1, count.tag);
		countPreparedStatement.setLong(2, count.itemId);
		countPreparedStatement.setDouble(3, 1D);
		countPreparedStatement.setDouble(4, 1D);
		countPreparedStatement.setDouble(5, decay);
    }

    private void addSQL(ClusterCount count) throws SQLException {
    	
    		getConnectionIfNeeded();
    		
    		// Add action batch
    		if (countPreparedStatement == null)
    			countPreparedStatement = connection.prepareStatement("insert into tag_cluster_counts values (?,?,?,unix_timestamp()) on duplicate key update count=?+exp(-(greatest(?-t,0)/?))*count,t=unix_timestamp()");
    		addActionBatch(count);
    		countPreparedStatement.addBatch();
    		countsAdded++;
    		
    		
       
    }
    

    public void put(ClusterCount count) {
        queue.add(count);
    }
    
    public void addCounts(Set<String> tags,Set<Long> items)
    {
    	for(String tag : tags)
    		for (Long item : items)
    			put(new ClusterCount(tag, item));
    }
    
    public void addCounts(String client,long userId,long itemId)
    {
    	IItemTagCache tagCache = new JdoItemTagCache(client);
    	Set<String> tags = tagCache.getTags(itemId, this.tagAttrId);
    	if (tags != null && tags.size() > 0)
    	{
    		Set<Long> items = new HashSet<Long>();
    		items.add(itemId);
    		if (userId != Constants.ANONYMOUS_USER && this.actionHistorySize > 0)
    		{
    			ActionHistoryCache ah = new ActionHistoryCache(client);
    			List<Long> recentActions = ah.getRecentActions(userId, this.actionHistorySize);
    			if (recentActions != null)
    				items.addAll(recentActions);
    		}
    		addCounts(tags,items);
    	}
    }
    
    public int getQSize()
    {
    	return queue.size();
    }

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getActionsAdded() {
        return countsAdded;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

	public int getMaxDBRetries() {
		return maxDBRetries;
	}

	public void setMaxDBRetries(int maxDBRetries) {
		this.maxDBRetries = maxDBRetries;
	}

	public boolean isKeepRunning() {
		return keepRunning;
	}

	public void setKeepRunning(boolean keepRunning) {
		this.keepRunning = keepRunning;
	}

	public int getBadActions() {
		return badActions;
	}

	public double getDecay() {
		return decay;
	}

	public synchronized void setDecay(double decay) {
		this.decay = decay;
	}
    
	

	

}
