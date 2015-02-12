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

package io.seldon.api.service.async;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import io.seldon.api.Constants;
import io.seldon.db.jdbc.JDBCConnectionFactory;
import io.seldon.general.Action;

/**
 * Provide an batched insert of Actions (including creating new users and items).
 * Uses a LinkedBlockingQueue in which Action objects are added. A thread runs reading actions off this
 * queue and adding them to the db using SQL batch statements. To run fast needs the Mysql extension rewriteBatchedStatements=true.
 *
 * <ul>
 * <li> new users are identified by having user_id 0
 * <li> new items are identified by having item_id 0
 * <li> basic validation checks carried out on client_user_id and client_item_id
 * <li> All exceptions are caught in an effort to never allow the thread to die
 * <li> batch size can be configured
 * <li> max wait timeout can be configured
 * <li> The SQL will be run if a) batch size is exceeeded, b) the max time between sql updates is exceeded 
 * </ul>
 * @author rummble
 *
 */
public class JdoAsyncActionQueue implements Runnable, AsyncActionQueue {

	private static Logger logger = Logger.getLogger(JdoAsyncActionQueue.class.getName());
	public static final int MAX_CLIENT_USER_ID_LEN = 254;
	public static final int MAX_CLIENT_ITEM_ID_LEN = 254;
    private String client;
    private int timeout; 
    private LinkedBlockingQueue<Action> queue;
    private int batchSize; // batch size for sql statements
    private int maxDBRetries = 1; // max # of times to try sql statement on exception
    boolean keepRunning;
    
    Connection connection = null;
    
    PreparedStatement actionPreparedStatement;
    private int actionsAdded = 0; // actions added so far to sql statement
    private int actionsProcessed = 0; // number of actions processed 
    
    PreparedStatement userPreparedStatement;
    private int usersAdded = 0; // users added so far to sql statement
    boolean updateUsers = false;
    
    PreparedStatement itemPreparedStatement;
    private int itemsAdded = 0; // users added so far to sql statement
    boolean updateItems = false;
     
    boolean runUserItemUpdates = true;
    boolean runUpdateIdsInActionTable = true;
    long lastSqlRunTime = 0;
    int badActions = 0;
    
    boolean insertActions = true;
    
    public JdoAsyncActionQueue(String client, int qTimeoutSecs, int batchSize, int maxQSize,int maxDBRetries,boolean runUserItemUpdates,boolean runUpdateIdsInActionTable,boolean insertActions) {
        this.client = client;
        this.batchSize = batchSize;
        this.maxDBRetries = maxDBRetries;
        this.queue = new LinkedBlockingQueue<Action>(maxQSize);
        this.timeout = qTimeoutSecs;
        this.runUserItemUpdates = runUserItemUpdates;
        this.runUpdateIdsInActionTable = runUpdateIdsInActionTable;
        this.insertActions = insertActions;
        logger.info("AsyncQ created for client "+client+" qTimeout:"+qTimeoutSecs+" batchSize:"+batchSize+" maxQSize:"+maxQSize+" maxDBRetries:"+maxDBRetries+" userItemUpdates:"+runUserItemUpdates+" rumActionIdUpdates:"+this.runUpdateIdsInActionTable+" insertActions:"+this.insertActions);
    }

    public void run() {
    	keepRunning = true;
    	this.lastSqlRunTime = System.currentTimeMillis();
        while (true) 
        {
            try 
            {
                Action action = queue.poll(timeout, TimeUnit.SECONDS);
                if (action != null)
                    addSQL(action);
                long timeSinceLastSQLRun = (System.currentTimeMillis() - this.lastSqlRunTime)/1000;
                boolean runSQL = false;
                if ((action == null && actionsProcessed > 0))
                {
                	runSQL = true;
                	logger.info("Run sql as timeout on poll and actionsProcessed > 0");
                }
                else if (actionsProcessed >= batchSize)
                {
                	runSQL = true;
                	logger.info("Run sql as batch size exceeded");
                }
                else if (timeSinceLastSQLRun > timeout && actionsProcessed > 0)
                {
                	runSQL = true;
                	logger.info("Run sql as time between sql runs exceeded");
                }
                if (runSQL)
                    runSQL();
                if (!keepRunning && action == null)
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
        clearSQLState();
        actionsAdded = 0;
        actionsProcessed = 0;
        itemsAdded = 0;
        usersAdded = 0;
        updateUsers = false;
        updateItems = false;
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
    		if (actionPreparedStatement != null)
    		{
    			try{actionPreparedStatement.close();}
    			catch( SQLException exception )
    			{
    				logger.error("Unable to close action perpared statment",exception);
    			}
    		}
    		
    		if (userPreparedStatement != null)
    		{
    			try{userPreparedStatement.close();}
    			catch( SQLException exception )
    			{
    				logger.error("Unable to close user perpared statment",exception);
    			}
    		}
    		
    		if (itemPreparedStatement != null)
    		{
    			try{itemPreparedStatement.close();}
    			catch( SQLException exception )
    			{
    				logger.error("Unable to close item perpared statment",exception);
    			}
    		}

		}
		finally
		{
			connection = null;
			actionPreparedStatement = null;
			userPreparedStatement = null;
			itemPreparedStatement = null;
		}
		
    }
    
    private void updateUsersIfNeeded() throws SQLException
    {
    	if (updateUsers && insertActions) //update user ids - if we have added new users or added users with 0 user_id
		{
    		logger.info("Updating users");
			PreparedStatement s = connection.prepareStatement("update actions a join users u on a.client_user_id=u.client_user_id set a.user_id=u.user_id where a.user_id=0");
			try
			{
				s.executeUpdate();
				connection.commit();
				updateUsers = false;
			}
			finally
			{
				if (s!= null)
					s.close();
			}
		}
    }
    
    private void updateItemsIfNeeded() throws SQLException
    {
    	if (updateItems && insertActions) //update user ids - if we have added new users or added users with 0 user_id
		{
    		logger.info("Updating items");
			PreparedStatement s = connection.prepareStatement("update actions a join items i on a.client_item_id=i.client_item_id set a.item_id=i.item_id where a.item_id=0");
			try
			{
				s.executeUpdate();
				connection.commit();
				updateItems = false;
			}
			finally
			{
				if (s!= null)
					s.close();
			}
		}
    }
    
    private void executeBatch() throws SQLException
    {
    	if (actionsProcessed > 0)
    	{
    		if (actionsAdded > 0)
    		{
    			actionPreparedStatement.executeBatch();
    		}
    		
    		if (usersAdded > 0)
			{
    			userPreparedStatement.executeBatch();
			}
			
			if (itemsAdded > 0)
			{
		    	itemPreparedStatement.executeBatch();
			}
    		
    		connection.commit();
    		if (actionPreparedStatement != null)
    			actionPreparedStatement.close();
    		if (usersAdded > 0)
    		{
    			usersAdded = 0;
    			userPreparedStatement.close();
    		}
    		if (itemsAdded > 0)
    		{
    			itemsAdded = 0;
    			itemPreparedStatement.close();
    		}    		
    		actionsAdded = 0;
    		actionsProcessed = 0;
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
   
    private void runSQL() 
    {
    	long t1 = System.currentTimeMillis();
    	int localActionsAdded = this.actionsAdded;
    	
    	// batch update actions
    	boolean success = false;
        for (int i = 0; i < this.maxDBRetries && !success; i++)
        {
        	try
    		{
    			executeBatch();
    			success = true;
    			break;
            }
    		catch (SQLException e) {
                logger.error("Failed to run batch update ",e);
                rollBack();
			}
    	}
        
        if (success)
        {
            // Update users if needed
            success = false;
            for (int i = 0; i < this.maxDBRetries && !success; i++)
            {
            	try
        		{
        			updateUsersIfNeeded();
        			success = true;
        			break;
                }
        		catch (SQLException e) {
                    logger.error("Failed to run user update ",e);
                    rollBack();
    			}
        	}
            
            //Update items if needed
            success = false;
            for (int i = 0; i < this.maxDBRetries && !success; i++)
            {
            	try
        		{
            		updateItemsIfNeeded();
        			success = true;
        			break;
                }
        		catch (SQLException e) {
                    logger.error("Failed to run item update ",e);
                    rollBack();
    			}
        	}
        }
        else {
//        	logger.error("Failed to add batch actions so not running user/item update");
            final String message = "Failed to add batch actions so not running user/item update";
            logger.error(message, new Exception(message));
        }
        
        resetState();
        long t2 = System.currentTimeMillis();
        //log q size
        logger.info("AsynAction Q for "+client+" at size:"+queue.size()+" actions added "+localActionsAdded+" time to process:"+(t2-t1));
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

    /**
     * Check the values of an action to ensure we don't try to manipulate bad data
     * @param action
     * @return
     */
    private boolean checkActionOK(Action action)
    {
    	repairAction(action);
    	if (action.getType() == null 
//    			|| action.getTimes() == null
//    			|| action.getValue() == null
    			|| action.getClientUserId() == null
    			|| action.getClientItemId() == null
    			|| action.getClientUserId().length() > MAX_CLIENT_USER_ID_LEN
    			|| action.getClientItemId().length() > MAX_CLIENT_ITEM_ID_LEN
    			)
    	{
    		badActions++;
    		return false;
    	}
    	else
    		return true;
    }
    
    private void getConnectionIfNeeded() throws SQLException
    {
    	if (connection == null)
    	{
    		connection = JDBCConnectionFactory.get().getConnection(client);
    		connection.setAutoCommit( false );
    	}
    }
    
    private void addUserBatch(Action action) throws SQLException
    {
    	userPreparedStatement.setString(1, ""); // Username hardwired to empty string
    	
    	if (action.getDate() != null)
			userPreparedStatement.setTimestamp(2, new java.sql.Timestamp(action.getDate().getTime()));
		else
			userPreparedStatement.setNull(2, java.sql.Types.TIMESTAMP);
    	
    	if (action.getDate() != null)
			userPreparedStatement.setTimestamp(3, new java.sql.Timestamp(action.getDate().getTime()));
		else
			userPreparedStatement.setNull(3, java.sql.Types.TIMESTAMP);
    	
    	userPreparedStatement.setInt(4, Constants.DEFAULT_USER_TYPE); // hardwired user type
    	userPreparedStatement.setString(5, action.getClientUserId());
    }
    
    private void addItemBatch(Action action) throws SQLException
    {
    	itemPreparedStatement.setString(1, ""); // item names hardwired to empty string
    	
    	if (action.getDate() != null)
			itemPreparedStatement.setTimestamp(2, new java.sql.Timestamp(action.getDate().getTime()));
		else
			itemPreparedStatement.setNull(2, java.sql.Types.TIMESTAMP);

    	if (action.getDate() != null)
			itemPreparedStatement.setTimestamp(3, new java.sql.Timestamp(action.getDate().getTime()));
		else
			itemPreparedStatement.setNull(3, java.sql.Types.TIMESTAMP);
    	
    	itemPreparedStatement.setString(4, action.getClientItemId());
    }
    
    private void addActionBatch(Action action) throws SQLException
    {
    	actionPreparedStatement.setLong(1, action.getUserId());
		actionPreparedStatement.setLong(2, action.getItemId());
		if (action.getType() != null)
			actionPreparedStatement.setInt(3, action.getType());
		else
			actionPreparedStatement.setNull(3, java.sql.Types.INTEGER);
		
		if (action.getTimes() != null)
			actionPreparedStatement.setInt(4, action.getTimes());
		else
			actionPreparedStatement.setNull(4, java.sql.Types.INTEGER);
		
		if (action.getDate() != null)
			actionPreparedStatement.setTimestamp(5, new java.sql.Timestamp(action.getDate().getTime()));
		else
			actionPreparedStatement.setNull(5, java.sql.Types.TIMESTAMP);
		
		if (action.getValue() != null)
			actionPreparedStatement.setDouble(6, action.getValue());
		else
			actionPreparedStatement.setNull(6, java.sql.Types.DOUBLE);
		
		actionPreparedStatement.setString(7, action.getClientUserId());
		actionPreparedStatement.setString(8, action.getClientItemId());
    }

    private void addSQL(Action action) throws SQLException {
    	if (!checkActionOK(action))
    	{
    		logger.warn("Bad Action "+action.toString());
    		return;
    	}
    	else
    	{
    		getConnectionIfNeeded();
    		
    		if (this.insertActions)
    		{
    			// Add action batch
    			if (actionPreparedStatement == null)
    				actionPreparedStatement = connection.prepareStatement("insert into actions (action_id,user_id,item_id,type,times,date,value,client_user_id,client_item_id) values (0,?,?,?,?,?,?,?,?)");
    			addActionBatch(action);
    			actionPreparedStatement.addBatch();
    			actionsAdded++;
    		}
    		
    		
    		if (runUserItemUpdates)
    		{
    			if (action.getUserId() == 0)
    			{
    				if (isRunUpdateIdsInActionTable())
    					updateUsers = true;
    				if (userPreparedStatement == null)
    					userPreparedStatement = connection.prepareStatement("insert ignore into users (user_id,username,first_op,last_op,type,num_op,active,client_user_id,avgrating,stddevrating) values (0,?,?,?,?,1,1,?,0,0)");
    				
    				addUserBatch(action);
    				userPreparedStatement.addBatch();
    				usersAdded++;
    			}
    	        	 
    			if (action.getItemId() == 0)
    			{
    				if (isRunUpdateIdsInActionTable())
    					updateItems = true;
    				if (itemPreparedStatement == null)
    					itemPreparedStatement = connection.prepareStatement("insert ignore into items (item_id,name,first_op,last_op,popular,client_item_id,type,avgrating,stddevrating,num_op) values (0,?,?,?,0,?,0,0,0,0)");
    				
    				addItemBatch(action);
    				itemPreparedStatement.addBatch();
    				itemsAdded++;
    				
    			}
    		}
    		
    		actionsProcessed++;
    	        
    	}
       
    }

    public void put(Action action) {
        queue.add(action);
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
        return actionsAdded;
    }
    
    public int getActionsProcessed() {
		return actionsProcessed;
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
	
	

	public boolean isInsertActions() {
		return insertActions;
	}

	public synchronized boolean isRunUpdateIdsInActionTable() {
		return runUpdateIdsInActionTable;
	}

	public synchronized void setRunUpdateIdsInActionTable(
			boolean runUpdateIdsInActionTable) {
		this.runUpdateIdsInActionTable = runUpdateIdsInActionTable;
	}
    
	
	
}

