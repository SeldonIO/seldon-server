/*
 * Seldon -- open source prediction engine
 * =======================================
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 **********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at       
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ********************************************************************************************** 
*/
package io.seldon.spark.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class ZkCuratorHandler implements ConnectionStateListener {

    private static ZkCuratorHandler peer;
    public boolean isConnected = false;

    public  ZkCuratorHandler(String servers)
    {
        if (servers != null)
        {
            this.zkServers = servers;
            startClient();
            peer = this;
        }
    }

    public static void shutdown()
    {
        if (peer != null)
            peer.stopClient();
    }

    public static ZkCuratorHandler getPeer() {
        return peer;
    }


    CuratorFramework curator;
    String zkServers;
    private void stopClient()
    {
        curator.close();
    }

    private void startClient()
    {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        curator = builder.connectString(zkServers).retryPolicy(new ExponentialBackoffRetry(1000,100)).build();
        curator.getConnectionStateListenable().addListener(this);
        curator.start();
    }

    public CuratorFramework getCurator() {
        return curator;
    }

    @Override
    public void stateChanged(CuratorFramework client, ConnectionState state) {
        switch (state)
        {
            case RECONNECTED:
            {
                isConnected = true;
            }
            break;
            case LOST:
            {
                isConnected = false;
            }
            break;
            case CONNECTED:
            {
                isConnected = true;
            }
            break;
            case SUSPENDED:
            {
                isConnected = false;
            }
            break;
        }

    }

}
