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

package io.seldon.resources.external;

import io.seldon.api.state.ZkNodeChangeListener;
import io.seldon.api.state.ZkSubscriptionHandler;
import io.seldon.mf.PerClientExternalLocationListener;
import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.*;

public class ZookeeperNewResourceNotifierTest {

    private ZkSubscriptionHandler mockZkSubHandler;
    private ZookeeperNewResourceNotifier classToTest;
    private PerClientExternalLocationListener mockPerClientListener;

    @Before
    public void setup(){
        mockZkSubHandler = createMock(ZkSubscriptionHandler.class);
        mockPerClientListener = createMock(PerClientExternalLocationListener.class);
        classToTest = new ZookeeperNewResourceNotifier(mockZkSubHandler);
    }

    @Test
    public void testAddingSubscriber() throws Exception {
        Capture<ZkNodeChangeListener> capturedAllClientListener = new Capture<ZkNodeChangeListener>();
        Capture<ZkNodeChangeListener> capturedAClientListener = new Capture<ZkNodeChangeListener>();

        mockZkSubHandler.addSubscription(eq("/clients/pattern"), capture(capturedAllClientListener));
        mockZkSubHandler.addSubscription(eq("/aclient/pattern"), capture(capturedAClientListener));
        mockPerClientListener.newClientLocation("aclient","alocation","pattern");
        replay(mockZkSubHandler, mockPerClientListener);

        classToTest.addListener("pattern", mockPerClientListener);
        capturedAllClientListener.getValue().nodeChanged("/clients/pattern","aclient");
        capturedAClientListener.getValue().nodeChanged("/pattern/aclient","alocation");
        verify(mockZkSubHandler, mockPerClientListener);
    }

    @Test
    public void testDeletingClient() throws Exception {
        Capture<ZkNodeChangeListener> capturedAllClientListener = new Capture<ZkNodeChangeListener>();
        Capture<ZkNodeChangeListener> capturedAClientListener = new Capture<ZkNodeChangeListener>();

        mockZkSubHandler.addSubscription(eq("/clients/pattern"), capture(capturedAllClientListener));
        mockZkSubHandler.addSubscription(eq("/aclient/pattern"), capture(capturedAClientListener));
        mockZkSubHandler.removeSubscription("/aclient/pattern");
        mockPerClientListener.newClientLocation("aclient","alocation","pattern");
        mockPerClientListener.clientLocationDeleted("aclient","pattern");
        replay(mockZkSubHandler, mockPerClientListener);

        classToTest.addListener("pattern", mockPerClientListener);
        capturedAllClientListener.getValue().nodeChanged("/clients/pattern","aclient");
        capturedAClientListener.getValue().nodeChanged("/pattern/aclient","alocation");
        capturedAllClientListener.getValue().nodeChanged("/clients/pattern","");
        verify(mockZkSubHandler, mockPerClientListener);
    }

}