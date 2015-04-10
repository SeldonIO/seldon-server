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

package io.seldon.api.state;

import java.util.Map;

/**
 * @author firemanphil
 *         Date: 27/11/14
 *         Time: 11:39
 */
public interface ClientConfigHandler {
    Map<String, String> requestCacheDump(String client);

    void addListener(ClientConfigUpdateListener listener, boolean notifyOnExistingData);

    void addNewClientListener(NewClientListener listener, boolean notifyExistingClients);
    void addNewClientListener(NewClientListener listener, boolean notifyExistingClients, boolean addFirst);
}

