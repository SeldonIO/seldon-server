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

package io.seldon.general;


/**
 * Created by dylanlentini on 28/02/2014.
 */
public interface ImpressionsPersistenceHandler
{
    Impressions getImpressions(String client, String user);
    void saveOrUpdateImpressions(String client, String user, Impressions impressions);
    Impressions getPendingImpressions(String client, String user);
    void saveOrUpdatePendingImpressions(String client, String user, Impressions impressions);
}

