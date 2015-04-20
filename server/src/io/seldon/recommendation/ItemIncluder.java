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

package io.seldon.recommendation;

import io.seldon.recommendation.filters.FilteredItems;

/**
 * @author firemanphil
 *         Date: 19/11/14
 *         Time: 11:29
 */
public interface ItemIncluder {

    public static final int NUMBER_OF_ITEMS_PER_INCLUDER_DEFAULT = 200;

    /**
     * Produces a list of item ids that should be considered by the
     * recommendation algorithm.
     * @param client the client to give items for
     * @param numItems the number of items to generate as a maximum
     * @return a list of item ids to include.
     */
    FilteredItems generateIncludedItems(String client, int dimension, int numItems);

}
