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

import java.util.List;

/**
 * A client strategy that provides the same algorithm strategies for every user.
 *
 * @author firemanphil
 *         Date: 01/12/14
 *         Time: 14:35
 */
public class SimpleClientStrategy implements ClientStrategy {

    private final List<AlgorithmStrategy> strategies;

    public SimpleClientStrategy(List<AlgorithmStrategy> strategies) {
        this.strategies = strategies;
    }

    @Override
    public List<AlgorithmStrategy> getAlgorithms(String userId) {
        return strategies;
    }

}
