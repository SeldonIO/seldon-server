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
 * @author philipince
 *         Date: 18/03/2014
 *         Time: 14:13
 */
public enum InteractionEventType {
    // the interaction was paired with a new user event
    // or a existing user via a request
    // THe other requirement is that this interaction is the most
    // recent that is valid (not out of time)
    CONVERTED(1),
    // the user2 referenced by the interaction has been converted,
    // but via another interaction. This is marked as inactive to prevent
    // further conversions.
    INACTIVE(2)
    ;
    private final int typeNumber;

    InteractionEventType(int typeNumber) {
        this.typeNumber = typeNumber;
    }

    public int getTypeNumber() {
        return typeNumber;
    }
}
