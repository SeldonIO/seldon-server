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

package io.seldon.api.resource;

import java.util.Date;

/**
 * @author philipince
 *         Date: 25/10/2013
 *         Time: 13:54
 */
public class ConversionBean extends ResourceBean {


    public enum ConversionType {
        NEW_USER, EXISTING_USER;

    }

    private final ConversionType type;
    private final String inviter;
    private final String invitee;
    private final Date date;

    public ConversionBean(ConversionType type, String inviter, String invitee, Date date) {
        this.type = type;
        this.inviter = inviter;
        this.invitee = invitee;
        this.date = date;
    }

    public String getInviter() {
        return inviter;
    }

    public String getInvitee() {
        return invitee;
    }

    public Date getDate() {
        return date;
    }

    public ConversionType getType() {
        return type;
    }

    @Override
    public String toKey() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
