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
package io.seldon.client.beans;

import org.springframework.stereotype.Component;

@Component
public class ConsumerBean extends ResourceBean {
    private static final long serialVersionUID = 7299800413196795751L;

    private String short_name;

	public ConsumerBean() {}

	public String getShort_name() {
		return short_name;
	}

	public void setShort_name(String shortName) {
		short_name = shortName;
	}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConsumerBean)) return false;

        ConsumerBean that = (ConsumerBean) o;

        return !(short_name != null ? !short_name.equals(that.short_name) : that.short_name != null);

    }

    @Override
    public int hashCode() {
        return short_name != null ? short_name.hashCode() : 0;
    }
}
