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

/**
 * @author claudio
 */

@Component
public class ItemSimilarityNodeBean extends ResourceBean {
    private static final long serialVersionUID = -8869264588054801412L;

    private String item;
	private double sim;
	
	ItemSimilarityNodeBean() {
	}

	public ItemSimilarityNodeBean(String id, double sim) {
		item = id;
		this.sim = sim;
	}

	public String getItem() {
		return item;
	}

	public void setItem(String item) {
		this.item = item;
	}

	public double getSim() {
		return sim;
	}

	public void setSim(double similarity) {
		this.sim = similarity;
	}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ItemSimilarityNodeBean)) return false;

        ItemSimilarityNodeBean that = (ItemSimilarityNodeBean) o;

        if (Double.compare(that.sim, sim) != 0) return false;
        if (item != null ? !item.equals(that.item) : that.item != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = item != null ? item.hashCode() : 0;
        temp = sim != +0.0d ? Double.doubleToLongBits(sim) : 0L;
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}
