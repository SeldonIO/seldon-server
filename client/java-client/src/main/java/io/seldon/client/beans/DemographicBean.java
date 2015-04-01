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
public class DemographicBean extends ResourceBean  {
    private static final long serialVersionUID = -2770250398972300803L;

    private int demoId;
	private Integer attr;
	private Integer val;
	private String attrName;
	private String valName;
	private double amount;
	
	public DemographicBean() {}

	public int getDemoId() {
		return demoId;
	}

	public void setDemoId(int demoId) {
		this.demoId = demoId;
	}

	public Integer getAttr() {
		return attr;
	}

	public void setAttr(Integer attr) {
		this.attr = attr;
	}

	public Integer getVal() {
		return val;
	}
	
	public void setVal(Integer val) {
		this.val = val;
	}

	public String getAttrName() {
		return attrName;
	}

	public void setAttrName(String attrName) {
		this.attrName = attrName;
	}

	public String getValName() {
		return valName;
	}

	public void setValName(String valName) {
		this.valName = valName;
	}

	public double getAmount() {
		return amount;
	}

	public void setAmount(double amount) {
		this.amount = amount;
	}
	
	@Override
    public String toString() {
        return "DemographicBean{" +
                "id='" + demoId + '\'' +
                ", attr='" + attr + '\'' +
                ", val=" + val +
                ", attrName=" + attrName +
                ", valName=" + valName +
                ", amount=" + amount +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DemographicBean)) return false;

        DemographicBean that = (DemographicBean) o;

        if (Double.compare(that.amount, amount) != 0) return false;
        if (demoId != that.demoId) return false;
        if (attr != null ? !attr.equals(that.attr) : that.attr != null) return false;
        if (attrName != null ? !attrName.equals(that.attrName) : that.attrName != null) return false;
        if (val != null ? !val.equals(that.val) : that.val != null) return false;
        if (valName != null ? !valName.equals(that.valName) : that.valName != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = demoId;
        result = 31 * result + (attr != null ? attr.hashCode() : 0);
        result = 31 * result + (val != null ? val.hashCode() : 0);
        result = 31 * result + (attrName != null ? attrName.hashCode() : 0);
        result = 31 * result + (valName != null ? valName.hashCode() : 0);
        temp = amount != +0.0d ? Double.doubleToLongBits(amount) : 0L;
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}
