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
public class DimensionBean extends ResourceBean {
    private static final long serialVersionUID = -3726021428982495025L;

    private int dimId;
    private int attr;
    private int val;
    private String attrName;
    private String valName;
    private double amount;
    private Integer itemType;

    public Integer getItemType() {
        return itemType;
    }

    public void setItemType(Integer itemType) {
        this.itemType = itemType;
    }

    public DimensionBean() {
    }

    public int getDimId() {
        return dimId;
    }

    public void setDimId(int dimId) {
        this.dimId = dimId;
    }

    public int getAttr() {
        return attr;
    }

    public void setAttr(int attr) {
        this.attr = attr;
    }

    public int getVal() {
        return val;
    }

    public void setVal(int val) {
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
        return "DimensionBean{" +
                "dimId=" + dimId +
                ", attr=" + attr +
                ", val=" + val +
                ", attrName='" + attrName + '\'' +
                ", valName='" + valName + '\'' +
                ", amount=" + amount +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DimensionBean)) return false;

        DimensionBean that = (DimensionBean) o;

        if (Double.compare(that.amount, amount) != 0) return false;
        if (attr != that.attr) return false;
        if (dimId != that.dimId) return false;
        if (val != that.val) return false;
        if (attrName != null ? !attrName.equals(that.attrName) : that.attrName != null) return false;
        if (itemType != null ? !itemType.equals(that.itemType) : that.itemType != null) return false;
        if (valName != null ? !valName.equals(that.valName) : that.valName != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = dimId;
        result = 31 * result + attr;
        result = 31 * result + val;
        result = 31 * result + (attrName != null ? attrName.hashCode() : 0);
        result = 31 * result + (valName != null ? valName.hashCode() : 0);
        temp = amount != +0.0d ? Double.doubleToLongBits(amount) : 0L;
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (itemType != null ? itemType.hashCode() : 0);
        return result;
    }
}
