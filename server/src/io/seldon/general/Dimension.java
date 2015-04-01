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

public class Dimension {
	Integer dimId;
	Integer itemType;
	Integer attr;
	Integer val;
	String attrName;
	String valName;
	boolean trustNetwork;
	
	
	public Dimension(Integer dimId, Integer itemType, Integer attr, Integer val, String attrName, String valName, boolean trustNetwork) {
		this.dimId = dimId;
		this.itemType = itemType;
		this.attr = attr;
		this.val = val;
		this.attrName = attrName;
		this.valName = valName;
		this.trustNetwork = trustNetwork;
	}
	
	public Dimension() {}

	public Integer getDimId() {
		return dimId;
	}
	public void setDimId(Integer dimId) {
		this.dimId = dimId;
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
	public boolean isTrustNetwork() {
		return trustNetwork;
	}
	public void setTrustNetwork(boolean trustNetwork) {
		this.trustNetwork = trustNetwork;
	}

	public Integer getItemType() {
		return itemType;
	}

	public void setItemType(Integer itemType) {
		this.itemType = itemType;
	}
	
	
	
}
