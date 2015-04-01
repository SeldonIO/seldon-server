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

import io.seldon.api.resource.ResourceBean;

public class ActionType extends ResourceBean {

	private Integer typeId;
	private String name;
	private Double weight;
	private Double defValue;
	private Integer linkType;
	private Boolean semantic;
	
	public ActionType() {}
	
	

	public ActionType(Integer typeId, String name, Double weight,
			Double defValue, Integer linkType, Boolean semantic) {
		this.typeId = typeId;
		this.name = name;
		this.weight = weight;
		this.defValue = defValue;
		this.linkType = linkType;
		this.semantic = semantic;
	}



	public Integer getTypeId() {
		return typeId;
	}

	public void setTypeId(Integer typeId) {
		this.typeId = typeId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Double getWeight() {
		return weight;
	}

	public void setWeight(Double weight) {
		this.weight = weight;
	}

	public Double getDefValue() {
		return defValue;
	}

	public void setDefValue(Double defValue) {
		this.defValue = defValue;
	}

	public Integer getLinkType() {
		return linkType;
	}

	public void setLinkType(Integer linkType) {
		this.linkType = linkType;
	}

	public Boolean getSemantic() {
		return semantic;
	}

	public void setSemantic(Boolean semantic) {
		this.semantic = semantic;
	}

	@Override
	public String toKey() {
		return this.typeId.toString();
	}

	
}
