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

import org.springframework.stereotype.Component;

import io.seldon.general.ActionType;

@Component
public class ActionTypeBean extends ResourceBean  {
	private Integer typeId;
	private String name;
	private Double weight;
	private Double defValue;
	private Integer linkType;
	private Boolean semantic;
	
	public ActionTypeBean() {}

	public ActionTypeBean(ActionType t) {
		this.typeId = t.getTypeId();
		this.name = t.getName();
		this.linkType = t.getLinkType();
		this.semantic = t.getSemantic();
		this.weight = t.getWeight();
		this.defValue = t.getDefValue();
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
		return typeId+"";
	};	
	
	
}
