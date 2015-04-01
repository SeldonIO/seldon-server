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
public class ItemTypeBean extends ResourceBean  {
    private static final long serialVersionUID = 5551895382163627462L;

    private Integer typeId;
	private String name;
	private Integer linkType;
	private Boolean semantic;
	
	public ItemTypeBean() {}

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
    public String toString() {
        return "ItemTypeBean{" +
                "id='" + typeId + '\'' +
                ", name='" + name + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ItemTypeBean)) return false;

        ItemTypeBean that = (ItemTypeBean) o;

        if (linkType != null ? !linkType.equals(that.linkType) : that.linkType != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (semantic != null ? !semantic.equals(that.semantic) : that.semantic != null) return false;
        if (typeId != null ? !typeId.equals(that.typeId) : that.typeId != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = typeId != null ? typeId.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (linkType != null ? linkType.hashCode() : 0);
        result = 31 * result + (semantic != null ? semantic.hashCode() : 0);
        return result;
    }
}
