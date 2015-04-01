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

import java.util.List;

/**
 * Implementation of {@link ListBean} with generics.
 * @param <T>
 */
public abstract class GenericListBean<T> extends ResourceBean {
    private static final long serialVersionUID = 5663033013341543349L;

    protected List<T> list;
    protected long size;
	protected long requested;
	
	public long getSize() {
		return size;
	}
	
	public void setSize(long size) {
		this.size = size;
	}
	
	public long getRequested() {
		return requested;
	}
	
	public void setRequested(long requested) {
		this.requested = requested;
	}

    public List<T> getList() {
        return list;
    }

    public void setList(List<T> list) {
        this.list = list;
    }

    @Override
    public String toString() {
        return "GenericListBean{" +
                "list=" + list +
                ", size=" + size +
                ", requested=" + requested +
                "} " + super.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GenericListBean)) return false;

        GenericListBean that = (GenericListBean) o;

        return requested == that.requested && size == that.size && !(list != null ? !list.equals(that.list) : that.list != null);
    }

    @Override
    public int hashCode() {
        int result = list != null ? list.hashCode() : 0;
        result = 31 * result + (int) (size ^ (size >>> 32));
        result = 31 * result + (int) (requested ^ (requested >>> 32));
        return result;
    }
}
