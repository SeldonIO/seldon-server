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

package io.seldon.cooc;

public class CooccurrenceCount implements Comparable<CooccurrenceCount> {

	double count;
	long time;
	
	public CooccurrenceCount(double count, long time) {
		super();
		this.count = count;
		this.time = time;
	}

	public double getCount() {
		return count;
	}

	public long getTime() {
		return time;
	};
	
	@Override
	public boolean equals(Object o)
	{
		if (!(o instanceof CooccurrenceCount))
			return false;
		else
		{
			CooccurrenceCount other = (CooccurrenceCount) o;
			return (count == other.getCount() && time == other.getTime());
		}
	}
	
	public String toString()
	{
		return "Cooccurrence Count:"+count+" time:"+time;
	}
	
	@Override
    public int hashCode() {
        int result = 0;
        long temp;
        temp = count != +0.0d ? Double.doubleToLongBits(count) : 0L;
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = time;
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
	}

	@Override
	public int compareTo(CooccurrenceCount other) {
		return Double.compare(this.count, other.count);
	}
	
}
