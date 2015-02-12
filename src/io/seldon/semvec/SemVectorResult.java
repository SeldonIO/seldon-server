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

package io.seldon.semvec;

public class SemVectorResult<T extends Comparable<T>> implements Comparable<SemVectorResult<T>>
{

	public T result;
	public double score;
	
	public SemVectorResult(T result,double score)
	{
		this.result = result;
		this.score = score;
	}

	public T getResult() {
		return result;
	}

	public double getScore() {
		return score;
	}

	@Override
	public int compareTo(SemVectorResult<T> o) {
		return result.compareTo(o.result);
	}
	
	public void setScore(float s)
	{
		this.score = s;
	}
	
}
