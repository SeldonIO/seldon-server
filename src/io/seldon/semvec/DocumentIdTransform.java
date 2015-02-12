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

public class DocumentIdTransform implements QueryTransform<Long> {

	
	@Override
	public Long fromSV(String doc) {
		int i = doc.indexOf("/");
		int j = doc.indexOf("/", i+1);
		String part1 = doc.substring(i+1, j);
		String part2 = doc.substring(j+1);
		return Long.parseLong(part1+part2);
	}

	@Override
	public String toSV(Long id) {
		if (id != null)
		{
			String idStr = "" + id;
			if (idStr.length() < 5)
				return "docs/0000/" + id;
			else
			{
				String[] struc = new String[2];
				struc[0] = idStr.substring(0, 4);
				struc[1] = idStr.substring(4);
				String docName = "docs/" + struc[0] + "/" +struc[1];
				return docName;
			}
		}
		else
			return null;
	}
	
	public static void main(String[] args)
	{
		DocumentIdTransform t = new DocumentIdTransform();
		System.out.println(""+(t.fromSV("docs/0000/456")));
		System.out.println(t.toSV(456L));
	}

}
