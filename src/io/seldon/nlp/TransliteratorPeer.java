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

package io.seldon.nlp;

import com.ibm.icu.text.Transliterator;

public class TransliteratorPeer {

	private static ThreadLocal transHolder = new ThreadLocal();
    
    public static Transliterator getPunctuationTransLiterator()
	{
		Transliterator tl  = (Transliterator) transHolder.get();
		if (tl == null)
		{
			tl = Transliterator.getInstance("Any-Latin; Lower; NFD; [[:P:]] Remove; NFC;");
			transHolder.set(tl);
		}
		return tl;
	}
    
    public static void main(String[] args)
    {
    	String s = "coca,cola-";
    	s = s.replaceAll("[-:;\\,]"," ").trim();
    	String res = TransliteratorPeer.getPunctuationTransLiterator().transliterate(s);
    	System.out.println(res);
    }
}
