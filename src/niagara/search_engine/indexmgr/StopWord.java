
/**********************************************************************
  $Id: StopWord.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


  NIAGARA -- Net Data Management System                                 
                                                                        
  Copyright (c)    Computer Sciences Department, University of          
                       Wisconsin -- Madison                             
  All Rights Reserved.                                                  
                                                                        
  Permission to use, copy, modify and distribute this software and      
  its documentation is hereby granted, provided that both the           
  copyright notice and this permission notice appear in all copies      
  of the software, derivative works or modified versions, and any       
  portions thereof, and that both notices appear in supporting          
  documentation.                                                        
                                                                        
  THE AUTHORS AND THE COMPUTER SCIENCES DEPARTMENT OF THE UNIVERSITY    
  OF WISCONSIN - MADISON ALLOW FREE USE OF THIS SOFTWARE IN ITS "        
  AS IS" CONDITION, AND THEY DISCLAIM ANY LIABILITY OF ANY KIND         
  FOR ANY DAMAGES WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.   
                                                                        
  This software was developed with support by DARPA through             
   Rome Research Laboratory Contract No. F30602-97-2-0247.  
**********************************************************************/


package niagara.search_engine.indexmgr;

/**
 * Maintains a list of stop words
 *
 * Chun Zhang, April 1999
 */

class StopWordEntry {
	String word;
	int count;
}

public class StopWord {
	public static String[] stopWords = {
		"an", "any", "all", "also", "are", "am", "and", "as", "at",
		"be", "because", "by", "but",
		"can", "could",
		"did", "do", "does",
		"for", "from",
		"had", "have", "he", "her", "here", "him", "his", "how",
		"if", "in", "into", "is", "it",
		"let",
		"may", "me", "more", "must", "my", "much",
		"no", "nor", "not", "now",
		"of", "on", "one", "or", "our",
		"said", "say", "shall", "she", "should", "so", "some",
		"than", "that", "the", "their", "them", "then", "there",
			"these", "they", "this", "to", "too",
		"was", "we", "well", "were", "what", "when", "where", "which",
			"who", "why", "will", "with", "would",
		"yet", "you", "your",
		"up", "upon", "us"
	};
	public static int[] wordCount = null;
	static {
		wordCount = new int[stopWords.length];
		for (int i = 0; i < stopWords.length; i++) {
			wordCount[i] = 0;
		}
	}
	public static boolean isStopWord(String word) {

		if (word.length()==1) return true;

		for (int i = 0; i < stopWords.length; i++) {
			if (word.compareToIgnoreCase(stopWords[i])==0) {
				wordCount[i]++;
				return true;
			}
		}
		return false;
	}
	public static void print() {
		System.out.println ("\nSTOP WORDS:");
		for (int i = 0; i < stopWords.length; i++) {
			System.out.println ("  "+stopWords[i]+": "+wordCount[i]);
		}
	}
}
