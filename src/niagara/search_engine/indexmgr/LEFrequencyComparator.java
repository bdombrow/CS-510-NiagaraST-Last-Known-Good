
/**********************************************************************
  $Id: LEFrequencyComparator.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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

import java.util.*;

/**
  * Comparator to compare two LexiconEntry's
  * This is used to sort LexiconEntry's in decreasing order of the word
  * frequencies.  When two lexicon entries have the same word number,
  * sort in the increasing order of wordno's.
  */
public class LEFrequencyComparator implements Comparator {

	public int compare (Object obj1, Object obj2) {
		if (! (obj1 instanceof LexiconEntry)
			|| !(obj2 instanceof LexiconEntry)) {

			throw new ClassCastException();
		}
		LexiconEntry le1 = (LexiconEntry)obj1;
		LexiconEntry le2 = (LexiconEntry)obj2;

		int f1 = le1.getFrequency();
		int f2 = le2.getFrequency();

		if (f1 != f2) return f2 - f1;
		else return le1.getWordNum() - le2.getWordNum();
	}

	public boolean equals (Object obj) {
		return (obj instanceof LEFrequencyComparator);
	}
}
