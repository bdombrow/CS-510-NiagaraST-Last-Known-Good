
/**********************************************************************
  $Id: Lexicon.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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

import java.io.*;
import java.util.*;

public abstract class Lexicon {
	protected String lexiconFileName = null;

	// for creation and maintenance of word numbers
	protected Set wordNumSet = null; // set of used word numbers
	protected int nextWordNum = 1;

    public abstract void persist();
    public abstract int add(WordEntry we, long docno) throws IMException;
    public abstract void print();
	public abstract void printAll(IVLMgr ivlmgr, PrintStream ps);

	/**
	 * Constructor. load in lexicon and inverted lists.
	 */
	public Lexicon (String lfname) throws IMException {
	
		lexiconFileName = lfname;
		wordNumSet = Collections.synchronizedSet(new HashSet());
	}

	/**
	 * Assign a word number for a new word
	 * 
	 * check to see if the next number to be assigned is already
	 * occupied.
	 */ 
	protected int assignNextWordNum() {

		// remember the number to started with, so that we
		// don't get stuck in the loop forever
		int firstTry = nextWordNum;

		while (true) {

	 		Integer numObj = new Integer(nextWordNum);

			if (wordNumSet.contains(numObj)) {
				nextWordNum++; // try next one
				if (nextWordNum == firstTry) {
		 			// swept thru one round and not be able to find a
					// good number. overflow.
					return -1;
				}
			}
			else {
				// found a number that is not used
				break;
			}
		}

		int ret = nextWordNum;
		nextWordNum++;
		return ret;
	}
}
