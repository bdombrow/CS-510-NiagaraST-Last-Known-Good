
/**********************************************************************
  $Id: StringOp.java,v 1.2 2003/07/08 02:12:08 tufte Exp $


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


// StringOp.java
package niagara.search_engine.operators;

import java.util.*;
import niagara.search_engine.indexmgr.*;

/**
 * StringOp returns the IVL of a quoted string ignoring stop words in it
 * It takes two parameters, IndexMgr and the quoted string 
 * 
 */
public class StringOp extends AbstractOperator 
{
    private IndexMgr indexMgr;
    private String string;
 
    public StringOp(Vector parameters) 
    {
	super(parameters);
	
	opType = "STRINGOP";
	
	indexMgr = (IndexMgr)parameters.elementAt(0);
	
	string = (String)parameters.elementAt(1);
	
    }
    
  //<NOTE: added 1, 
    public StringOp(String str) 
    {
      string = str;
    }

    public String toString() {
      return "StringOp("+string+")";
    }
  //NOTE>      

    public void evaluate() throws IMException
    {
	isEvaluated = true;
	
	if (string == null || indexMgr == null) {
	    return;
	}
	
	StringTokenizer st = new StringTokenizer(string, IndexParser.tokenDelimiters);

	String word = new String();
	
	while (st.hasMoreTokens() ) {
	    if (!StopWord.isStopWord(word = st.nextToken())) {
		break;
	    }
	}

	if (!st.hasMoreTokens() &&  StopWord.isStopWord(word)) {
	    // In case the string are all stop words or delimiters
	    return;
	}

	Vector firstIVL = indexMgr.getInvertedList(word, false);
		
	while (firstIVL != null && st.hasMoreTokens() ){
	    do {
		if (!StopWord.isStopWord(word = st.nextToken())) {
		    break;
		}
	    }while (st.hasMoreTokens());

	    if (StopWord.isStopWord(word)) {
		//st runs out of token
		resultIVL = firstIVL;
		return;
	    }
	    Vector qualifiedIVL = new Vector();
    
	    Vector nextIVL = indexMgr.getInvertedList(word, false);
	        
	    if (nextIVL == null) {
		return;
	    }

	    //concatenate conjacent position pairs in the two IVLs
	    int i=0, j=0;
	    IVLEntry firstIvlEnt, nextIvlEnt;
	    long firstDocno, nextDocno;

	    while (i < firstIVL.size() && j < nextIVL.size()) {

		firstIvlEnt = (IVLEntry)firstIVL.elementAt(i);
		nextIvlEnt = (IVLEntry)nextIVL.elementAt(j);
		firstDocno = firstIvlEnt.getDocNo();
		nextDocno = nextIvlEnt.getDocNo();
	    
		if (firstDocno < nextDocno) {
		    i++;
		}

		else if (nextDocno < firstDocno) {
		    j++;
		}

		else { //(firstDocno == nextDocno) 
		    IVLEntry ivlent = null;

		    if (IndexMgr.idxmgr.indexType == IndexMgr.INDEX_T_FULL) {
			Vector qualifiedPosList = new Vector();
			Vector firstPosList = firstIvlEnt.getPositionList();
			Vector nextPosList = nextIvlEnt.getPositionList();
		
			// scan over two inverted lists and compare
			for (int pi=0; pi< firstPosList.size(); pi++) {
			    Position firstPos = (Position)firstPosList.elementAt(pi);

			    for (int pj=0; pj< nextPosList.size(); pj++) {
				Position nextPos = (Position)nextPosList.elementAt(pj);		       
				//not ask nextTo because of possible stopwords 
				if (nextPos.compareTo(firstPos)>0) {
				    Position newPos = firstPos.concatenate(nextPos);
				    qualifiedPosList.addElement(newPos);
				}
			    }
			    
			} // end for the position list in a nextIVL entry
				
			if (qualifiedPosList.size()!= 0) {
			    ivlent = new IVLEntry (nextDocno, qualifiedPosList);
			}
		    }
		    else {
		    	ivlent = new IVLEntry (nextDocno);
		    }

		    if (ivlent != null) {
			qualifiedIVL.addElement (ivlent);
		    }

		    i++;
		    j++;		    
		}		
	    } // end while  

	    firstIVL = qualifiedIVL;
    	    	    
	} //end of while (st.hasMoreToken() && firstIVL != null)
	
	resultIVL = firstIVL;	
    }
}
