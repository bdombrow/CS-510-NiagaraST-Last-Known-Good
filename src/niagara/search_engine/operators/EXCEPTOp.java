
/**********************************************************************
  $Id: EXCEPTOp.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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


// EXCEPTOp.java
package niagara.search_engine.operators;

import java.io.*;
import java.util.*;
import niagara.search_engine.indexmgr.*;

/** 
 * EXCEPT operator implements the substraction of two inverted lists
 * Implied condition:
 *    two inverted lists correspond to the same type of elements.
 * 
 * The substraction of two inverted lists is defined as follows:
 *
 * for each document in the intersection of the two document sets 
 * corresponding to the two IVLs,
 *    the position pairs in the corresponding result entry
 *    are the substraction of the two position pairs sets
 *    of the two corresponing IVLEntries.
 * for each document in the leftIVL but not in the rightIVL
 *    the result entry is the corresponding left IVLentry
 *
 */

public class EXCEPTOp extends AbstractOperator
{
    private Vector leftIVL;
    private Vector rightIVL;    

    /* constructor */
    public EXCEPTOp(Vector sourceIVLs) 
    {
	super(sourceIVLs);
	
	this.leftIVL = (Vector)sourceIVLs.elementAt(0);
	this.rightIVL = (Vector)sourceIVLs.elementAt(1);
	opType = "EXCEPTOP";
	
    }
    
  //<NOTE: added 1, 2.1    
    /* constructor */
    public EXCEPTOp() 
    {
    }

    public void addIVLs(Vector sourceIVLs) 
    {
	this.leftIVL = (Vector)sourceIVLs.elementAt(0);
	this.rightIVL = (Vector)sourceIVLs.elementAt(1);
    }

    public String toString() {
        return "EXCEPTOp";
    }
  //NOTE>
    
    public void evaluate() throws IMException
    {
	//early return if one of the oprand IVLs is null already.

	isEvaluated = true;

	if ((leftIVL == null) || (rightIVL == null)) {
	    return;	    
	}
		
	//otherwise, scan over two IVLs.

	int i=0;
	int j=0; 
	IVLEntry leftIVLEntry, rightIVLEntry;
	long leftDocNo, rightDocNo;

	while (i< leftIVL.size() && j< rightIVL.size()) {

	    leftIVLEntry = (IVLEntry)leftIVL.elementAt(i);
	    rightIVLEntry = (IVLEntry)rightIVL.elementAt(j);
	    
	    leftDocNo = leftIVLEntry.getDocNo();
	    rightDocNo = rightIVLEntry.getDocNo();

	    //assume IVLentries in an IVL sorted by docno in ascending order
	    
	    if (leftDocNo < rightDocNo) {

		//add it into result
		resultIVL.addElement(new IVLEntry
				     (leftDocNo, 
				      leftIVLEntry.getPositionList()));    
		i++;
		continue;		
	    } 
	    		
	    else if (leftDocNo > rightDocNo) {
		j++;
		continue;		
	    } 

	    else { //(leftDocNo == rightDocNo) 
		
		Vector qualifiedPosList = new Vector();
		Vector leftPosList = leftIVLEntry.getPositionList();
		Vector rightPosList = rightIVLEntry.getPositionList();
		
		// scan two position pair lists and intersect
		for (int pi=0; pi<leftPosList.size(); pi++) {

		    Position leftPos = (Position)leftPosList.elementAt(pi);
		    int pj = 0;
		    	    
		    for (pj=0; pj<rightPosList.size(); pj++) {
			
			Position rightPos = (Position)rightPosList.elementAt(pj);
			if (leftPos.getType() == Position.PT_PAIR
			&& rightPos.getType() == Position.PT_PAIR) {

			    if (leftPos.compareTo(rightPos)==0) {
				//this one is in rightIVLEntry, not qualified
				break; 
			    }
			}
			else { // error in query execution plan
			    throw new IMException();			    
			}
		    } // end for pj

		    //sure it is not in rightIVLEntry, qualified
		    if (pj>=rightPosList.size()) {
		
			qualifiedPosList.addElement(leftPos);
		    }
		    
		}// end for pi

		if (qualifiedPosList.size() != 0) {
		    resultIVL.addElement(new IVLEntry
			(leftDocNo, qualifiedPosList));
		}

		i++;
		j++;
		
	    }  //end of if leftDocNo==rightDocNo

	 } //end of while
	
	//rightIVL reaches the end
	while (i < leftIVL.size()) {
          //add the remains of leftIVL into the results
	    leftIVLEntry = (IVLEntry)leftIVL.elementAt(i);
	    resultIVL.addElement(new IVLEntry
				 (leftIVLEntry.getDocNo(), 
				  leftIVLEntry.getPositionList()));
	    i++;	    
	}
    }
    
}  //end of class EXCEPTOp

