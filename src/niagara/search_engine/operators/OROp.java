
/**********************************************************************
  $Id: OROp.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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


// OROp.java
package niagara.search_engine.operators;

import java.io.*;
import java.util.*;
import niagara.search_engine.indexmgr.*;

/** 
 * OR operator implements the disjunction of two inverted lists
 * Implied condition:
 *    two inverted lists correspond to the same type of elements.
 * 
 * The disjunction of two inverted lists is defined as follows:
 *
 * for each document in the intersection of the two document sets 
 * corresponding to the two IVLs,
 *    the position pairs in the corresponding result IVLentry
 *    are the union of the two position pairs sets
 *    of the two corresponing IVLEntries.
 * for other documents in either IVL,
 *    the position pairs in the corresponding result IVLentry
 *    are the same as the position pairs in that source IVLentry
 *
 */

public class OROp extends AbstractOperator
{
    private Vector leftIVL;
    private Vector rightIVL;    
    
    /* constructor */
    public OROp(Vector sourceIVLs) 
    {
	super(sourceIVLs);
	
	this.leftIVL = (Vector)sourceIVLs.elementAt(0);
	this.rightIVL = (Vector)sourceIVLs.elementAt(1);
	opType = "OROP";
	
    }
    
  //<NOTE: change 1, 2.1
    /* constructor */
    public OROp() 
    {
    }

    public void addIVLs(Vector sourceIVLs) 
    {
	this.leftIVL = (Vector)sourceIVLs.elementAt(0);
	this.rightIVL = (Vector)sourceIVLs.elementAt(1);
    }

    public String toString() {
        return "OROp";
    }
  //NOTE>
    
    public void evaluate() throws IMException
    {
	isEvaluated = true;	

	//early return if one of the oprand IVLs is null.

	if ((leftIVL == null) || (rightIVL == null)) {
	    System.out.println("Null IVLs!");
	    if (leftIVL != null) {
		resultIVL = (Vector)leftIVL.clone();
	    }
	    if (rightIVL!= null) {
		resultIVL = (Vector)rightIVL.clone();
	    }
	    
	    return;	    
	}
		
	//otherwise, scan over two IVLs.

	int i=0;
	int j=0; 

	IVLEntry leftIVLEntry = null;
	IVLEntry rightIVLEntry = null;
	long leftDocNo=0, rightDocNo=0;

	while (i< leftIVL.size() && j< rightIVL.size()) {

	    leftIVLEntry = (IVLEntry)leftIVL.elementAt(i);
	    rightIVLEntry = (IVLEntry)rightIVL.elementAt(j);
	    
	    leftDocNo = leftIVLEntry.getDocNo();
	    rightDocNo = rightIVLEntry.getDocNo();

	    //assume IVLentries in an IVL sorted by docno in ascending order
	    if (leftDocNo < rightDocNo) {
		resultIVL.addElement(new IVLEntry
				     (leftDocNo, 
				      leftIVLEntry.getPositionList()));
		i++;
		continue;
	    }
	    else if (leftDocNo > rightDocNo) {
		resultIVL.addElement(new IVLEntry
				     (rightDocNo, 
				      rightIVLEntry.getPositionList()));
		j++;
		continue;
	    }
	    else { //(leftDocNo == rightDocNo) 
		
		Vector leftPosList = leftIVLEntry.getPositionList();
		Vector rightPosList = rightIVLEntry.getPositionList();

		//first add all position pairs of rightIVLEntry into the result
		Vector qualifiedPosList = (Vector)rightPosList.clone();
	     		
		// Then scan position pair lists of leftIVLEntry
		for (int pi=0; pi<leftPosList.size(); pi++) {

		    Position leftPos = (Position)leftPosList.elementAt(pi);
		    int pj;
		    
		    for (pj=0; pj<rightPosList.size(); pj++) {
			Position rightPos = (Position)rightPosList.elementAt(pj);
			if (leftPos.getType() == Position.PT_PAIR
			&& rightPos.getType() == Position.PT_PAIR) {

			    if (leftPos.compareTo(rightPos)==0) {
				//this one has been added from rightIVLEntry
				break; 
			    }
			}
			else { // error in query execution plan
			    throw new IMException();			    
			}
		    } // end for pj

		    //sure it hasn't been added from rightIVLEntry
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

	}  //end of while

	while (i < leftIVL.size()) {
	    leftIVLEntry = (IVLEntry)leftIVL.elementAt(i);	       
	    leftDocNo = leftIVLEntry.getDocNo();
	    resultIVL.addElement(new IVLEntry
				     (leftDocNo, 
				      leftIVLEntry.getPositionList()));
	    i++;
	}
	while (j < rightIVL.size()) {    
	    rightIVLEntry = (IVLEntry)rightIVL.elementAt(j);
	    rightDocNo = rightIVLEntry.getDocNo();
	    resultIVL.addElement(new IVLEntry
				     (rightDocNo, 
				      rightIVLEntry.getPositionList()));
	    j++;
	}
    }//end of evaluate()

}  //end of class OROp

