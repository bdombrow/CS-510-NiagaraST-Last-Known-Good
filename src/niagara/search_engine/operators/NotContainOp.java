
/**********************************************************************
  $Id: NotContainOp.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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


package niagara.search_engine.operators;

import java.util.Vector;
import niagara.search_engine.indexmgr.*;

/**
 * NotContainOp is the operator that processes inverse containment relationship
 * between two inverted lists.
 *
 */

public class NotContainOp extends AbstractOperator{
    private Vector containerIVL;
    private Vector containeeIVL;
    
    
  //<NOTE: change 1, 2.1
    /**
     * Constructor
     */
    public NotContainOp() {
    }

    public void addIVLs(Vector operands) {
	containerIVL = (Vector)operands.elementAt(0);
	containeeIVL = (Vector)operands.elementAt(1);	
    }

    public String toString() {
        return "NotContainOp";
    }
  //NOTE>

    /**
     * Constructor
     */
    public NotContainOp(Vector operands) {

	super(operands);
	containerIVL = (Vector)operands.elementAt(0);
	containeeIVL = (Vector)operands.elementAt(1);	
	opType = "NOTCONTAINOP";	
    }

    /**
     * Operator evaluation.
     */
    public void evaluate() throws IMException {
	
	isEvaluated = true;
	Vector parentIVL = containerIVL;
	Vector childIVL = containeeIVL;

	if (childIVL == null || parentIVL == null) {
	    return;
	}
	
	int i=0, j=0;
	IVLEntry parentIvlEnt, childIvlEnt;
	long parentDocno, childDocno;

	while (i < parentIVL.size() && j < childIVL.size()) {

	    parentIvlEnt = (IVLEntry)parentIVL.elementAt(i);
	    childIvlEnt = (IVLEntry)childIVL.elementAt(j);
	    parentDocno = parentIvlEnt.getDocNo();
	    childDocno = childIvlEnt.getDocNo();
	    
	    if (parentDocno < childDocno) {
		resultIVL.addElement(parentIvlEnt);
		i++;
		continue;		
	    }
	    else if (childDocno < parentDocno) {
		j++;
		continue;
	    }
	    
	    else { //(parentDocno == childDocno) 
		Vector qualifiedPosList = new Vector();
		Vector parentPosList = parentIvlEnt.getPositionList();
		Vector childPosList = childIvlEnt.getPositionList();
		
		// scan over two inverted lists and compare
		for (int pi=0; pi<parentPosList.size(); pi++) {
		    Position parentPos = (Position)parentPosList.elementAt(pi);

		    for (int pj=0; pj<childPosList.size(); pj++) {
			Position childPos = (Position)childPosList.elementAt(pj);
			//Object childPosObj = childPosList.elementAt(pj);
			if (!childPos.isNestedIn(parentPos)) {
			    qualifiedPosList.addElement(parentPos);
			    break; // do not add parent multiple times
			}
		    } // end for
		}// end for
		
		if (qualifiedPosList.size() != 0) {
		    resultIVL.addElement (new IVLEntry
					(parentDocno, qualifiedPosList));
		}
		i++;
		j++;		
	    }  
	}  
    }
}
