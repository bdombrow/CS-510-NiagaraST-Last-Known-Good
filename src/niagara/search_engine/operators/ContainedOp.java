
/**********************************************************************
  $Id: ContainedOp.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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


//ContainedOp.java
package niagara.search_engine.operators;

import java.util.*;
import java.io.*;
import niagara.search_engine.indexmgr.*;

/**
 * ContainedOp processes on two inverted lists, containeeIVL and containerIVL.
 * It returns an IVL of quailified entries in containeeIVL.
 */

public class ContainedOp extends AbstractOperator 
{
    private Vector containeeIVL;
    private Vector containerIVL;
    
  //<NOTE: added 1, 2.1
    public ContainedOp() 
    {
	opType = "CONTAINEDOP";	
    }

    public void addIVLs(Vector operands) 
    {
	containeeIVL = (Vector)operands.elementAt(0);
	containerIVL = (Vector)operands.elementAt(1);
    }

    public String toString() {
        return "ContainedOp";
    }
  //NOTE>

    public ContainedOp(Vector operands) 
    {
	super(operands);
	containeeIVL = (Vector)operands.elementAt(0);
	containerIVL = (Vector)operands.elementAt(1);
	opType = "CONTAINEDOP";
	
    }

    public void evaluate() throws IMException 
    {
	Vector parentIVL = containerIVL;
	Vector childIVL = containeeIVL;

	isEvaluated = true;

        if (parentIVL == null || childIVL == null) {
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
		i++;
		continue;
	    }
	    else if (childDocno < parentDocno) {
		j++;
		continue;		
	    }
	    
	    else {//(parentDocno == childDocno)
		Vector qualifiedPosList = new Vector();
		Vector parentPosList = parentIvlEnt.getPositionList();
		Vector childPosList = childIvlEnt.getPositionList();
		
		// scan over two inverted lists and compare
		for (int pi=0; pi<childPosList.size(); pi++) {
		    Position childPos = (Position)childPosList.elementAt(pi);

		    for (int pj=0; pj< parentPosList.size(); pj++) {
			Position parentPos = (Position)parentPosList.elementAt(pj);
			if (childPos.isNestedIn(parentPos)) {
			    qualifiedPosList.addElement(childPos);
			    break; // do not add child multiple times
			}
		    } // end for
		}// end for
		
		if (qualifiedPosList.size() != 0) {
		    resultIVL.addElement (new IVLEntry
					(childDocno, qualifiedPosList));
		}
		i++;
		j++;
		
	    }	    
	}  
    }

} //end of ContainedOp

	
