
/**********************************************************************
  $Id: ConformstoOp.java,v 1.2 2003/07/08 02:12:08 tufte Exp $


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
 * conformsto operator.
 * returns only ivl entries of documents that conforms to the given dtd.
 *
 */
public class ConformstoOp extends ContainedOp
{
    protected Vector queryIVL;
    protected Vector dtdIVL;

    public ConformstoOp() 
    {
	opType = "CONFORMSTOOP";	
    }

    public void addIVLs(Vector operands) 
    {
	queryIVL = (Vector)operands.elementAt(0);
	dtdIVL = (Vector)operands.elementAt(1);
    }

    public ConformstoOp(Vector operands) 
    {
	super(operands);
	queryIVL = (Vector)operands.elementAt(0);
	dtdIVL = (Vector)operands.elementAt(1);
	opType = "CONFORMSTOOP";
	
    }
    
    public void evaluate() throws IMException 
    {
	Vector parentIVL = dtdIVL;
	Vector childIVL = queryIVL;

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
		resultIVL.addElement (childIvlEnt);
		i++;
		j++;
	    }	    
	}  
    }

    public String toString() {
	return "ConformstoOp";
    }
	
} 

	
