
/**********************************************************************
  $Id: AbstractOperator.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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


// AbstractOperator.java
package niagara.search_engine.operators;

import java.io.*;
import java.util.*;
import niagara.search_engine.indexmgr.*;

/**
 * super class of all IVL operators.
 *
 */
public abstract class AbstractOperator 
{
    /** 
     * opType can be one of the following:
     * "ANDOP",   "CONTAINOP", "CONTAINEDOP", "DISTANCEOP", 
     * "EXCEPTOP","ISOP",      "IVLOP",     
     * "ORDEROP", "OROP",      "STRINGOP"
     * "CONFORMSTOOP", "DTDOP", "ELEMENTOP"
     * "MERGEOP", "NOTCONTAINOP"
     */
    protected String opType = new String();  
  
    protected Vector operands = new Vector();
    
    protected Vector resultIVL = new Vector();

    protected boolean isEvaluated = false;
 
    public AbstractOperator(Vector operands) 
    {
	super();
	this.operands = operands;
    }
    
  //<NOTE: added 1,2.1.
    public AbstractOperator() {
    }

    public void addIVLs(Vector ivls) {
    }
  //NOTE>  

    public void evaluate() throws IMException
    {
	isEvaluated = true;	
    }
    
    public String getOpType() 
    {
	return opType;
    }
    
    public Vector getOperands() 
    {
	return operands;
	
    }

    /**
     * if the operator is not evaluated yet, evaluate it;
     * return the result IVL
     */
    public Vector getResult() throws IMException
    {
	if (isEvaluated == false) {    
	    evaluate();
	}
	return resultIVL;
    }

    public boolean ifEvaluated() 
    {
	return isEvaluated;
    }

    /**
     * print the result IVL
     */
    public void printResult() 
    {
	if (resultIVL == null ) {
	    System.out.println("null result");
	    return;	    
	}
	
	for (int i = 0; i< resultIVL.size(); i++) {
	    IVLEntry entry = (IVLEntry)resultIVL.elementAt(i);
	    long docno = entry.getDocNo();

	    Vector poslist = entry.getPositionList();

	    System.out.println ("In document " + 
		IndexMgr.idxmgr.getDocName(docno));
	    System.out.println(IndexMgr.idxmgr.retrieve(docno,poslist));
	}
    }
    
}

