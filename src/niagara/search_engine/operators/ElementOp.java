
/**********************************************************************
  $Id: ElementOp.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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

import java.util.*;
import java.io.*;
import niagara.search_engine.indexmgr.*;

/**
 * returns an element inverted list of the given element.
 *
 *
 */ 
public class ElementOp extends AbstractOperator 
{
    private IndexMgr indexMgr;
    private String element;
 
    public ElementOp(Vector parameters) 
    {
	super(parameters);
	
	opType = "ELEMENTOP";
	
	indexMgr = (IndexMgr)parameters.elementAt(0);
	
	element = (String)parameters.elementAt(1);
	
    }
    
    public ElementOp(String str) 
    {
      element = str;
    }

    public String toString() {
      return "ElementOp("+element+")";
    }

    public void evaluate() throws IMException
    {
	isEvaluated = true;
	
	if (element == null || indexMgr == null) {
	    System.err.println("ERROR!!!");
	    return;
	}
	
	//	    System.err.println("before idx!");
	//	    System.err.println(element);
	Vector ivl = indexMgr.getInvertedList(element, true);
	resultIVL = ivl;	
	//	    System.err.println("after idx!!");
    }
}
