
/**********************************************************************
  $Id: IVLOp.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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


//IVLOp.java
package niagara.search_engine.operators;

import java.util.Vector;
import niagara.search_engine.indexmgr.*;

/**
 * IVLOp is an operator that returns an inverted list given:
 * 1) a word, or
 * 2) an element name, or
 * 3) a numeric predicate
 *
 * It operates by looking up the appropriate lexicon and inverted file
 *
 */

public class IVLOp extends AbstractOperator
{
    private IndexMgr indexMgr = null;
    private String item;
    private boolean itemIsElement;
    private double value;
    private boolean valueIsValid;
    
    /**
     * Constructor
     * The parameters are :
     * indexMgr, item , itemIsElement, [value,valueIsValid]
     */

    public IVLOp (Vector parameters) {
	
	super(parameters);	
	indexMgr = (IndexMgr)parameters.elementAt(0);
	item = ((String)parameters.elementAt(1)).toLowerCase();
	itemIsElement = ((Boolean)parameters.elementAt(2)).booleanValue();
	if (parameters.size() > 3) {
	    value = ((Double)parameters.elementAt(3)).doubleValue();
	    valueIsValid = ((Boolean)parameters.elementAt(4)).booleanValue();
	}	
    }

    /**
     * Operator evaluation.
     */
    public void evaluate () throws IMException {

	isEvaluated = true;	

	if (item==null) {
	    return;
	}

	resultIVL = indexMgr.getInvertedList (item, itemIsElement);
	
    }
}
