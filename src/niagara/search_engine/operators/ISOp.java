
/**********************************************************************
  $Id: ISOp.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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


// ISOp.java
package niagara.search_engine.operators;

import java.util.*;
import java.io.*;
import niagara.search_engine.indexmgr.*;

/** 
 *IS operator takes two inverted lists (elemIVL and stringIVL), 
 * and returns the IVL of qualified elements .
 */
public class ISOp extends AbstractOperator
{
    private Vector elemIVL;
    private Vector stringIVL; 
    
    public ISOp(Vector parameters) 
    {
	super(parameters);
	elemIVL = (Vector)parameters.elementAt(0);
	stringIVL  = (Vector)parameters.elementAt(1);
	opType = "ISOP";	
    }
    
   //<NOTE: change 1, 2.1
    public ISOp() 
    {
	opType = "ISOP";
    }

    public void addIVLs(Vector parameters) 
    {
	elemIVL = (Vector)parameters.elementAt(0);
	stringIVL  = (Vector)parameters.elementAt(1);	
    }

    public String toString() {
	return "ISOp";
    }
  //NOTE>
    
    public void evaluate() throws IMException 
    {
	isEvaluated = true;

	if ((elemIVL == null) || (stringIVL == null)) {
	    return;
	}
		
	//go through the two IVLs and find the qualified elements
	int i=0, j=0;
	IVLEntry elemIvlEnt, stringIvlEnt;
	long elemDocno, stringDocno;

	while (i < elemIVL.size() && j < stringIVL.size()) {

	    elemIvlEnt = (IVLEntry)elemIVL.elementAt(i);
	    stringIvlEnt = (IVLEntry)stringIVL.elementAt(j);
	    elemDocno = elemIvlEnt.getDocNo();
	    stringDocno = stringIvlEnt.getDocNo();
	    
	    if (elemDocno < stringDocno) {
		i++;
	    }
	    
	    else if (stringDocno < elemDocno) {
		j++;
	    }
	    
	    else { //(elemDocno == stringDocno) 
	    	IVLEntry ivlent = null;

	    	if (IndexMgr.idxmgr.indexType == IndexMgr.INDEX_T_FULL) {

		    Vector qualifiedPosList = new Vector();
		    Vector elemPosList = elemIvlEnt.getPositionList();
		    Vector stringPosList = stringIvlEnt.getPositionList();
		
		    // scan over two inverted lists and compare
		    for (int pi=0; pi<elemPosList.size(); pi++) {
			Position elemPos = (Position)elemPosList.elementAt(pi);

			for (int pj=0; pj<stringPosList.size(); pj++) {
			    Position stringPos = (Position)stringPosList.elementAt(pj);		    
			    //check if the element contains nothing but the string
			    if (stringPos.isNestedIn(elemPos) == true && 
				stringPos.isProperNestedIn(elemPos) == false) {

				qualifiedPosList.addElement(elemPos);
				break; // do not add elem multiple times
			    }
			}
		    } // end for

		    if (qualifiedPosList.size() != 0) {
			ivlent = new IVLEntry (elemDocno, qualifiedPosList);
		    }
		}
		else {
		    ivlent = new IVLEntry (elemDocno);
		}
		
		if (ivlent != null) {
		    resultIVL.addElement (ivlent);
		}

		i++;
		j++;		

	    }  //end of if (elemDocNo == strDocNo)
	    
	}  // end while
    }	
    
}  //end of class ISOp

	
