
/**********************************************************************
  $Id: CountOp.java,v 1.5 2002/10/27 01:20:21 vpapad Exp $


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


/**
 * This is the class for the count operator, that is a type of group
 * operator.
 *
 * @version 1.0
 *
 */

package niagara.xmlql_parser.op_tree;


import java.util.ArrayList;

import org.w3c.dom.*;

import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.utils.PEException;
import niagara.xmlql_parser.syntax_tree.*;

public class CountOp extends groupOp {
    // This is the attribute on which counting is done
    Attribute countingAttribute;

    /**
     * This function sets the skolem attributes on which grouping is
     * done, and the attribute that is counted
     *
     * @param skolemAttributes Attributes on which grouping is done
     * @param countAttribute Attribute on which counting is done
     */

    public void setCountingInfo (skolem skolemAttributes,
				Attribute countingAttribute) {
	this.countingAttribute = countingAttribute;

	// Set the skolem attributes in the super class
	this.setSkolemAttributes(skolemAttributes);
    }


    /**
     * This function returns the counting attributes
     *
     * @return Counting attribute of the operator
     */
    public Attribute getCountingAttribute () {
	return countingAttribute;
    }

    public void dump() {
	System.out.println("CountOp");
    }

    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op copy() {
        CountOp cop = new CountOp();
        cop.setCountingInfo(skolemAttributes, countingAttribute);
        return cop;
    }
}
