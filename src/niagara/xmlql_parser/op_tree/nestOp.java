
/**********************************************************************
  $Id: nestOp.java,v 1.5 2002/10/27 01:20:21 vpapad Exp $


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
 * This is the class for the nest operator, that is a type of group
 * operator.
 *
 * @version 1.0
 *
 */

package niagara.xmlql_parser.op_tree;


import org.w3c.dom.*;

import niagara.optimizer.colombia.Op;
import niagara.utils.PEException;
import niagara.xmlql_parser.syntax_tree.*;

public class nestOp extends groupOp {

    /////////////////////////////////////////////////////////////////
    // These are the private members of the nest operator          //
    /////////////////////////////////////////////////////////////////

    // This stores the template of the result
    //
    constructBaseNode resultTemplate;


    /////////////////////////////////////////////////////////////////
    // These are the methods of the class                          //
    /////////////////////////////////////////////////////////////////

    /**
     * This function sets the construct part of the nest operator, which
     * specifies the result template
     *
     * @param resultTemplate The template of the result
     */

    public void setResTemp (constructBaseNode resultTemplate) {

	// Set the result template
	//
	this.resultTemplate = resultTemplate;

	// The result template has to be rooted at a construct internal
	// node - get the skolem from the result template
	//
	skolem skolemAttributes = ((constructInternalNode) resultTemplate).getSkolem();

	// Set this in the super class
	//
	this.setSkolemAttributes(skolemAttributes);
    }


    /**
     * This function returns the result template associated with the operator
     *
     * @return Result template associated with the operator
     */

    public constructBaseNode getResTemp () {

	// Return the result template
	//
	return resultTemplate;
    }

    public void dump() {
	System.out.println("NestOp");
    }

    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op copy() {
        throw new PEException("Optimization is not supported for this operator");        
    }

    /**
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object obj) {
        return this == obj;
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        return System.identityHashCode(this);
    }
}
