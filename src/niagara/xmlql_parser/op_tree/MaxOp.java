
/**********************************************************************
  $Id: MaxOp.java,v 1.1 2003/02/05 21:26:23 jinli Exp $


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
 * This is the class for the max operator, that is a type of group
 * operator.
 *
 * @version 1.0
 *
 */

package niagara.xmlql_parser.op_tree;


import org.w3c.dom.*;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Op;
import niagara.xmlql_parser.syntax_tree.*;


public class MaxOp extends groupOp {

    /////////////////////////////////////////////////////////////////
    // These are the private members of the maxing operator       //
    /////////////////////////////////////////////////////////////////

    // This is the attribute on which maxing is done
    //
    Attribute maxingAttribute;


    /////////////////////////////////////////////////////////////////
    // These are the methods of the class                          //
    /////////////////////////////////////////////////////////////////

    /**
     * This function sets the skolem attributes on which grouping is
     * done, and the attribute that is maxed
     *
     * @param skolemAttributes Attributes on which grouping is done
     * @param maxingAttribute Attribute on which maxing is done
     */

    public void setMaxingInfo (skolem skolemAttributes,
				Attribute maxingAttribute) {

	// Set the maxing attribute
	//
	this.maxingAttribute = maxingAttribute;

	// Set the skolem attributes in the super class
	//
	this.setSkolemAttributes(skolemAttributes);
    }


    /**
     * This function returns the averaging attributes
     *
     * @return Maxing attribute of the operator
     */

    public Attribute getMaxingAttribute () {
	return maxingAttribute;
    }

    public void dump() {
	System.out.println("MaxOp");
	skolemAttributes.dump();
	System.err.println(maxingAttribute.getName());
    }
    
    public Op copy() {
        MaxOp op = new MaxOp();
        op.setMaxingInfo(skolemAttributes, maxingAttribute);
        return op;
    }

    public int hashCode() {
        return skolemAttributes.hashCode() ^ maxingAttribute.hashCode();
    }

    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof MaxOp)) return false;
        if (obj.getClass() != MaxOp.class) return obj.equals(this);
        MaxOp other = (MaxOp) obj;
        return skolemAttributes.equals(other.skolemAttributes) &&
                maxingAttribute.equals(other.maxingAttribute);
    }    
}
