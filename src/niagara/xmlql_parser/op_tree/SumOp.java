
/**********************************************************************
  $Id: SumOp.java,v 1.3 2000/08/21 00:38:37 vpapad Exp $


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
 * This is the class for the sum operator, that is a type of group
 * operator.
 *
 * @version 1.0
 *
 */

package niagara.xmlql_parser.op_tree;


import org.w3c.dom.*;
import niagara.xmlql_parser.syntax_tree.*;

public class SumOp extends groupOp {

    /////////////////////////////////////////////////////////////////
    // These are the private members of the summing operator       //
    /////////////////////////////////////////////////////////////////

    // This is the attribute on which summing is done
    //
    schemaAttribute summingAttribute;


    /////////////////////////////////////////////////////////////////
    // These are the methods of the class                          //
    /////////////////////////////////////////////////////////////////

    /**
     * This is the constructor that initialize the class with the list
     * of algorithms
     *
     * @param algoList The list of algorithms associated with this
     *                 operator
     */

    public SumOp (Class[] algoList) {

	// Call the constructor of the super class
	//
	super(new String("Sum"), algoList);

	// Initially no summing attribute
	//
	summingAttribute = null;
    }

    /**
     * This constructor initialize the class with the operator name
     * and the list of algorithms. This is used by derived classes
     * (if any)
     *
     * @param operatorName The name of the operator
     * @param algoList The list of algorithms for the operator
     */

    protected SumOp (String operatorName, Class[] algoList) {

	// Call the constructor of the super class
	//
	super(operatorName, algoList);

	// Initially no summing attribute
	//
	summingAttribute = null;
    }
    
    /**
     * This function sets the skolem attributes on which grouping is
     * done, and the attribute that is summed
     *
     * @param skolemAttributes Attributes on which grouping is done
     * @param summingAttribute Attribute on which summing is done
     */

    public void setSummingInfo (skolem skolemAttributes,
				schemaAttribute summingAttribute) {

	// Set the summing attribute
	//
	this.summingAttribute = summingAttribute;

	// Set the skolem attributes in the super class
	//
	this.setSkolemAttributes(skolemAttributes);
    }


    /**
     * This function returns the averaging attributes
     *
     * @return Averaging attribute of the operator
     */

    public schemaAttribute getSummingAttribute () {
	return summingAttribute;
    }

    public void dump() {
	System.out.println("SumOp");
	skolemAttributes.dump();
	summingAttribute.dump();
    }
}
