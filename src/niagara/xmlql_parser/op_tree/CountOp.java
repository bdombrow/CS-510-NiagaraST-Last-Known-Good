
/**********************************************************************
  $Id: CountOp.java,v 1.2 2000/08/09 23:54:18 tufte Exp $


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

import com.ibm.xml.parser.*;
import org.w3c.dom.*;
import niagara.xmlql_parser.syntax_tree.*;

public class CountOp extends groupOp {

    /////////////////////////////////////////////////////////////////
    // These are the private members of the count operator       //
    /////////////////////////////////////////////////////////////////

    // This is the attribute on which counting is done
    //
    schemaAttribute countingAttribute;


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

    public CountOp (Class[] algoList) {

	// Call the constructor of the super class
	//
	super(new String("Count"), algoList);

	// Initially no count attribute
	//
	countingAttribute = null;
    }

    /**
     * This constructor initialize the class with the operator name
     * and the list of algorithms. This is used by derived classes
     * (if any)
     *
     * @param operatorName The name of the operator
     * @param algoList The list of algorithms for the operator
     */

    protected CountOp (String operatorName, Class[] algoList) {

	// Call the constructor of the super class
	//
	super(operatorName, algoList);

	// Initially no count attribute
	//
	countingAttribute = null;
    }
    
    /**
     * This function sets the skolem attributes on which grouping is
     * done, and the attribute that is counted
     *
     * @param skolemAttributes Attributes on which grouping is done
     * @param countAttribute Attribute on which counting is done
     */

    public void setCountingInfo (skolem skolemAttributes,
				schemaAttribute countingAttribute) {

	// Set the counting attribute
	//
	this.countingAttribute = countingAttribute;

	// Set the skolem attributes in the super class
	//
	this.setSkolemAttributes(skolemAttributes);
    }


    /**
     * This function returns the counting attributes
     *
     * @return Counting attribute of the operator
     */

    public schemaAttribute getCountingAttribute () {

	// Return the counting attribute
	//
	return countingAttribute;
    }

    public void dump() {
	System.out.println("CountOp");
    }
}
