
/**********************************************************************
  $Id: averageOp.java,v 1.1 2000/05/30 21:03:29 tufte Exp $


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
 * This is the class for the average operator, that is a type of group
 * operator.
 *
 * @version 1.0
 *
 */

package niagara.xmlql_parser.op_tree;

import com.ibm.xml.parser.*;
import org.w3c.dom.*;
import niagara.xmlql_parser.syntax_tree.*;

public class averageOp extends groupOp {

    /////////////////////////////////////////////////////////////////
    // These are the private members of the average operator       //
    /////////////////////////////////////////////////////////////////

    // This is the attribute on which averaging is done
    //
    schemaAttribute averageAttribute;


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

    public averageOp (Class[] algoList) {

	// Call the constructor of the super class
	//
	super(new String("Average"), algoList);

	// Initially no average attribute
	//
	averageAttribute = null;
    }

    /**
     * This constructor initialize the class with the operator name
     * and the list of algorithms. This is used by derived classes
     * (if any)
     *
     * @param operatorName The name of the operator
     * @param algoList The list of algorithms for the operator
     */

    protected averageOp (String operatorName, Class[] algoList) {

	// Call the constructor of the super class
	//
	super(operatorName, algoList);

	// Initially no average attribute
	//
	averageAttribute = null;
    }
    
    /**
     * This function sets the skolem attributes on which grouping is
     * done, and the attribute that is average
     *
     * @param skolemAttributes Attributes on which grouping is done
     * @param averageAttribute Attribute on which averaging is done
     */

    public void setAverageInfo (skolem skolemAttributes,
				schemaAttribute averageAttribute) {

	// Set the average attribute
	//
	this.averageAttribute = averageAttribute;

	// Set the skolem attributes in the super class
	//
	this.setSkolemAttributes(skolemAttributes);
    }


    /**
     * This function returns the averaging attributes
     *
     * @return Averaging attribute of the operator
     */

    public schemaAttribute getAveragingAttribute () {

	// Return the averaging attribute
	//
	return averageAttribute;
    }
}
