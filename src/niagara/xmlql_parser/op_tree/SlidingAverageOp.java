
/**********************************************************************
  $Id: SlidingAverageOp.java,v 1.2 2003/02/05 21:46:03 jinli Exp $


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

public class SlidingAverageOp extends SlidingWindowOp {

    /////////////////////////////////////////////////////////////////
    // These are the private members of the average operator       //
    /////////////////////////////////////////////////////////////////

    // This is the attribute on which averaging is done
    //
    Attribute averageAttribute;
	int range;
	int every;

    /////////////////////////////////////////////////////////////////
    // These are the methods of the class                          //
    /////////////////////////////////////////////////////////////////

    /**
     * This function sets the skolem attributes on which grouping is
     * done, and the attribute that is average
     *
     * @param skolemAttributes Attributes on which grouping is done
     * @param averageAttribute Attribute on which averaging is done
     */

    public void setAverageInfo (skolem skolemAttributes,
				Attribute averageAttribute) {

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

    public Attribute getAveragingAttribute () {

	// Return the averaging attribute
	//
	return averageAttribute;
    }

   
    public void setWindowInfo (int range, int every) {
    this.range = range;
    this.every = every;
    }
    
    public int getWindowRange () {
    return this.range;
    }
    
    public int getWindowEvery () {
    return this.every;
    }
    
    public Attribute getAverageAttribute () {
	return averageAttribute;
    }

    public void dump() {
	System.out.println("SlidingAverageOp");
	skolemAttributes.dump();
	System.err.println(averageAttribute.getName());
    }
    
    public Op copy() {
        SlidingAverageOp op = new SlidingAverageOp();
        op.setAverageInfo(skolemAttributes, averageAttribute);
        op.setWindowInfo (range, every);
        return op;
    }

    public int hashCode() {
        return skolemAttributes.hashCode() ^ averageAttribute.hashCode();
    }

    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof SlidingAverageOp)) return false;
        if (obj.getClass() != SlidingAverageOp.class) return obj.equals(this);
        SlidingAverageOp other = (SlidingAverageOp) obj;
        return skolemAttributes.equals(other.skolemAttributes) &&
                averageAttribute.equals(other.averageAttribute);
    }

}
