
/**********************************************************************
  $Id: PhysicalMaxOperator.java,v 1.5 2003/03/19 00:36:09 tufte Exp $


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


package niagara.query_engine;

import java.util.ArrayList;

import org.w3c.dom.*;

import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.Op;


/**
 * This is the <code>PhysicalSumOperator</code> that extends the
 * <code>PhysicalGroupOperator</code> with the implementation of
 * Sum (a form of grouping)
 *
 * @version 1.0
 *
 */

public class PhysicalMaxOperator extends PhysicalAggregateOperator {

    /**
     * This function updates the statistics with a value
     *
     * @param newValue The value by which the statistics are to be
     *                 updated
     */
    public void updateAggrResult (PhysicalAggregateOperator.AggrResult result, 
				  Object ungroupedResult) {
	// increm num values and update the max
	double newValue = ((Double) ungroupedResult).doubleValue();
	result.count++;
	if (newValue > result.doubleVal)  // doubleVal holds max
	    result.doubleVal = newValue;
    }


    /////////////////////////////////////////////////////////////////////////
    // These functions are the hooks that are used to implement specific   //
    // sum operator (specializing the group operator)                  //
    /////////////////////////////////////////////////////////////////////////


    /**
     * This function constructs a ungrouped result from a tuple
     *
     * @param tupleElement The tuple to construct the ungrouped result from
     *
     * @return The constructed object; If no object is constructed, returns
     *         null
     */

    protected final Object constructUngroupedResult (StreamTupleElement 
						     tupleElement) 
	throws ShutdownException {
	return getDoubleValue(tupleElement);
    }

    /**
     * This function returns an empty result in case there are no groups
     *
     * @return The result when there are no groups. Returns null if no
     *         result is to be constructed
     */
    protected final Node constructEmptyResult () {
	return null;
    }

    /**
     * This function constructs a result from the grouped partial and final
     * results of a group. Both partial result and final result cannot be null
     *
     * @param partialResult The partial results of the group (this can be null)
     * @param finalResult The final results of the group (this can be null)
     *
     * @return A results merging partial and final results; If no such result,
     *         returns null
     */

    protected final Node constructAggrResult (
			   PhysicalAggregateOperator.AggrResult partialResult,
			   PhysicalAggregateOperator.AggrResult finalResult) {

	// Create number of values and sum of values variables
	int numValues = 0;
	double max = 0;

	if (partialResult != null) {
	    numValues += partialResult.count;
	    if (partialResult.doubleVal > max)
		max = partialResult.doubleVal;
	}
	if (finalResult != null) {
	    numValues += finalResult.count;
	    if (finalResult.doubleVal > max) 
		max = finalResult.doubleVal;
	}

	// If the number of values is 0, sum does not make sense
	if (numValues == 0) {
	    assert false : "KT don't think returning null is ok";
	    //return null;
	}

	Element resultElement = doc.createElement("Max");
	Text childElement = doc.createTextNode(Double.toString(max));
	resultElement.appendChild(childElement);
	return resultElement;
    }

    protected PhysicalOperator getInstance() {
	return new PhysicalMaxOperator();
    }
}
