/**********************************************************************
  $Id: PhysicalWindowCount.java,v 1.2 2006/12/18 21:50:02 jinli Exp $


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


package niagara.physical;

import niagara.utils.Tuple;
import niagara.utils.BaseAttr;
import niagara.utils.IntegerAttr;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.Text;

/**
 * This is the <code>PhysicalCountOperator</code> that extends the
 * <code>PhysicalGroupOperator</code> with the implementation of
 * Count (a form of grouping)
 *
 * @version 1.0
 *
 */

public class PhysicalWindowCount extends PhysicalWindowAggregate {

	/**
	 * This function updates the statistics with a value
	 *
	 * @param newValue The value by which the statistics are to be
	 *                 updated
	 */
	public void updateAggrResult (PhysicalWindowAggregate.AggrResult result, 
				  Object ungroupedResult) {
	// Increment the number of values
	// KT - is this correct??
	// code from old mrege results:
	//finalResult.updateStatistics(((Integer) ungroupedResult).intValue());
	
	//int size = ((Vector)ungroupedResult).size();
	//assert ((Integer)((Vector)ungroupedResult).get(0)).intValue() == 1 :
	assert ((Integer)ungroupedResult).intValue() == 1:
			"KT BAD BAD BAD";

	result.count++;	
	}
	
	

    
    
	/////////////////////////////////////////////////////////////////////////
	// These functions are the hooks that are used to implement specific   //
	// Count operator (specializing the group operator)                  //
	/////////////////////////////////////////////////////////////////////////


	/**
	 * This function constructs a ungrouped result from a tuple
	 *
	 * @param tupleElement The tuple to construct the ungrouped result from
	 *
	 * @return The constructed object; If no object is constructed, returns
	 *         null
	 */

	protected final Object constructUngroupedResult (Tuple 
							 tupleElement) {

	// First get the atomic values
	atomicValues.clear();
	ae.getAtomicValues(tupleElement, atomicValues);

	assert atomicValues.size() == 1 : "Must have exactly one atomic value";

	return new Integer(1);
	
	}

	/**
	 * This function returns an empty result in case there are no groups
	 *
	 * @return The result when there are no groups. Returns null if no
	 *         result is to be constructed
	 */

	protected final BaseAttr constructEmptyResult () {
	// Create an Count result element
	IntegerAttr resultElement = new IntegerAttr(0);
	
	// Return the result element
	return resultElement;
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

	protected final BaseAttr constructAggrResult (
		PhysicalWindowAggregate.AggrResult partialResult,
		PhysicalWindowAggregate.AggrResult finalResult) {
	int numValues = 0;
	double timestamp = 0;

	if (partialResult != null) 
		numValues += partialResult.count;
	
	if (finalResult != null) 
		numValues += finalResult.count;

	IntegerAttr resultElement = new IntegerAttr(numValues);	
	return resultElement;
	}

	protected PhysicalWindowAggregate getInstance() {
	return new PhysicalWindowCount();
	}
}
