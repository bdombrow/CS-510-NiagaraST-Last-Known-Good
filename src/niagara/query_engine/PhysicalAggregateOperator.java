
/**********************************************************************
  $Id: PhysicalAggregateOperator.java,v 1.2 2003/03/19 22:43:36 tufte Exp $


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

public abstract class PhysicalAggregateOperator extends PhysicalGroupOperator {

    Attribute aggrAttr;
    AtomicEvaluator ae;
    ArrayList atomicValues;

    protected abstract PhysicalAggregateOperator getInstance();
    protected abstract Node constructAggrResult(AggrResult partialResult,
						AggrResult finalResult);
    protected abstract void updateAggrResult(AggrResult result, 
					     Object ungroupedResult);

    protected void localInitFrom(LogicalOp logicalOperator) {
	aggrAttr = ((AggregateOp)logicalOperator).getAggrAttr();
    }

    protected void initializeForExecution() {
        ae = new AtomicEvaluator(aggrAttr.getName());
        ae.resolveVariables(inputTupleSchemas[0], 0);
        atomicValues = new ArrayList();
    }

    protected final Double getDoubleValue (StreamTupleElement tupleElement) 
	throws ShutdownException {
	try {
	    // First get the atomic values
	    atomicValues.clear();
	    ae.getAtomicValues(tupleElement, atomicValues);
	    assert atomicValues.size() == 1 : "Need exactly one atomic value";
	    
	    // Try to convert to double 
	    return new Double((String)(atomicValues.get(0)));
	} catch (java.lang.NumberFormatException nfe) {
	    throw new ShutdownException(nfe.getMessage());
	} 
    }

    /**
     * This function merges a grouped result with an ungrouped result
     *
     * @param groupedResult The grouped result that is to be modified (this can
     *                      be null)
     * @param ungroupedResult The ungrouped result that is to be grouped with
     *                        groupedResult (this can never be null)
     *
     * @return The new grouped result
     */

    protected final Object mergeResults (Object groupedResult,
					 Object ungroupedResult) {

	// Set up the final result - if the groupedResult is null, then
	// create holder for final result, else just use groupedResult
	AggrResult finalResult = null;
	if (groupedResult == null) {
	    finalResult = new AggrResult();
	} else {
	    finalResult = (AggrResult)groupedResult;
	}

	// Add effects of ungrouped result 
	updateAggrResult(finalResult, ungroupedResult);
	return finalResult;
    }

    public Node constructResult (Object partialResult,
				 Object finalResult) {
	return constructAggrResult((AggrResult)partialResult,
				   (AggrResult)finalResult);
    }

    protected PhysicalGroupOperator localCopy() {
        PhysicalAggregateOperator op = getInstance();
	op.aggrAttr = this.aggrAttr;
        return op;
    }

    protected boolean localEquals(Object o) {
        return aggrAttr.equals(((PhysicalAggregateOperator)o).aggrAttr);
    }

    public int hashCode() {
        return groupAttributeList.hashCode() ^ aggrAttr.hashCode();
    }

    /*
     * class to be used to hold and pass around an aggregate result
     * a union of all values needed for numeric aggregates - wastes
     * a bit of space, but saves casting and makes code simpler.
     */

    protected class AggrResult {
	int count;
	double doubleVal;
    }
}
