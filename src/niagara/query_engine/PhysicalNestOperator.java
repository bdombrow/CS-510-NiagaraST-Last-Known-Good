/**********************************************************************
  $Id: PhysicalNestOperator.java,v 1.13 2003/07/03 19:56:52 tufte Exp $


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

import org.w3c.dom.*;
import niagara.utils.*;
import niagara.logical.Nest;
import niagara.xmlql_parser.syntax_tree.*;
import niagara.ndom.*;
import niagara.optimizer.colombia.*;

/**
 * This is the <code>PhysicalNestOperator</code> that extends the
 * <code>PhysicalGroupOperator</code> with the implementation of
 * nesting (a form of grouping)
 *
 * @version 1.0
 *
 */

public class PhysicalNestOperator extends PhysicalGroupOperator {

    private static final boolean[] blockingSourceStreams = { true };

    // The result template for the nest operator
    private constructBaseNode resultTemplate;

    // The root tag of the constructed results
    private String rootTag;

    private int numGroupingAttributes;

    // temporary result list storage place
    private NodeVector resultList;

    /**
     * This is the constructor for the PhysicalNestOperator class that
     * initializes it with the appropriate logical operator, source streams,
     * sink streams, and the responsiveness to control information.
     *
     * @param logicalOperator The logical operator that this operator implements
     * @param sourceStreams The Source Streams associated with the operator
     * @param sinkStreams The Sink Streams associated with the
     *                           operator
     * @param responsiveness The responsiveness to control messages, in milli
     *                       seconds
     */
    public PhysicalNestOperator() {
        setBlockingSourceStreams(blockingSourceStreams);
    }

    /**
     * @see niagara.optimizer.colombia.PhysicalOp#initFrom(LogicalOp)
     */
    public void localInitFrom(LogicalOp logicalOp) {
	this.resultTemplate = ((Nest)logicalOp).getResTemp();
    }

    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    protected PhysicalGroupOperator localCopy() {
        PhysicalNestOperator op = new PhysicalNestOperator();
        // XXX vpapad: We treat resultTemplate as an immutable object
        op.resultTemplate = resultTemplate;
        return op;
    }

    protected boolean localEquals(Object other) {
	return resultTemplate.equals(
                    ((PhysicalNestOperator)other).resultTemplate);
    }


    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
	return groupAttributeList.hashCode() ^ resultTemplate.hashCode();
    }

    /////////////////////////////////////////////////////////////////////////
    // These functions are the hooks that are used to implement specific   //
    // nest operator (specializing the group operator)                     //
    /////////////////////////////////////////////////////////////////////////

    /**
     * This function is called to initialize a grouping operator for execution
     * by setting up relevant structures etc.
     * PhysicalGroupOperator calls this function...
     */

    protected final void initializeForExecution () {
	rootTag = (String) ((constructInternalNode)
			 resultTemplate).getStartTag().getSdata().getValue();
	numGroupingAttributes = groupAttributeList.size();
	resultList = new NodeVector();

	// old code - am not using skolem anymore... KT
	//skolem grouping = ((constructInternalNode) resultTemplate).getSkolem();
	//numGroupingAttributes = grouping.getVarList().size();
    }


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

	// Construct the result as per the template for the tuple
	resultList.quickReset();
	PhysicalConstructOperator.constructResult(tupleElement, 
						  resultTemplate,
						  resultList, doc);

	// The list can have a size of only one, get that result
	// and return it
	assert resultList.size() == 1;
	return resultList.get(0);
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
	// ungrouped nodes was a node list, now it is the whole node
	// created by constructResult, 
	// grouped result will be a node containing a root element
	// followed by a list of children 
	// if grouped result is null, we first add stripped down root element

	NodeVector resultVec;

	if (groupedResult == null) {
	    resultVec = new NodeVector();
	    // add root element to the result
	    resultVec.add(((Node)ungroupedResult).cloneNode(false));
	} else {
	    resultVec = (NodeVector) groupedResult;
	}

	// The ungrouped result is a node list
	NodeList ungroupedNodes = ((Node)ungroupedResult).getChildNodes();

	// Add all items in ungrouped result
	int numNodes = ungroupedNodes.getLength();

	for (int node = 0; node < numNodes; ++node) {
	    resultVec.add(ungroupedNodes.item(node));
	}

	// Return the grouped result
	return resultVec;
    }

    /**
     * This function returns an empty result in case there are no groups
     *
     * @return The result when there are no groups. Returns null if no
     *         result is to be constructed
     */

    protected final Node constructEmptyResult () {

	// If the number of grouping attributes is 0, then construct result,
	// else return null
	if (numGroupingAttributes == 0) {
	    // Just create and return a Element with the root tag
	    return doc.createElement(rootTag);
	} else {
	    return null;
	}
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

    protected final Node constructResult (Object partialResult,
					  Object finalResult) {
	// first element in finalResult and partial result 
	// should be the same
	Element resultElement;
	if(finalResult != null)
	    resultElement = (Element)((NodeVector)finalResult).get(0);
	else if(partialResult != null)
	    resultElement = (Element)((NodeVector)partialResult).get(0);
	else
	    resultElement = (Element)constructEmptyResult();

	// If the partial result is not null, add all elements 
	// of the partial result
	if (partialResult != null) {
	    // Add nodes in vector to result
	    addNodesToResult(resultElement, (NodeVector)partialResult);
	}
	
	// If the final result is not null, add all element of the final result
	if (finalResult != null) {
	    // Add nodes in vector to result
	    addNodesToResult(resultElement, (NodeVector) finalResult);
	}
	return resultElement;
    }


    /**
     * This function add the elements of a node vector to the result element
     *
     * @param resultElement The result Element
     * @param nodeVector The vector of nodes to be added
     */
    private void addNodesToResult (Element resultElement, 
				   NodeVector nodeVector) {

	// Loop over all elements and add a clone to the result element
	int numNodes = nodeVector.size();

	// ignore node at index 0, that is the root element...
	for (int node = 1; node < numNodes; ++node) {
	    resultElement.appendChild(((Node)
			 nodeVector.get(node)).cloneNode(true));
	}
    }

    /**
     * @see niagara.optimizer.colombia.PhysicalOp#FindLocalCost(ICatalog, LogicalProperty, LogicalProperty[])
     */
    public final Cost findLocalCost(
        ICatalog catalog,
        LogicalProperty[] inputLogProp) {
	// KT - stolen from construct and PhysicalGroup
	// XXX vpapad: really naive. Only considers the hashing cost
        float inpCard = inputLogProp[0].getCardinality();
        float outputCard = logProp.getCardinality();

        double cost = inpCard * catalog.getDouble("tuple_reading_cost");
        cost += inpCard * catalog.getDouble("tuple_hashing_cost");
        cost += outputCard * catalog.getDouble("tuple_construction_cost");

        // XXX vpapad: Absolutely no connection to reality!
        // We consider only a fixed cost per output tuple
	cost += constructTupleCost(catalog) * getLogProp().getCardinality();
        return new Cost(cost);            
    }
    
    /**
     * @see niagara.query_engine.PhysicalOperator#constructTupleSchema(TupleSchema[])
     */
    public void constructTupleSchema(TupleSchema[] inputSchemas) {
        super.constructTupleSchema(inputSchemas);
        resultTemplate.replaceVar(new varTbl(inputSchemas[0]));        
    }
}
