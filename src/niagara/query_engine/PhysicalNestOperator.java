
/**********************************************************************
  $Id: PhysicalNestOperator.java,v 1.6 2002/08/18 17:38:52 tufte Exp $


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

import java.util.Vector;
import java.util.ArrayList;

import org.w3c.dom.*;
import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;
import niagara.ndom.*;

/**
 * This is the <code>PhysicalNestOperator</code> that extends the
 * <code>PhysicalGroupOperator</code> with the implementation of
 * nesting (a form of grouping)
 *
 * @version 1.0
 *
 */

public class PhysicalNestOperator extends PhysicalGroupOperator {

    ////////////////////////////////////////////////////////////////////
    // These are the private variables of the class                   //
    ////////////////////////////////////////////////////////////////////

    // This has the result template for the nest operator
    //
    constructBaseNode resultTemplate;

    // The root tag of the constructed results
    //
    String rootTag;

    // The number of grouping attributes
    //
    int numGroupingAttributes;

    // The result document - all result elements of nest
    // will be a part of this document. Need this document
    // since we can longer create an element without a parent document
    Document resultDoc;

    ////////////////////////////////////////////////////////////////////
    // These are the methods of the class                             //
    ////////////////////////////////////////////////////////////////////

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

    public PhysicalNestOperator(op logicalOperator,
				SourceTupleStream[] sourceStreams,
				SinkTupleStream[] sinkStreams,
				Integer responsiveness) {

	// Call the constructor of the super class
	//
	super(logicalOperator,
	      sourceStreams,
	      sinkStreams,
	      responsiveness);

	// Get the result template of the nest logical operator
	//
	resultTemplate = ((nestOp) logicalOperator).getResTemp();

	// construct the result document 
	resultDoc = DOMFactory.newDocument();
    }


    /////////////////////////////////////////////////////////////////////////
    // These functions are the hooks that are used to implement specific   //
    // nest operator (specializing the group operator)                     //
    /////////////////////////////////////////////////////////////////////////

    /**
     * This function is called to initialize a grouping operator for execution
     * by setting up relevant structures etc.
     */

    protected final void initializeForExecution () {

        // Get the root tag of the constructed results
	//
	rootTag = (String) ((constructInternalNode)
			    resultTemplate).getStartTag().getSdata().getValue();

	// Get the number of grouping attributes
	//
	skolem grouping = ((constructInternalNode) resultTemplate).getSkolem();

	numGroupingAttributes = grouping.getVarList().size();
    }


    /**
     * This function constructs a ungrouped result from a tuple
     *
     * @param tupleElement The tuple to construct the ungrouped result from
     *
     * @return The constructed object; If no object is constructed, returns
     *         null
     */

    protected final Object constructUngroupedResult (StreamTupleElement tupleElement) {
	
	// Construct the result as per the template for the tuple
	//
	ArrayList resultList =
	    physConstructResult(tupleElement, resultTemplate);

	// The list can have a size of only one, get that result
	//
	Node ungroupedResult = (Node) resultList.get(0);

	// Return the list of children
	//
	return ungroupedResult.getChildNodes();
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
	//
	Vector finalResult;

	if (groupedResult == null) {
	    finalResult = new Vector();
	}
	else {
	    finalResult = (Vector) groupedResult;
	}

	// The ungrouped result is a node list
	//
	NodeList ungroupedNodes = (NodeList) ungroupedResult;

	// Add all items in ungrouped result
	//
	int numNodes = ungroupedNodes.getLength();

	for (int node = 0; node < numNodes; ++node) {

	    finalResult.add(ungroupedNodes.item(node));
	}

	// Return the grouped result
	//
	return finalResult;
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
	//
	if (numGroupingAttributes == 0) {

	    // Just create and return a Element with the root tag
	    //
	    Element temp = resultDoc.createElement(rootTag);
	    
	    return temp;
	}
	else {

	    // Return null
	    //
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

	// Create a result element with root tag
	//
	Element resultElement = resultDoc.createElement(rootTag);

	// If the partial result is not null, add all elements of the partial result
	//
	if (partialResult != null) {

	    // Type cast to a vector
	    //
	    Vector vecPartialResult = (Vector) partialResult;

	    // Add nodes in vector to result
	    //
	    addNodesToResult(resultElement, vecPartialResult);
	}
	
	// If the final result is not null, add all element of the final result
	//
	if (finalResult != null) {

	    // Type cast to a vector
	    //
	    Vector vecFinalResult = (Vector) finalResult;

	    // Add nodes in vector to result
	    //
	    addNodesToResult(resultElement, vecFinalResult);
	}

	// Return the result element
	//
	return resultElement;
    }


    ///////////////////////////////////////////////////////////////////////////
    // These are private methods used in the class                           //
    ///////////////////////////////////////////////////////////////////////////

    /**
     * This function add the elements of a node vector to the result element
     *
     * @param resultElement The result Element
     * @param nodeVector The vector of nodes to be added
     */

    private void addNodesToResult (Element resultElement, 
				   Vector nodeVector) {

	// Loop over all elements and add a clone to the result element
	//
	int numNodes = nodeVector.size();

	for (int node = 0; node < numNodes; ++node) {

	    resultElement.appendChild(((Node)
			 nodeVector.elementAt(node)).cloneNode(true));
	}
    }

    // vpapad: Brought these over from PhysicalConstruct operator,
    // because constructResult is no longer a static method
    private Document txd = DOMFactory.newDocument();
    public ArrayList physConstructResult (
				       StreamTupleElement tupleElement,
				       constructBaseNode templateRoot) {

	// Check if the template root is an internal node or a leaf node
	// and process accordingly
	//
	if (templateRoot instanceof constructLeafNode) {

	    return processLeafNode(tupleElement,
				   (constructLeafNode) templateRoot);
	}
	else if (templateRoot instanceof constructInternalNode) {

	    return processInternalNode(tupleElement,
				       (constructInternalNode) templateRoot);
	}
	else {
	    System.err.println("Error: Unknown construct node type!");
	    return null;
	}
    }


    /**
     * This function processes a leaf node during the construction process
     *
     * @param tupleElement The tuple to construct the result from
     * @param leafConstructNode The leaf node having details of construction
     *
     * @return The list of results constructed
     */

    private ArrayList processLeafNode (StreamTupleElement tupleElement,
					      constructLeafNode leafNode) {

	// Create a place holder for the result
	//
	ArrayList result = new ArrayList();

	// Get the data of the leaf node
	//
	data leafData = leafNode.getData();

	// Check the type of the data
	//
	int type = leafData.getType();

	if (type == dataType.STRING) {

	    // Add the string value to the result
	    //
	    result.add(txd.createTextNode((String) leafData.getValue()));
	}
	else if (type == dataType.ATTR) {

	    // First get the schema attribute
	    //
	    schemaAttribute schema = (schemaAttribute) leafData.getValue();

	    // Now construct result based on whether it is to be interpreted
	    // as an element or a parent
	    //
	    if (schema.getType() == varType.ELEMENT_VAR) {

		// The value of the leafData is a schema attribute - from it
		// get the attribute id in the tuple to construct from
		//
		int attributeId = ((schemaAttribute) leafData.getValue()).getAttrId();

		// Add the attribute as the result
		//
		result.add(tupleElement.getAttribute(attributeId));
	    }
	    else if (schema.getType() == varType.CONTENT_VAR) {

		// The value of the leafData is a schema attribute - from it
		// get the attribute id in the tuple to construct from
		//
		int attributeId = ((schemaAttribute) leafData.getValue()).getAttrId();

		// Get the children of the attribute
		//
		NodeList nodeList =
		    ((Node) tupleElement.getAttribute(attributeId)).getChildNodes();

		// Add all the children to the result
		//
		int numChildren = nodeList.getLength();

		for (int child = 0; child < numChildren; ++child) {

		    result.add(nodeList.item(child));
		}
	    }
	    else {

		System.err.println("Unknown schema attribute type in construct leaf node");
	    }
	}
	else {

	    System.err.println("Unknown type in construct leaf node");
	}

	// Return the constructed result
	//
	return result;
    }

    
    /**
     * This function processes a internal node during the construction process
     *
     * @param tupleElement The tuple to construct the result from
     * @param interalNode The internal node having details of construction
     *
     * @return The list of results constructed
     */

    private ArrayList processInternalNode (
					 StreamTupleElement tupleElement,
					 constructInternalNode internalNode) {

	// Create a new element node with the required tag name
	// taking care of tagvariables

	data tagData = internalNode.getStartTag().getSdata();
	String tagName;

	if(tagData.getType() == dataType.ATTR) {
	   schemaAttribute sattr = (schemaAttribute)tagData.getValue();
	   int attrId = sattr.getAttrId();
	   tagName = ((Node)tupleElement.getAttribute(attrId)).getNodeName();
	}
	else
	   tagName = (String)tagData.getValue();

	Element resultElement = txd.createElement(tagName);

	// Recurse on all children and construct result
	//
	Vector children = internalNode.getChildren();

	int numChildren = children.size();

	for (int child = 0; child < numChildren; ++child) {

	    // Get constructed results from child
	    //
	    ArrayList childResult = 
		physConstructResult(tupleElement,
				(constructBaseNode) children.get(child));

	    // Add each constructed result to the result element
	    //
	    int numResults = childResult.size();

	    for (int res = 0; res < numResults; ++res) {
                Node n = ((Node) childResult.get(res)).cloneNode(true);
                DOMFactory.importNode(txd, n);
                resultElement.appendChild(n);
	    }
	}

	// Construct the result array list
	//
	ArrayList result = new ArrayList(1);

	result.add(resultElement);

	// Return the result
	//
	return result;
    }

}
