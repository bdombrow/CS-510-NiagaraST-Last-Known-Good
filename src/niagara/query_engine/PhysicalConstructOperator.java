
/**********************************************************************
  $Id: PhysicalConstructOperator.java,v 1.12 2002/04/29 19:51:23 tufte Exp $


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
import java.util.Vector;

import org.w3c.dom.*;

import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;
import niagara.ndom.*;

/**
 * This is the <code>PhysicalConstructOperator</code> that extends
 * the basic PhysicalOperator with the implementation of the Construct
 * operator.
 *
 * @version 1.0
 *
 */

public class PhysicalConstructOperator extends PhysicalOperator {

    /////////////////////////////////////////////////////////////////////////
    // These are the private data members of the PhysicalConstructOperator //
    // class                                                               //
    /////////////////////////////////////////////////////////////////////////

    // This is the array having information about blocking and non-blocking
    // streams
    //
    private static final boolean[] blockingSourceStreams = { false };

    // The result template to construct from
    //
    private constructBaseNode resultTemplate;

    // "Clear the output tuple" flag
    boolean clear;

    // We will use this Document as the "owner" of all the DOM nodes
    // we create
    private Document doc;

    // temporary result list storage place
    private NodeVector resultList;
    
    ///////////////////////////////////////////////////////////////////////////
    // These are the methods of the PhysicalConstructOperator class          //
    ///////////////////////////////////////////////////////////////////////////

    /**
     * This is the constructor for the PhysicalConstructOperator class that
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

    public PhysicalConstructOperator(op logicalOperator,
				     SourceTupleStream[] sourceStreams,
				     SinkTupleStream[] sinkStreams,
				     Integer responsiveness) {
	// Call the constructor of the super class
	super(sourceStreams,
	      sinkStreams,
	      blockingSourceStreams,
	      responsiveness);

	// Typecast to a construct logical operator
	//
	constructOp constructLogicalOp = (constructOp) logicalOperator;

	// Initialize the result template and the "clear" flag
	this.resultTemplate = constructLogicalOp.getResTemp();
	this.clear = constructLogicalOp.isClear();
	resultList = new NodeVector();
    }
    

    /**
     * This function processes a tuple element read from a source stream
     * when the operator is in a non-blocking state. This over-rides the
     * corresponding function in the base class.
     *
     * @param tupleElement The tuple element read from a source stream
     * @param streamId The source stream from which the tuple was read
     *
     * @exception ShutdownException query shutdown by user or execution error
     */

    protected void nonblockingProcessSourceTupleElement (
					     StreamTupleElement tupleElement,
						 int streamId) 
    throws InterruptedException, ShutdownException {
	// Recurse down the result template to construct result
	resultList.quickReset(); // just for safety
	constructResult(tupleElement, resultTemplate, resultList);

	// Add all the results in the result list as result tuples
	//
	int numResults = resultList.size();

	for (int res = 0; res < numResults; ++res) {

	    // Clone the input tuple element to create a new output tuple
	    //element
	    //
	    StreamTupleElement outputTuple;

	    if (clear) {
		// Start a completely new tuple
		outputTuple = new StreamTupleElement(tupleElement.isPartial());
	    } else {
		outputTuple = (StreamTupleElement) tupleElement.clone();
	    }

	    // Append the constructed result to end of newly created tuple
	    outputTuple.appendAttribute(resultList.get(res));

	    // Add the new tuple to the result
            putTuple(outputTuple, 0);
	}

	resultList.clear();
    }


    /**
     * This function constructs results given a tuple and a result template
     *
     * @param tupleElement the tuple to construct results from
     * @param templateRoot the root of the result template
     *
     * @return a list of nodes constructed as per the template
     */

    public void constructResult (StreamTupleElement tupleElement,
				 constructBaseNode templateRoot,
				 NodeVector result) {

	// Check if the template root is an internal node or a leaf node
	// and process accordingly
	//
	if (templateRoot instanceof constructLeafNode) {
	    processLeafNode(tupleElement,
			    (constructLeafNode) templateRoot,
			    result);
	} else if (templateRoot instanceof constructInternalNode) {
	    processInternalNode(tupleElement,
				(constructInternalNode) templateRoot,
				result);
	} else {
	    throw new PEException("Error: Unknown construct node type!");
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

    private void processLeafNode (StreamTupleElement tupleElement,
				  constructLeafNode leafNode,
				  NodeVector result) {
	// Get the data of the leaf node
	data leafData = leafNode.getData();

	// Check the type of the data
	int type = leafData.getType();

	if (type == dataType.STRING) {
	    // Add the string value to the result
	    result.add(doc.createTextNode((String) leafData.getValue()));
	}
	else if (type == dataType.ATTR) {
	    // First get the schema attribute
	    schemaAttribute schema = (schemaAttribute) leafData.getValue();

	    // Now construct result based on whether it is to be interpreted
	    // as an element or a parent
	    if (schema.getType() == varType.ELEMENT_VAR) {

		// The value of the leafData is a schema attribute - from it
		// get the attribute id in the tuple to construct from
		int attributeId = ((schemaAttribute) leafData.getValue()).getAttrId();

		// Add the attribute as the result
		result.add(tupleElement.getAttribute(attributeId));
	    }
	    else if (schema.getType() == varType.CONTENT_VAR) {

		// The value of the leafData is a schema attribute - from it
		// get the attribute id in the tuple to construct from
		int attributeId = ((schemaAttribute) leafData.getValue()).getAttrId();

		// Get the children of the attribute
		NodeList nodeList =
		    tupleElement.getAttribute(attributeId).getChildNodes();

		// Add all the children to the result
		int numChildren = nodeList.getLength();

		for (int child = 0; child < numChildren; ++child) {
		    result.add(nodeList.item(child));
		}
	    }
	    else {
		throw new PEException("Unknown schema attribute type in construct leaf node");
	    }
	} else {
	    throw new PEException("Unknown type in construct leaf node");
	}
	return;
    }

    
    /**
     * This function processes a internal node during the construction process
     *
     * @param tupleElement The tuple to construct the result from
     * @param interalNode The internal node having details of construction
     *
     * @return The list of results constructed
     */

    private void processInternalNode (StreamTupleElement tupleElement,
				      constructInternalNode internalNode,
				      NodeVector result) {

	// Create a new element node with the required tag name
	// taking care of tagvariables
	data tagData = internalNode.getStartTag().getSdata();
	String tagName;

	if(tagData.getType() == dataType.ATTR) {
	   schemaAttribute sattr = (schemaAttribute)tagData.getValue();
	   int attrId = sattr.getAttrId();
	   tagName = tupleElement.getAttribute(attrId).getNodeName();
	}
	else
	   tagName = (String)tagData.getValue();

	Element resultElement = doc.createElement(tagName);

	Vector attrs = internalNode.getStartTag().getAttrList();

	for (int i = 0; i < attrs.size(); i++) {
	    attr attribute = (attr) attrs.get(i);
	    String name = attribute.getName();
	    data attrData = attribute.getValue();
	    int type = attrData.getType();
	    if (type == dataType.STRING) {
		// Add the string value to the result
		resultElement.setAttribute(name, (String) attrData.getValue());
	    } else if (type == dataType.ATTR) {
		// First get the schema attribute
		schemaAttribute schema = (schemaAttribute) attrData.getValue();
		
		// Now construct result based on whether it is to be interpreted
		// as an element or a parent
		if (schema.getType() == varType.ELEMENT_VAR) {
		    // The value of the leafData is a schema attribute - from it
		    // get the attribute id in the tuple to construct from
		    int attributeId = ((schemaAttribute) attrData.getValue()).getAttrId();
		    
		    // Add the attribute as the result
		    // This better BE an attribute!
		    Attr a = (Attr) tupleElement.getAttribute(attributeId);
		    System.err.println("XXX adding attribute: " + name + "=" + a.getValue());
		    resultElement.setAttribute(name, a.getValue());
		}
		else if (schema.getType() == varType.CONTENT_VAR) {
		    int attributeId = ((schemaAttribute) attrData.getValue()).getAttrId();
		    
		    // This better BE an element
		    Element elt = (Element) tupleElement.getAttribute(attributeId);
		    
		    // Concatenate the node values of the element's children
		    StringBuffer attrValue = new StringBuffer("");
		    Node n = elt.getFirstChild();
		    while (n != null) 
			attrValue.append(n.getNodeValue());
		    resultElement.setAttribute(name, attrValue.toString());
		}
		else 
		    throw new PEException("Unknown type in attribute constructor");
	    }
	}

	// Recurse on all children and construct result
	//
	Vector children = internalNode.getChildren();

	int numChildren = children.size();

	for (int child = 0; child < numChildren; ++child) {

	    // Get constructed results from child
	    int prevSize = resultList.size();
	    constructResult(tupleElement,
			    (constructBaseNode) children.get(child),
			    resultList);

	    // Add each constructed result to the result element
	    int numResults = resultList.size() - prevSize;

	    for (int res = 0; res < numResults; ++res) {
                Node n;
                n = DOMFactory.importNode(doc, resultList.get(prevSize+res));
                resultElement.appendChild(n);
	    }
	    resultList.setSize(prevSize);
	}

	// Construct the result array list
	//
	result.add(resultElement);
	return; 
    }

    public void setResultDocument(Document doc) {
        this.doc = doc;
    }

    public boolean isStateful() {
	return false;
    }

}

