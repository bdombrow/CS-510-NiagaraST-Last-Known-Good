/**********************************************************************
  $Id: PhysicalConstructOperator.java,v 1.14 2002/10/31 03:54:38 vpapad Exp $


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
import niagara.optimizer.colombia.*;

/**
 * <code>PhysicalConstructOperator</code> is an implementation of 
 * the Construct operator.
 */
public class PhysicalConstructOperator extends PhysicalOperator {
    // No blocking inputs
    private static final boolean[] blockingSourceStreams = { false };

    private Attribute variable;
    
    // The result template to construct from
    private constructBaseNode resultTemplate;

    // We will use this Document as the "owner" of all the DOM nodes
    // we create
    private Document doc;

    // temporary result list storage place
    private NodeVector resultList;
    

    public PhysicalConstructOperator() {
        setBlockingSourceStreams(blockingSourceStreams);
        resultList = new NodeVector();
    }
    
    public void initFrom(LogicalOp logicalOperator) {
	// Typecast to a construct logical operator
	constructOp constructLogicalOp = (constructOp) logicalOperator;

	// Initialize the result template 
	this.resultTemplate = constructLogicalOp.getResTemp();
    
        this.variable = constructLogicalOp.getVariable();
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

		outputTuple = (StreamTupleElement) tupleElement.clone();

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
	result.add(resultElement);
    }

    public void setResultDocument(Document doc) {
        this.doc = doc;
    }

    public boolean isStateful() {
	return false;
    }
    
    public int hashCode() {
        return resultTemplate.hashCode() ^ variable.hashCode();
    }
    
    public boolean equals(Object o) {
        if (o == null || !(o instanceof PhysicalConstructOperator))
            return false;
        if (o.getClass() != getClass())
            return o.equals(this);
        PhysicalConstructOperator other = (PhysicalConstructOperator) o;
        // XXX vpapad TODO constructBaseNode doesn't override equals
        return resultTemplate.equals(other.resultTemplate)
                        && variable.equals(other.variable);
    }

    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op copy() {
        PhysicalConstructOperator op = new PhysicalConstructOperator();
        // XXX vpapad: We treat resultTemplate as an immutable object
        op.resultTemplate = resultTemplate;
        op.variable = variable;
        return op;
    }
    
    /**
     * @see niagara.optimizer.colombia.PhysicalOp#FindLocalCost(ICatalog, LogicalProperty, LogicalProperty[])
     */
    public Cost findLocalCost(
        ICatalog catalog,
        LogicalProperty[] InputLogProp) {
        // XXX vpapad: Absolutely no connection to reality!
        // We consider only a fixed cost per incoming tuple
        // XXX vpapad: it should be: touch input tuples + construction cost
        // + output tuples (?)
        return new Cost(
            catalog.getDouble("tuple_construction_cost")
                * InputLogProp[0].getCardinality());
    }

    /**
     * @see niagara.query_engine.SchemaProducer#constructTupleSchema(TupleSchema[])
     */
    public void constructTupleSchema(TupleSchema[] inputSchemas) {
        this.inputTupleSchemas = inputSchemas;
        resultTemplate.replaceVar(new varTbl(inputSchemas[0]));
        outputTupleSchema = inputSchemas[0].copy();
        outputTupleSchema.addMapping(variable);
    }
}

