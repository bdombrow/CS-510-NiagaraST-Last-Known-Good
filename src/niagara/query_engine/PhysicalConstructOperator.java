/**********************************************************************
  $Id: PhysicalConstructOperator.java,v 1.22 2003/07/27 02:35:16 tufte Exp $


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

    /** Output variable */
    private Attribute variable;
    /** Are we projecting attributes away? */
    private boolean projecting;
    /** Maps shared attribute positions between incoming and outgoing tuples */
    private int[] attributeMap;

    // The result template to construct from
    private constructBaseNode resultTemplate;

    // We will use this Document as the "owner" of all the DOM nodes
    // we create
    private Document doc;

    // temporary result list storage place
    private NodeVector resultList;
    private int outSize; // save a fcn call each time through

    public PhysicalConstructOperator() {
        setBlockingSourceStreams(blockingSourceStreams);
        resultList = new NodeVector();
    }

    public void opInitFrom(LogicalOp logicalOperator) {
        // Typecast to a construct logical operator
        constructOp constructLogicalOp = (constructOp) logicalOperator;

        // Initialize the result template 
        this.resultTemplate = constructLogicalOp.getResTemp();
        this.variable = constructLogicalOp.getVariable();
    }

    public void opInitialize() {
        outSize = outputTupleSchema.getLength();
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

    protected void nonblockingProcessSourceTupleElement(
        StreamTupleElement tupleElement,
        int streamId)
        throws InterruptedException, ShutdownException {
        // Recurse down the result template to construct result
        resultList.quickReset(); // just for safety
        constructResult(tupleElement, resultTemplate, resultList, doc);

        // Add all the results in the result list as result tuples
        int numResults = resultList.size();
	assert numResults == 1 : "HELP numResults is " + numResults;
        for (int res = 0; res < numResults; ++res) {
            // Clone the input tuple 
            StreamTupleElement outputTuple;

            if (projecting) // We can project some attributes away
                outputTuple = tupleElement.copy(outSize, attributeMap);
            else // Just clone
                outputTuple = tupleElement.copy(outSize);
            
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

    public static void constructResult(
        StreamTupleElement tupleElement,
        constructBaseNode templateRoot,
        NodeVector localResult,
	Document localDoc) 
    throws ShutdownException {
        // Check if the template root is an internal node or a leaf node
        // and process accordingly
       
        if (templateRoot instanceof constructLeafNode) {
            processLeafNode(
                tupleElement,
                (constructLeafNode) templateRoot,
                localResult, localDoc);
        } else if (templateRoot instanceof constructInternalNode) {
            processInternalNode(
                tupleElement,
                (constructInternalNode) templateRoot,
                localResult, localDoc);
        } else {
            assert false: "Unknown construct node type!";
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

    private static void processLeafNode(StreamTupleElement tupleElement,
					constructLeafNode leafNode,
					NodeVector localResult,
					Document localDoc) {
	data leafData = leafNode.getData();

	switch(leafData.getType()) {

	case dataType.STRING:
            // Add the string value to the result
            localResult.add(localDoc.createTextNode((String) 
						    leafData.getValue()));
	    break;

	case dataType.ATTR:
		// First get the schema attribute
        schemaAttribute schema = (schemaAttribute) leafData.getValue();

        // Now construct result based on whether it is to be interpreted
        // as an element or a parent
		//		The value of the leafData is a schema attribute - from it
		// get the attribute id in the tuple to construct from
	    int attributeId = ((schemaAttribute) leafData.getValue()).getAttrId();
	    Node n = tupleElement.getAttribute(attributeId);
	    if(n == null) {
	    	return;
	    }
	    
	    switch(schema.getType()) {
	    case varType.ELEMENT_VAR:
        	localResult.add(n);
		break;
        
        case varType.CONTENT_VAR:
        	// The value of the leafData is a schema attribute - from it
            // get the attribute id in the tuple to construct from
            
                // Get the children of the attribute
                NodeList nodeList =
                    tupleElement.getAttribute(attributeId).getChildNodes();

                // Add all the children to the result
                int numChildren = nodeList.getLength();

                for (int child = 0; child < numChildren; ++child) {
                    localResult.add(nodeList.item(child));
                }
		break;
	    default:
		assert false: "Unknown schema attribute type in construct leaf node";
	    }
	    break;

	default:
	    assert false: "Unknown type in construct leaf node";
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

    private static void processInternalNode(
        StreamTupleElement tupleElement,
        constructInternalNode internalNode,
        NodeVector localResult,
	Document localDoc) 
    throws ShutdownException {

        // Create a new element node with the required tag name
        // taking care of tagvariables
        data tagData = internalNode.getStartTag().getSdata();
        String tagName;

        if (tagData.getType() == dataType.ATTR) {
            schemaAttribute sattr = (schemaAttribute) tagData.getValue();
            int attrId = sattr.getAttrId();
            // TODO HERE what to do if we get null attribute??
            tagName = tupleElement.getAttribute(attrId).getNodeName();
        } else
            tagName = (String) tagData.getValue();

        Element resultElement = localDoc.createElement(tagName);

	    // appends any appropriate attributes to the resultElement
	    addAttributes(tupleElement, internalNode, resultElement);

        // Recurse on all children and construct result
        Vector children = internalNode.getChildren();

        int numChildren = children.size();

        for (int child = 0; child < numChildren; ++child) {
            // Get constructed results from child
            int prevSize = localResult.size();
            constructResult(
                tupleElement,
                (constructBaseNode) children.get(child),
                localResult, localDoc);

            // Add each constructed result to the result element
            int numResults = localResult.size() - prevSize;

	    Node res;
            for (int i = 0; i < numResults; i++) {
		res = localResult.get(prevSize + i);
		Node n = DOMFactory.importNode(localDoc, res);
		resultElement.appendChild(n);
            }
            localResult.setSize(prevSize);
        }

        // Construct the result array list
        localResult.add(resultElement);
    }

    public static void addAttributes(StreamTupleElement tupleElement,
				     constructInternalNode internalNode,
				     Element resultElement) 
    throws ShutdownException {

        Vector attrs = internalNode.getStartTag().getAttrList();
	
        for (int i = 0; i < attrs.size(); i++) {
            attr attribute = (attr) attrs.get(i);
            String name = attribute.getName();
            data attrData = attribute.getValue();
	    int attributeId;

	    switch(attrData.getType()) {
	    case dataType.STRING:
                // Add the string value to the result
                resultElement.setAttribute(name, (String) attrData.getValue());
		break;

	    case dataType.ATTR:
                // First get the schema attribute
                schemaAttribute schema = (schemaAttribute) attrData.getValue();
		assert schema != null : "Schema null for attribute " + name;

                // Now construct result based on whether it is to be 
		// interpreted as an element or a parent
                switch(schema.getType()) {
		case varType.ELEMENT_VAR:
                    // The value of the leafData is a schema attribute
		    // - from it get the attribute id in the tuple 
		    // to construct from
		    attributeId =
                        ((schemaAttribute) attrData.getValue()).getAttrId();

                    // Add the attribute as the result
                    // This better BE an attribute!
                 
		    Node na = tupleElement.getAttribute(attributeId);
		    if(na == null)
		    	break;
		    if(!(na instanceof Attr)) {
			throw new ShutdownException("Can not use element type variable to create attribute");
		    }
                    Attr a = (Attr) na;
                    resultElement.setAttribute(name, a.getValue());
		    break;

		case varType.CONTENT_VAR:
		    attributeId =
                        ((schemaAttribute) attrData.getValue()).getAttrId();
			
		    Node attr = tupleElement.getAttribute(attributeId);
		    if(attr == null)
		    	break;
		    if(attr instanceof Element) {
			Element elt = (Element)attr;
			
			// Concatenate the node values of 
			// the element's children
			StringBuffer attrValue = new StringBuffer("");
			Node n = elt.getFirstChild();
			while (n != null) {
			    attrValue.append(n.getNodeValue());
			    n = n.getNextSibling();
			}
			resultElement.setAttribute(name, attrValue.toString());
		    } else if (attr instanceof Attr) {
			// KT used to require that this be an element,
			// but I think attribute is valid also
			resultElement.setAttribute(name, 
						   ((Attr)attr).getValue());
		    } else {
			assert false : "KT: what did I get here??";
		    }
		    break;

		default:
		    assert false: "Unknown var type in attribute constructor";
		}
		break;

	    default:
		assert false: "Unknown data type";
            }
        }
    }

    public void setResultDocument(Document doc) {
        this.doc = doc;
    }

    public boolean isStateful() {
        return false;
    }

    public int hashCode() {
        return resultTemplate.hashCode()
            ^ variable.hashCode()
            ^ hashCodeNullsAllowed(getLogProp());
    }

    public boolean equals(Object o) {
        if (o == null || !(o instanceof PhysicalConstructOperator))
            return false;
        if (o.getClass() != getClass())
            return o.equals(this);
        PhysicalConstructOperator other = (PhysicalConstructOperator) o;
        // XXX vpapad TODO constructBaseNode doesn't override equals
        return resultTemplate.equals(other.resultTemplate)
            && variable.equals(other.variable)
            && equalsNullsAllowed(getLogProp(), other.getLogProp());
    }

    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op opCopy() {
        PhysicalConstructOperator op = new PhysicalConstructOperator();
        // XXX vpapad: We treat resultTemplate as an immutable object
        op.resultTemplate = resultTemplate;
        op.variable = variable;
	op.outSize = outSize;
        return op;
    }

    /**
     * @see niagara.optimizer.colombia.PhysicalOp#FindLocalCost(ICatalog, LogicalProperty, LogicalProperty[])
     */
    public Cost findLocalCost(
        ICatalog catalog,
        LogicalProperty[] inputLogProp) {
        // XXX vpapad: Absolutely no connection to reality!
        // We consider only a fixed cost per output tuple
        return new Cost(
            constructTupleCost(catalog) * getLogProp().getCardinality());
    }

    public void constructTupleSchema(TupleSchema[] inputSchemas) {
        super.constructTupleSchema(inputSchemas);
        resultTemplate.replaceVar(new varTbl(inputSchemas[0]));        
        // Without projection, (length of output tuple) = (length of input tuple + 1)
        projecting = (inputSchemas[0].getLength() + 1 != outputTupleSchema.getLength());
        if (projecting)
            attributeMap = inputSchemas[0].mapPositions(outputTupleSchema);
    }
}
