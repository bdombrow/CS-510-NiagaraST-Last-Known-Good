/**********************************************************************
  $Id: PhysicalMagicConstruct.java,v 1.1 2003/12/24 01:49:02 vpapad Exp $


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

import java.util.Vector;

import org.w3c.dom.*;
import niagara.utils.*; // NodeVector, StreamTupleElement, etc
import niagara.magic.*;
import niagara.xmlql_parser.*;
import niagara.optimizer.colombia.*;
import niagara.query_engine.*;
import niagara.logical.MagicConstruct;

/**
 * <code>PhysicalMagicConstruct</code> is an implementation of 
 * the Construct operator.
 */
public class PhysicalMagicConstruct extends PhysicalOperator {
    // No blocking inputs
    private static final boolean[] blockingSourceStreams = { false };

    /** Are we projecting attributes away? */
    private boolean projecting;

    /** Maps shared attribute positions between incoming 
	and outgoing tuples */
    private int[] attributeMap;

    private constructBaseNode resultTemplate;
    private Node magicResult;

    // We will use this Document as the "owner" of all the DOM nodes
    // we create
    private Document doc;

    // temporary result list storage place
    private NodeVector resultList;
    private int outSize; // save a fcn call each time through
    private MagicBaseNode magicRoot;

    public PhysicalMagicConstruct() {
        setBlockingSourceStreams(blockingSourceStreams);
        resultList = new NodeVector();
    }

    public void opInitFrom(LogicalOp logicalOperator) {
        // Typecast to a construct logical operator
        MagicConstruct constructLogicalOp = (MagicConstruct) logicalOperator;

        // Initialize the result template 
        this.resultTemplate = constructLogicalOp.getResTemp();
    }

    public void opInitialize() throws ShutdownException {
        outSize = outputTupleSchema.getLength();
        buildTemplate();
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

    protected void processTuple(
        Tuple tupleElement,
        int streamId)
        throws InterruptedException, ShutdownException {


	// add the template to the input tuple and send it 
	// on its way
	
	Tuple outputTuple;
	if (projecting) // We can project some attributes away
	    outputTuple = tupleElement.copy(outSize, attributeMap);
	else // Just clone
	    outputTuple = tupleElement.copy(outSize);
	
	// Append the template to end of newly created tuple
	outputTuple.appendAttribute(magicResult);
	
	// Add the new tuple to the result
	putTuple(outputTuple, 0);

        resultList.clear();
    }

    // takes the resultTemplate (created from parsing the construct
    // xml specification in the query file) and converts it into
    // an xml tree which is the result minus any variables 
    // variables will be filled in from incoming tuples
    private void buildTemplate() throws ShutdownException {
        resultList.quickReset(); // just for safety

	buildTempl(resultList, resultTemplate, true);
	assert resultList.size() == 1 : "HELP got " +
	    resultList.size() + " results - expected only 1";

	magicResult = resultList.get(0);
    }

    private void buildTempl(NodeVector localResult,
			    constructBaseNode constructNode,
			    boolean isRoot) throws ShutdownException {
        if (constructNode instanceof constructLeafNode) {
            buildLeafNodeTempl((constructLeafNode) constructNode,
			       localResult, isRoot);
        } else if (constructNode instanceof constructInternalNode) {
            buildInternalNodeTempl((constructInternalNode) constructNode,
				   localResult, isRoot);
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

    private void buildLeafNodeTempl(constructLeafNode leafNode,
				    NodeVector localResult,
				    boolean isRoot) {
	data leafData = leafNode.getData();

	switch(leafData.getType()) {

	case dataType.STRING:
	    // Leaf node consists of a string value
	    assert !isRoot : "Root of construct can't be a string";
	    localResult.add(doc.createTextNode((String) 
					       leafData.getValue()));
	    break;

	case dataType.ATTR:
	    // Leaf node is to be taken from some attribute in the tuple
	    // either we take that attribute as is - or we take
	    // that attribute's content
            schemaAttribute schema = (schemaAttribute) leafData.getValue();

	    // figure out which attribute we should use
	    int attrIdx =
                    ((schemaAttribute) leafData.getValue()).getAttrId();

	    switch(schema.getType()) {
	    case varType.ELEMENT_VAR:
		// KT - major hack - this could be an element or text
		// for now we assume element - fix once we make sure
		// this actually saves time,
		MagicNode magicNode = new MagicNode(attrIdx, magicRoot,
						    Node.ELEMENT_NODE);
		if(isRoot) {
		    magicRoot = magicNode;
		    magicNode.youAreTheRoot(doc);
		}

		// put a magic node pointing to this
		// attribute in the result
                localResult.add(magicNode);
		break;

            case varType.CONTENT_VAR:
		// do not know tag name here...
		MagicContentElement magicContent =
		    new MagicContentElement(attrIdx, magicRoot, null);
		localResult.add(magicContent);
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

    private void buildInternalNodeTempl(
        constructInternalNode internalNode,
        NodeVector localResult,
	boolean isRoot)
    throws ShutdownException {

        // Create a new element node with the required tag name
        data tagData = internalNode.getStartTag().getSdata();
        String tagName;

        if (tagData.getType() == dataType.ATTR) {
	    assert false : "Magic tag names are not allowed";
	    tagName = null; // make compiler happy
        } else
            tagName = (String) tagData.getValue();

	MagicBaseNode resultElement = 
	    new MagicShellElement(tagName, magicRoot);
	if(isRoot) {
	    magicRoot = resultElement;
	    resultElement.youAreTheRoot(doc);
	}

        // Recurse on all children and construct result
        Vector children = internalNode.getChildren();
        int numChildren = children.size();
	MagicBaseNode prevChild = null;
        for (int i = 0; i < numChildren; i++) {
            // Get constructed results from child
            int prevSize = localResult.size();
            buildTempl(localResult, 
		       (constructBaseNode) children.get(i),
		       false);

            // Add each constructed result to the result element
            int numResults = localResult.size() - prevSize;

	    MagicBaseNode child;
            for (int j = 0; j < numResults; j++) {
		child = (MagicBaseNode)localResult.get(prevSize + j);
		if(child instanceof MagicContentElement) {
		    assert numResults == 1 : "Should have only 1 result. Had "
			+ numResults;
		    ((MagicContentElement)child).setTagName(tagName);
		    resultElement = child;
		    assert !isRoot;
		} else {
		    resultElement.appendChildDNS(child);
		}
		if(prevChild != null)
		    prevChild.setNextSibling(child);
		prevChild = child;
            }
            localResult.setSize(prevSize);
        }

	// appends any appropriate attributes to the resultElement
	// must happen after recursion on children due
	// to magic element stuff...
	addAttributes(internalNode, (Element)resultElement);

        // Construct the result array list
        localResult.add(resultElement);
    }

    public void addAttributes(constructInternalNode internalNode,
			      Element resultElement) 
    throws ShutdownException {

        Vector attrs = internalNode.getStartTag().getAttrList();
	
        for (int i = 0; i < attrs.size(); i++) {
            attr attribute = (attr) attrs.get(i);
            String attrName = attribute.getName();
            data attrData = attribute.getValue();

	    switch(attrData.getType()) {
	    case dataType.STRING:
                // Add the string value to the result
		Attr a = doc.createAttribute(attrName);
		a.setValue((String)attrData.getValue());
                resultElement.setAttributeNode(a);
		break;

	    case dataType.ATTR:
                // First get the schema attribute
                schemaAttribute schema = (schemaAttribute) attrData.getValue();
		assert schema != null : "Schema null for attribute " + attrName;

		int attrIdx =
                        ((schemaAttribute) attrData.getValue()).getAttrId();

                // Now construct result based on whether it is to be 
		// interpreted as an element or a parent
                switch(schema.getType()) {
		case varType.ELEMENT_VAR:
		    MagicNode magicAttr = new MagicNode(attrIdx, magicRoot,
							Node.ATTRIBUTE_NODE);
                    resultElement.setAttributeNode(magicAttr);
		    break;

		case varType.CONTENT_VAR:
		    MagicContentAttr magicContentAttr =
			new MagicContentAttr(attrIdx, magicRoot,
					     attrName);
                    resultElement.setAttributeNode(magicContentAttr);
		    // magiccontent node has no attributes in this case
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
            //^ variable.hashCode()
            ^ hashCodeNullsAllowed(getLogProp());
    }

    public boolean equals(Object o) {
        if (o == null || !(o instanceof PhysicalMagicConstruct))
            return false;
        if (o.getClass() != getClass())
            return o.equals(this);
        PhysicalMagicConstruct other = (PhysicalMagicConstruct) o;
        // XXX vpapad TODO constructBaseNode doesn't override equals
        return resultTemplate.equals(other.resultTemplate)
            //&& variable.equals(other.variable)
            && equalsNullsAllowed(getLogProp(), other.getLogProp());
    }

    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op opCopy() {
        PhysicalMagicConstruct op = new PhysicalMagicConstruct();
        // XXX vpapad: We treat resultTemplate as an immutable object
        op.resultTemplate = resultTemplate;
        //op.variable = variable;
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
        // Without projection, (length of output tuple) 
	//                        = (length of input tuple + 1)
        projecting = (inputSchemas[0].getLength() + 1 
		      != outputTupleSchema.getLength());
        if (projecting)
            attributeMap = inputSchemas[0].mapPositions(outputTupleSchema);
    }
}
