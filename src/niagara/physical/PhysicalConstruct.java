/**********************************************************************
  $Id: PhysicalConstruct.java,v 1.5 2007/05/15 22:15:03 jinli Exp $


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

import niagara.utils.*;
import niagara.xmlql_parser.*;
import niagara.logical.*;
import niagara.ndom.*;
import niagara.optimizer.colombia.*;
import niagara.query_engine.*;

/**
 * <code>PhysicalConstruct</code> is an implementation of 
 * the Construct operator.
 */
public class PhysicalConstruct extends PhysicalOperator {
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
    
    private long epoch = Long.MAX_VALUE;
    private Tuple dumpTuple;
    
    private Attribute epochAttr;
    private int epochAttrIndex = -1;

    public PhysicalConstruct() {
        setBlockingSourceStreams(blockingSourceStreams);
        resultList = new NodeVector();
    }

    public void opInitFrom(LogicalOp logicalOperator) {
        // Typecast to a construct logical operator
        Construct constructLogicalOp = (Construct) logicalOperator;

        // Initialize the result template 
        this.resultTemplate = constructLogicalOp.getResTemp();
        this.variable = constructLogicalOp.getVariable();
        this.epochAttr = constructLogicalOp.getEpochAttr();
    }

    public void opInitialize() {
        outSize = outputTupleSchema.getLength();
        if (epochAttr != null)
        	epochAttrIndex = inputTupleSchemas[0].getPosition(epochAttr.getName());
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

    protected void processTuple(Tuple tuple, int streamId)
        throws InterruptedException, ShutdownException {

        //dumpTuple is a speical tuple used as epoch seperator;
    	if (epochAttr != null) {
	    	if (dumpTuple == null) {
	        	epoch = 0;
	        	Tuple t = (Tuple)tuple.clone();
	        	t.setAttribute(epochAttrIndex, new StringAttr("::::"));
	
	            resultList.quickReset(); // just for safety
	            constructResult(t, resultTemplate, resultList, doc);
	
	            // Add all the results in the result list as result tuples
	            int numResults = resultList.size();
	            assert numResults == 1 : "HELP numResults is " + numResults;
	
	            if (projecting) // We can project some attributes away
	                dumpTuple = t.copy(outSize, attributeMap);
	            else // Just clone
	                dumpTuple = t.copy(outSize);
	                
	            // Append the constructed result to end of newly created tuple
	            Node result = resultList.get(0);
	            dumpTuple.appendAttribute(result);
	        }
	
	    	long epochVal = Long.valueOf(((BaseAttr)tuple.getAttribute(epochAttrIndex)).toASCII());
	    	if (epochVal > epoch) {
	        //if (Long.valueOf(list.item(epochAttrIndex).getFirstChild().getNodeValue()).compareTo(epoch) > 0) {
	        	// use null tuple as seperator;
	        	putTuple(dumpTuple, 0);
	        	epoch = epochVal;
	        }
    	}
    	
    	// Recurse down the result template to construct result
        resultList.quickReset(); // just for safety
        constructResult(tuple, resultTemplate, resultList, doc);

        
        // Add all the results in the result list as result tuples
        int numResults = resultList.size();
        assert numResults == 1 : "HELP numResults is " + numResults;
        for (int res = 0; res < numResults; ++res) {
            // Clone the input tuple 
            Tuple outputTuple;

            if (projecting) // We can project some attributes away
                outputTuple = tuple.copy(outSize, attributeMap);
            else // Just clone
                outputTuple = tuple.copy(outSize);
            
            // Append the constructed result to end of newly created tuple
            Node result = resultList.get(res);
            outputTuple.appendAttribute(result);
            
            // Add the new tuple to the result
            putTuple(outputTuple, 0);
        }

        resultList.clear();
    }
    
	protected void processPunctuation(
			  Punctuation inputTuple,
			  int streamId)
	throws ShutdownException, InterruptedException {
    	processTuple(inputTuple, streamId);
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
        Tuple tuple,
        constructBaseNode templateRoot,
        NodeVector localResult,
	Document localDoc) 
    throws ShutdownException {
        // Check if the template root is an internal node or a leaf node
        // and process accordingly
       
        if (templateRoot instanceof constructLeafNode) {
            processLeafNode(
                tuple,
                (constructLeafNode) templateRoot,
                localResult, localDoc);
        } else if (templateRoot instanceof constructInternalNode) {
            processInternalNode(
                tuple,
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

    private static void processLeafNode(Tuple tupleElement,
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
	    int attributeId = schema.getAttrId();
	    //Node n = tupleElement.getAttribute(attributeId);
	    BaseAttr n = (BaseAttr)tupleElement.getAttribute(attributeId);
	    if (n == null) {
	    	return;
	    }
	    

	    switch(schema.getType()) {
	    case varType.ELEMENT_VAR:
		Element elt = localDoc.createElement(schema.getName());
		if (n instanceof XMLAttr) {
		    Node xnode = localDoc.importNode(((XMLAttr) n).getNodeValue(), true);
		    elt.appendChild(xnode);
		} else {
		    elt.appendChild(localDoc.createTextNode(n.toASCII()));
		}
		localResult.add(elt);
		break;
        
        case varType.CONTENT_VAR:
        	// The value of the leafData is a schema attribute - from it
            // get the attribute id in the tuple to construct from
            if (n instanceof XMLAttr) { 
            	// Get the children of the attribute
            	NodeList nodeList =
            		((XMLAttr)n).getNodeValue().getChildNodes();

            	// Add all the children to the result
            	int numChildren = nodeList.getLength();
            	for (int child = 0; child < numChildren; ++child) {
            		localResult.add(nodeList.item(child));
            	}
            } else {
            	localResult.add(localDoc.createTextNode(n.toASCII()));
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
        Tuple tupleElement,
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
            //tagName = tupleElement.getAttribute(attrId).getNodeName();
            tagName = sattr.getName();
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
        // If the node is already in the document and connected
        // to the tree, we must clone it first
        if (n == res && n.getParentNode() != null) 
            n = res.cloneNode(true);
        
		resultElement.appendChild(n);
            }
            localResult.setSize(prevSize);
        }

        // Construct the result array list
        localResult.add(resultElement);
    }

    public static void addAttributes(Tuple tupleElement,
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
	
			    BaseAttr ba = (BaseAttr) tupleElement.getAttribute(attributeId);
			    if (ba != null)
				resultElement.setAttribute(name, ba.toASCII());
			    break;
	
			case varType.CONTENT_VAR:
			    attributeId =
	                        ((schemaAttribute) attrData.getValue()).getAttrId();

			    assert tupleElement.getAttribute(attributeId) instanceof XMLAttr:
			    	"??? - Jenny";
				
			    //Node attr = tupleElement.getAttribute(attributeId);
			    Node attr = ((XMLAttr)tupleElement.getAttribute(attributeId)).getNodeValue();
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
        if (o == null || !(o instanceof PhysicalConstruct))
            return false;
        if (o.getClass() != getClass())
            return o.equals(this);
        PhysicalConstruct other = (PhysicalConstruct) o;
        // XXX vpapad TODO constructBaseNode doesn't override equals
        return resultTemplate.equals(other.resultTemplate)
            && variable.equals(other.variable)
            && equalsNullsAllowed(getLogProp(), other.getLogProp())
            && equalsNullsAllowed(epochAttr, other.epochAttr);
    }

    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op opCopy() {
        PhysicalConstruct op = new PhysicalConstruct();
        // XXX vpapad: We treat resultTemplate as an immutable object
        op.resultTemplate = resultTemplate;
        op.variable = variable;
        op.outSize = outSize;
        op.epochAttr = epochAttr;
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
