/**********************************************************************
  $Id: PhysicalPunctuateOperator.java,v 1.2 2003/07/03 19:56:52 tufte Exp $


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

import niagara.optimizer.colombia.*;
import niagara.utils.*;
import niagara.xmlql_parser.syntax_tree.*;
import niagara.xmlql_parser.op_tree.punctuateOp;
import niagara.logical.Predicate;
import niagara.logical.Comparison;
import niagara.logical.Variable;
import niagara.logical.NodeDomain;
import niagara.logical.NumericConstant;

/**
 * <code>PhysicalPunctuateOperator</code> implements a punctuate operator for
 * an incoming stream;
 *
 * @see PhysicalOperator
 */
public class PhysicalPunctuateOperator extends PhysicalOperator {
    private final int cInput = 2;
    //Which input contains data?
    private int iDataInput = 1;

    //Which timer attribute contains the timer value
    private Attribute attrTimer;

    //Which data attribute correponds to the timer
    private String stDataTimer;

    //What is the data root name
    private String stDataRoot;
    private Attribute attrDataRoot;
    private int iAttrDataRoot;

    //Store for input values
    private AtomicEvaluator aeTimer, aeData;
    private ArrayList vTimer, vData;

    //Data template for creating punctuation
    private String rgstDataChild[] = null;
    private short rgiDataType[];
    private Document doc;
    private double dblLastTimer = -1;

    //Predicate for enforcing the punctuation
    private Predicate pred = null;
    private PredicateImpl predEval = null;
    
    public PhysicalPunctuateOperator() {
        setBlockingSourceStreams(new boolean[cInput]);
    }

    public PhysicalPunctuateOperator(int iDI, Attribute aTimer,
				     String stData, Attribute aRoot) {
        setBlockingSourceStreams(new boolean[cInput]);

	iDataInput = iDI;
	attrTimer = aTimer;
	stDataTimer = stData;
	attrDataRoot = aRoot;
    }
    
    public void opInitFrom(LogicalOp logicalOperator) {
        setBlockingSourceStreams(new boolean[cInput]);

	punctuateOp pop = (punctuateOp) logicalOperator;

	iDataInput = pop.getDataInput();
	attrTimer = pop.getTimerAttr();
	stDataTimer = pop.getDataTimer();
	attrDataRoot = pop.getDataRoot();
    }

    public void opInitialize() {
        // XXX vpapad: really ugly...
        setBlockingSourceStreams(new boolean[cInput]);

	aeTimer = new AtomicEvaluator(attrTimer.getName());
        aeTimer.resolveVariables(inputTupleSchemas[1-iDataInput],1-iDataInput);
	vTimer = new ArrayList();

	iAttrDataRoot =
	    inputTupleSchemas[iDataInput].getPosition(attrDataRoot.getName());
    }

    /**
     * This function processes a tuple element read from a source stream
     * when the operator is non-blocking. This over-rides the corresponding
     * function in the base class.
     *
     * @param inputTuple The tuple element read from a source stream
     * @param streamId The source stream from which the tuple was read
     *
     * @exception ShutdownException query shutdown by user or execution error
     */
    protected void nonblockingProcessSourceTupleElement (
						 StreamTupleElement inputTuple,
						 int streamId)
	throws ShutdownException, InterruptedException {
	boolean fOutput = true;

	if (streamId == iDataInput) {
	    //Make sure this tuple doesn't match previously output
	    // punctuations
	    if (dblLastTimer != -1) 
		fOutput=enforcePunctuation(inputTuple);

	    if (fOutput) {
	        // set the tuple's timestamp to the current time
	        // NOTE: might be wise to only call currentTimeMillis
	        // occassionally, and let multiple tuples have the
	        // same timestamp. Its not as accurate, but it will
	        // speed this up.

	        inputTuple.setTimeStamp(System.currentTimeMillis());
	        putTuple(inputTuple, 0);
	    }

	    //If we haven't already picked up a template tuple,
	    // copy this one.
	    if (rgstDataChild == null)
		setupDataTemplate(inputTuple);
	} else {
	    if (rgstDataChild != null) {
		vTimer.clear();
		aeTimer.getAtomicValues(inputTuple, vTimer);
		if (vTimer.size() != 1)
		    throw new PEException("Punctuate requires exactly one value");

		dblLastTimer = Double.parseDouble((String) vTimer.get(0));

		//Put the punctuation in the output
		putTuple(createPunctuation(inputTuple, vTimer), 0);
	    }
	}
    }

    private boolean enforcePunctuation(StreamTupleElement inputTuple) {
	boolean fOutput = false;
	String stEle;

	//ptucker: This approach is simple. It would be more robust to
	// create a Predicate object to handle this instead. I
	// use this approach because I didn't want the overhead of
	// creating a new Predicate object (and the associated
	// NumericConstant) every time a punctuation arrived
        Node ndRoot = inputTuple.getAttribute(iAttrDataRoot);
        NodeList nl = ndRoot.getChildNodes();
	//This is a little ugly. Since each XML node is not consistent
	// (that is, one time a tuple may have four child elements,
	// the next time 5, and the next 3), I need to walk through
	// the node's children and find the element with the required
	// timestamp field.
        for (int i=0; i<nl.getLength(); i++) {
            stEle = nl.item(i).getNodeName();
	    if (stEle.compareTo(stDataTimer) == 0) {
	        //Need to get the descendant text field, and its value
		Node ndTime = nl.item(i).getFirstChild();
	    	double dbl =
		    Double.parseDouble((String) ndTime.getNodeValue());
	
		//Note: This assumes the timer input is nondecreasing
		if (dbl > dblLastTimer)
		    fOutput=true;
		break;
	    }
	}

	return fOutput;
    }

    /**
     * This function take the first data tuple and uses it as a
     * template for punctuations
     */
    private void setupDataTemplate(StreamTupleElement inputTuple) {
	Node ndRoot = inputTuple.getAttribute(iAttrDataRoot);
	stDataRoot = ndRoot.getNodeName();
		
	NodeList nl = ndRoot.getChildNodes();
	rgstDataChild = new String[nl.getLength()];
	rgiDataType = new short[nl.getLength()];
	for (int i=0; i<rgstDataChild.length; i++) {
	    rgstDataChild[i] = nl.item(i).getNodeName();
	    rgiDataType[i] = nl.item(i).getNodeType();
	}
    }

    /**
     * This function generates a punctuation based on the timer value
     * using the template generated by setupDataTemplate
     */
    private StreamPunctuationElement createPunctuation(
	StreamTupleElement inputTuple, ArrayList values) {
	//This input came from the timer. Generate punctuation 
	// based on the timer value, where the time value is
	// (,last), indicating that all values from the beginning
	// to `last' have been seen. Note this assumes the 
	// attribute is strictly increasing.

	//Create a new punctuation element
	Element ePunct = doc.createElement(StreamPunctuationElement.STPUNCTNS
					   + ":" + stDataRoot);

	for (int iAttr=0; iAttr<rgstDataChild.length; iAttr++) {
	    switch(rgiDataType[iAttr]) {
	    case Node.ELEMENT_NODE:
		Element eChild = doc.createElement(rgstDataChild[iAttr]);
		Text tPattern;

		if (rgstDataChild[iAttr].compareTo(stDataTimer) != 0)
		    tPattern = doc.createTextNode("*");
		else
		    //Note this assume the timer input is non-decreasing
		    tPattern = doc.createTextNode("(," + values.get(0) + ")");

		eChild.appendChild(tPattern);
		ePunct.appendChild(eChild);
		break;

	    case Node.TEXT_NODE:
		Text tChild = doc.createTextNode("*");
		ePunct.appendChild(tChild);
		break;
	    default:
		System.out.println("Unhandled node type: " +
				   rgiDataType[iAttr]);
	    }
	}

	StreamPunctuationElement spe = new StreamPunctuationElement(false);
	spe.appendAttribute(ePunct);

	return spe;
    }

    /**
     * This function handles punctuations for the given operator. For
     * Punctuate, we can simply output any incoming punctuation.
     *
     * @param tuple The current input tuple to examine.
     * @param streamId The id of the source streams the partial result of
     *                 which are to be removed.
     *
     */

    protected void processPunctuation(StreamPunctuationElement tuple,
				      int streamId)
	throws ShutdownException, InterruptedException {
	putTuple(tuple,0);
    }

    public boolean isStateful() {
	return false;
    }
    
    /**
     * @see niagara.optimizer.colombia.PhysicalOp#FindLocalCost(LogicalProperty, LogicalProperty[])
     */
    public Cost findLocalCost(ICatalog catalog, LogicalProperty[] inputLogProp) {
        double trc = catalog.getDouble("tuple_reading_cost");
        double sumCards = 0;
        for (int i = 0; i < inputLogProp.length; i++)
            sumCards += inputLogProp[i].getCardinality();
        return new Cost(trc * sumCards);
    }

    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op opCopy() {
        return new  PhysicalPunctuateOperator(iDataInput, attrTimer,
					      stDataTimer,
					      attrDataRoot);
    }

    /**
     * @see niagara.query_engine.SchemaProducer#constructTupleSchema(TupleSchema[])
     */
    public void constructTupleSchema(TupleSchema[] inputSchemas) {
        inputTupleSchemas = inputSchemas;
        outputTupleSchema = inputTupleSchemas[iDataInput];
    }

    /**
     * @see niagara.query_engine.PhysicalOperator.setResultDocument
     */
    public void setResultDocument(Document doc) {
        this.doc = doc;
    }
    
    /**
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object o) {
        if (o == null || !(o instanceof PhysicalPunctuateOperator))
            return false;
        if (o.getClass() != PhysicalPunctuateOperator.class)
            return o.equals(this);
        return getArity() == ((PhysicalPunctuateOperator) o).getArity();
    }

    public int hashCode() {
        return getArity();
    }
}
