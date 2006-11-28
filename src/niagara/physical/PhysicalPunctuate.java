/**********************************************************************
  $Id: PhysicalPunctuate.java,v 1.2 2006/11/28 05:16:09 jinli Exp $


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

import java.util.ArrayList;
import java.util.Vector;

import org.w3c.dom.*;

import niagara.logical.Punctuate;
import niagara.optimizer.colombia.*;
import niagara.query_engine.*;
import niagara.utils.*;

/**
 * <code>PhysicalPunctuateOperator</code> implements a punctuate operator for
 * an incoming stream;
 *
 * @see PhysicalOperator
 */
public class PhysicalPunctuate extends PhysicalOperator {
    private final int cInput = 2;
    //Which input contains data?
    private int iDataInput = 1;

    //Which timer attribute contains the timer value
    private Attribute attrTimer;

    //Which data attribute correponds to the timer
    private Attribute attrDataTimer;

    //Store for input values
    private AtomicEvaluator aeTimer, aeData;
    private ArrayList vTimer, vData, vPrevTimer;

    //Data template for creating punctuation
    Tuple tupleDataSample = null;
    private short rgiDataType[];
    private Document doc;
    //private double dblLastTimer = -1;
    private long dblLastTimer = -1;
    private boolean fLastTimerFirstSet = false;
    private Vector tupleBuffer = new Vector();
    
    private static int WARP = 1;  //Hack
    
    private boolean dataStreamClosed = false;
    private boolean firstTimestamp = true;
    
    public PhysicalPunctuate() {
        setBlockingSourceStreams(new boolean[cInput]);
    }

    public PhysicalPunctuate(int iDI, Attribute aTimer,
				     Attribute attrData) {
        setBlockingSourceStreams(new boolean[cInput]);

	iDataInput = iDI;
	attrTimer = aTimer;
	attrDataTimer = attrData;
    }
    
    public void opInitFrom(LogicalOp logicalOperator) {
        setBlockingSourceStreams(new boolean[cInput]);

	Punctuate pop = (Punctuate) logicalOperator;

	iDataInput = pop.getDataInput();
	attrTimer = pop.getTimerAttr();
	attrDataTimer = pop.getDataTimer();
    }

    public void opInitialize() {
        // XXX vpapad: really ugly...
        setBlockingSourceStreams(new boolean[cInput]);

	aeTimer = new AtomicEvaluator(attrTimer.getName());
        aeTimer.resolveVariables(inputTupleSchemas[1-iDataInput],1-iDataInput);
	vTimer = new ArrayList();
	vPrevTimer = new ArrayList();

	if (attrDataTimer != null) {
	    aeData = new AtomicEvaluator(attrDataTimer.getName());
	    aeData.resolveVariables(inputTupleSchemas[iDataInput],
	                            iDataInput);
	    vData = new ArrayList();
	}
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
    protected void processTuple (
						 Tuple inputTuple,
						 int streamId)
						 
	throws ShutdownException, InterruptedException, OperatorDoneException {
	
	boolean fOutput = true;
	Tuple tmpTuple;

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
	        
	        //Buffer the data tuples for a while before the dbLastTimer is set
	        if (dblLastTimer == -1)
			;//Jenny  tupleBuffer.add(inputTuple);  Jenny:  currently skip all the tuples before we get the first timestamp
	        else { 
	        	/*if (fLastTimerFirstSet) {
	        		for (int i = 0; i < tupleBuffer.size(); i++) {
	        			appendTimestamp((StreamTupleElement)tupleBuffer.get(i));
	        			putTuple((StreamTupleElement)tupleBuffer.get(i), 0);
	        		}
	        		fLastTimerFirstSet = false;
	        	} 
	        	else {
                	appendTimestamp(inputTuple);
	        	putTuple(inputTuple, 0);
	        	}*/
         	
			appendTimestamp(inputTuple);
			putTuple(inputTuple, 0);
	        }
	    }

	    //If we haven't already picked up a template tuple,
	    // copy this one.
	    //if (tupleDataSample == null)
	    if ((tupleDataSample == null) && (dblLastTimer != -1))
	        tupleDataSample = inputTuple;
	} else {
	    //if (tupleDataSample != null) {

		vPrevTimer.clear();
		vPrevTimer = (ArrayList) vTimer.clone();
		vTimer.clear();
		aeTimer.getAtomicValues(inputTuple, vTimer);
		if (vTimer.size() != 1)
		    throw new PEException("Punctuate requires exactly one timer value");

		/*if (firstTimestamp) {
			dblLastTimer = Long.parseLong((String) vTimer.get(0), 10);
			dblLastTimer = dblLastTimer * WARP;
			firstTimestamp = false;
			return;
		}*/
		dblLastTimer = Long.parseLong((String) vTimer.get(0), 10);
				

		if (tupleDataSample != null){
			putTuple(createPunctuation(inputTuple, vPrevTimer), 0);
		}

		
/*		if (dblLastTimer == -1)
		dblLastTimer = Long.parseLong((String) vTimer.get(0), 10);
		else if (tupleDataSample != null){
			dblLastTimer = Long.parseLong((String) vTimer.get(0), 10);
			putTuple(createPunctuation(inputTuple, vTimer), 0);
		}*/
		
		if (dataStreamClosed) 
		    throw new OperatorDoneException();
		
		//if (!fLastTimerFirstSet && (dblLastTimer == -1))
		//	fLastTimerFirstSet = true;
		//else
			//Put the punctuation in the output
			//putTuple(createPunctuation(inputTuple, vTimer), 0);
	   //if (tupleDataSample }
	} 
    }
    private void appendTimestamp(Tuple inputTuple) {
    	String stTS;
        Element eleTS = doc.createElement(Punctuate.STTIMESTAMPATTR);

        if (dblLastTimer == -1){
		//jenny, to set the TIMESTAMP before we get first value from timer
		stTS = String.valueOf(System.currentTimeMillis()*WARP); 
        }else {
        	stTS = String.valueOf(dblLastTimer);
        }
        
        eleTS.appendChild(doc.createTextNode(stTS));
	inputTuple.appendAttribute(eleTS);
    }

    private void appendTimestamp(Punctuation spe,
                                 boolean fTSWildcard) {
        Element eleTS = doc.createElement(Punctuate.STTIMESTAMPATTR);
	String stTS;

	if (fTSWildcard)
	    stTS = new String("*");
	else
	    stTS = new String("(," + String.valueOf(dblLastTimer)  + ")");
	    
        eleTS.appendChild(doc.createTextNode(stTS));
        spe.appendAttribute(eleTS);
    }

    private boolean enforcePunctuation(Tuple tuple) {
        boolean fOutput = true;
	
        if (attrDataTimer != null) {
            vData.clear();
            aeData.getAtomicValues(tuple, vData);
	    if (vData.size() != 1)
	        throw new PEException("Punctuate-more than one value");
	    
	    double dbl = Double.parseDouble((String) vData.get(0));
	    //fOutput = (dbl >= dblLastTimer);  Jenny: think "==" should not be allowed;
	    fOutput = (dbl > dblLastTimer);
	    if (fOutput == false)
	        System.out.println("PT - Tuple matches punctuation that has been output -- dropping it");
	}

	return fOutput;
    }

    /**
     * This function generates a punctuation based on the timer value
     * using the template generated by setupDataTemplate
     */
    private Punctuation createPunctuation(
	Tuple inputTuple, ArrayList values) {
	//This input came from the timer. Generate punctuation 
	// based on the timer value, where the time value is
	// (,last), indicating that all values from the beginning
	// to `last' have been seen. Note this assumes the 
	// attribute is strictly non-decreasing.

	//Create a new punctuation element
	int iAttrTimer = -1;
	short nodeType;
	
	if (attrDataTimer != null)
	    iAttrTimer = 
	        inputTupleSchemas[iDataInput].getPosition(attrDataTimer.getName());
	Punctuation spe =
	    new Punctuation(false);
	for (int iAttr=0; iAttr<tupleDataSample.size() - 1; iAttr++) { //"-1" is used to exclude the "TIMESTAMP" field
		if (iAttr != iAttrTimer)
			spe.appendAttribute(new StringAttr("*"));
		else
			spe.appendAttribute(new StringAttr("(," + values.get(0) + ")"));			
	}

	/*for (int iAttr=0; iAttr<tupleDataSample.size() - 1; iAttr++) { //"-1" is used to exclude the "TIMESTAMP" field
		Node ndSample = tupleDataSample.getAttribute(iAttr);
	    nodeType = ndSample.getNodeType();
	    if (nodeType == Node.ATTRIBUTE_NODE) {
	    	Attr attr = doc.createAttribute(ndSample.getNodeName());
	    	attr.appendChild(doc.createTextNode("*"));
	    	spe.appendAttribute(attr);
	    } else {
			String stName = ndSample.getNodeName();
			if (stName.compareTo("#document") == 0)
			stName = new String("document");
			Element ePunct = doc.createElement(stName);
			if (iAttr != iAttrTimer)
				ePunct.appendChild(doc.createTextNode("*"));
			else
				ePunct.appendChild(doc.createTextNode("(," + values.get(0) + ")"));
			spe.appendAttribute(ePunct);	    	
	    }
	}*/
	appendTimestamp(spe, (iAttrTimer != -1));

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

    protected void processPunctuation(Punctuation tuple,
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
        return new  PhysicalPunctuate(iDataInput, attrTimer,
					      attrDataTimer);
    }

    /**
     * @see niagara.query_engine.SchemaProducer#constructTupleSchema(TupleSchema[])
     */
    public void constructTupleSchema(TupleSchema[] inputSchemas) {
        inputTupleSchemas = inputSchemas;
	outputTupleSchema = inputSchemas[iDataInput];
	Attribute attrTS = logProp.getAttr(Punctuate.STTIMESTAMPATTR);
	outputTupleSchema.addMapping(attrTS);
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
        if (o == null || !(o instanceof PhysicalPunctuate))
            return false;
        if (o.getClass() != PhysicalPunctuate.class)
            return o.equals(this);
        return getArity() == ((PhysicalPunctuate) o).getArity();
    }

    public int hashCode() {
        return getArity();
    }
    
    public void streamClosed( int streamId) 
    throws ShutdownException {
	if (streamId == iDataInput) 
    		dataStreamClosed = true;
 
    }
}
