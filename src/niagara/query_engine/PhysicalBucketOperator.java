/**********************************************************************
  $Id: PhysicalBucketOperator.java,v 1.2 2003/12/06 06:52:14 jinli Exp $


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

import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;

import niagara.optimizer.colombia.*;
import org.w3c.dom.*;
import java.lang.Object;

/**
 * Implementation of the Select operator.
 */
 
public class PhysicalBucketOperator extends PhysicalOperator {
    // No blocking source streams
    private static final boolean[] blockingSourceStreams = { false };

    //window Attributes
    private Attribute windowAttribute;
    private int windowType;
    private long range;
    private long slide;
    private int count = 0;
    private long windowId_from = 0;
    private long windowId_to = 0;    
    private int windowCount = 0;
    private Document doc;
    
   //Data template for creating punctuation
   private String rgstDataChild[] = null;
   private short rgiDataType[];
   private String stDataRoot;
   // Data template for creating punctuation
   StreamTupleElement tupleDataSample = null;  
   private long startSecond; 
   
   
    public PhysicalBucketOperator() {
	setBlockingSourceStreams(blockingSourceStreams);
    }
    
    public void opInitFrom(LogicalOp logicalOperator) { //I think I should change the logical operator to groupOp and related stuff
	// Get the averaging attribute of the average logical operator
	windowAttribute = ((BucketOp) logicalOperator).getWindowAttr();
	windowType = ((BucketOp) logicalOperator).getWindowType();		
	range = ((BucketOp) logicalOperator).getWindowRange();
	slide = ((BucketOp) logicalOperator).getWindowSlide();
    }

    public Op opCopy() {
	PhysicalBucketOperator p = new PhysicalBucketOperator();
	p.windowAttribute = windowAttribute;
	p.windowType = windowType;
	p.range = range;
	p.slide = slide;

	return p;
    }
    
    public void opInitialize() {
	setBlockingSourceStreams(blockingSourceStreams);	
	}   
    
    /**
     * This function processes a tuple element read from a source stream
     * when the operator is non-blocking. This over-rides the corresponding
     * function in the base class.
     *
     * @param tupleElement The tuple element read from a source stream
     * @param streamId The source stream from which the tuple was read
     *
     * @exception ShutdownException query shutdown by user or execution error
     */

    protected void nonblockingProcessSourceTupleElement (
			     StreamTupleElement inputTuple, int streamId)
	throws ShutdownException, InterruptedException {
	StreamTupleElement result;
	//If we haven't already picked up a template tuple,
	// copy this one.
	if (tupleDataSample == null)
		tupleDataSample = inputTuple;
			
	count++;
        result = appendWindowId(inputTuple, streamId);
        putTuple(result, 0);	    
	if ((count % slide == 0) && (windowType == 0)) {
		//output a punctuation to say windowId_from is completed			
		putTuple (createPunctuation (inputTuple, windowId_from), 0);
	}    		
        
    }
       
    protected StreamTupleElement appendWindowId (StreamTupleElement inputTuple, int steamId) 
    throws ShutdownException, InterruptedException {
	
	long mod;
	long numOfWindows;
	long prev = 0;
	
	//wid_from = 1, 2, 3 ...
	//count = 1, 2, 3, ...	
    	if (windowType == 0) { // tuple-based window    		
		// Compute the window Id
		//
		if ((range % slide) == 0) {
			numOfWindows = range / slide;
			windowId_from = count / slide;
			
			mod = count % slide;
			if (mod != 0)
				windowId_from += 1;
			windowId_to = windowId_from + numOfWindows - 1;		
		}
		else {
			numOfWindows = range / slide + 1;
			windowId_from = count / slide;
			mod = count % slide;
			if (mod != 0)
				windowId_from += 1;
			prev = windowId_from;
			windowId_to = (range + count) / slide;
			if ((range + count) % slide == 0)
				windowId_to -= 1;			
		}
		
/*		if (count % slide == 0) {
			//output a punctuation to say windowId_from is completed			
			putTuple (createPunctuation (inputTuple, windowId_from), 0);
		}*/
    	}
	else if (windowType == 1) { // value-based window
				
		long timestamp;
		try {
//			int pos = inputTupleSchemas[0].getPosition("TIMESTAMP");
			String windowAttrName = windowAttribute.getName();
			int pos = inputTupleSchemas[0].getPosition(windowAttrName);
			
			String tmp = inputTuple.getAttribute(pos).getFirstChild().getNodeValue();
			timestamp = Long.parseLong(tmp);
			
		} catch (NumberFormatException nfe) {
			timestamp = 0;
		}
		if ( count==1 )
			startSecond = timestamp;
    		
		if ((range % slide) == 0) {
			numOfWindows = range / slide;
			windowId_from = (timestamp - startSecond + 1) / slide;
			mod = (timestamp - startSecond + 1) % slide;
			if (mod != 0)
				windowId_from += 1;
			prev = windowId_from;
			windowId_to = windowId_from + numOfWindows - 1;
			}
			else {
				numOfWindows = range / slide + 1;
				windowId_from = (timestamp - startSecond + 1) / slide;
				mod = (timestamp - startSecond + 1) % slide;
				if (mod != 0)
					windowId_from += 1;
				prev = windowId_from;
				windowId_to = (range + (timestamp - startSecond + 1)) / slide;
				if (( (range + (timestamp - startSecond + 1)) / slide ) == 0) {
					windowId_to -= 1;
				}
    			
			} 
		}	
    		
	int numNodes = inputTuple.size();
	
	StreamTupleElement result = inputTuple.copy(numNodes);

	Element from = doc.createElement("wid_from");
	from.appendChild(doc.createTextNode(String.valueOf(windowId_from)));    	
	Element to = doc.createElement("wid_to");
	to.appendChild(doc.createTextNode(String.valueOf(windowId_to)));    	

	result.appendAttribute(from);
	result.appendAttribute(to);
	
	return result;

    }
    
    /**
     * This function processes a punctuation element read from a source stream
     * when the operator is non-blocking. This over-rides the corresponding
     * function in the base class.
     *
     * Punctuations can simply be sent to the next operator from Select
     *
     * @param inputTuple The tuple element read from a source stream
     * @param streamId The source stream from which the tuple was read
     *
     * @exception ShutdownException query shutdown by user or execution error
     */
/*    protected void processPunctuation(StreamPunctuationElement inputTuple,
				      int streamId)
	throws ShutdownException, InterruptedException {
	if (inputTuple.isPunctuation()) {  // for time-based bucket
		
	    windowCount++;  	    
	    Element from = doc.createElement("wid_from");
	    from.appendChild(doc.createTextNode("(," + String.valueOf(windowCount) + ")"));   //windowId_from? 	
	    Element to = doc.createElement("wid_to");
	    to.appendChild(doc.createTextNode(String.valueOf("*")));    
	    
	    inputTuple.appendAttribute(from);
	    inputTuple.appendAttribute(to);
	    
	    putTuple(inputTuple, streamId);
	    
	}
	//putTuple(inputTuple, streamId);
    }*/
    
	protected void processPunctuation(StreamPunctuationElement inputTuple,
					  int streamId)
	throws ShutdownException, InterruptedException {
	if (inputTuple.isPunctuation()) {  // for time-based bucket  
	   
	   long wid=0, mod;
	   //long timestamp;
	   double timestamp;
	   int length;
	   String transform;

	   String windowAttrName = windowAttribute.getName();
	   int pos = inputTupleSchemas[0].getPosition(windowAttrName);

	   String tmp = inputTuple.getAttribute(pos).getFirstChild().getNodeValue();
	   //assume that the punctuation on winattr should have the format of (, *****)
	   transform = tmp.trim();
	   length = transform.length();
	   tmp = transform.substring(1, length - 1);
	   transform = tmp.trim();
	   length = transform.length();
	   tmp = transform.substring(1, length);
	   
	  // timestamp = Long.parseLong(tmp);	   
	  timestamp = Double.parseDouble(tmp);
	   
	   if ((range % slide) == 0) {
		wid = (long)(timestamp - startSecond + 1) / slide;
		mod = (long)(timestamp - startSecond + 1) % slide;
		if (mod != 0)
			wid += 1;
		}
		else {
			wid =(long) (timestamp - startSecond + 1) / slide;
			mod = (long) (timestamp - startSecond + 1) % slide;
		   } 
		StreamPunctuationElement punct = null;
	    	    
		Element ts = doc.createElement(windowAttrName);
		ts.appendChild(doc.createTextNode("*"));
		inputTuple.setAttribute(pos, ts);
	    	    
		Element from = doc.createElement("wid_from");
		from.appendChild(doc.createTextNode("(," + String.valueOf(wid-1) + ")"));   //windowId_from? 	
		//from.appendChild(doc.createTextNode( String.valueOf(wid-1)));   //windowId_from?
		Element to = doc.createElement("wid_to");
		to.appendChild(doc.createTextNode(String.valueOf("*")));    
	    
		inputTuple.appendAttribute(from);	   	    
		inputTuple.appendAttribute(to);

		putTuple(inputTuple, streamId);

	}
	}
	    
	/**
	 * This function generates a punctuation based on the timer value
	 * using the template generated by setupDataTemplate
	 */
	private StreamPunctuationElement createPunctuation(
	StreamTupleElement inputTuple, long value) {
	//This input came from the timer. Generate punctuation 
	// based on the timer value, where the time value is
	// (,last), indicating that all values from the beginning
	// to `last' have been seen. Note this assumes the 
	// attribute is strictly non-decreasing.
	Element eChild;
	Text tPattern;
	short nodeType;
	
	//Create a new punctuation element
	StreamPunctuationElement spe =
		new StreamPunctuationElement(false);
	for (int iAttr=0; iAttr<tupleDataSample.size(); iAttr++) {
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
			ePunct.appendChild(doc.createTextNode("*"));
			spe.appendAttribute(ePunct);
		}
	}
	tPattern = doc.createTextNode("(," +Long.toString(value) + ")");
	eChild = doc.createElement("wid_from");
	eChild.appendChild(tPattern);
	spe.appendAttribute(eChild);
	
	tPattern = doc.createTextNode("*");
	eChild = doc.createElement("wid_to");
	eChild.appendChild(tPattern);
	spe.appendAttribute(eChild);	
		
	return spe;
	}	
    public boolean isStateful() {
	return false;
    }
    
    /**
     * @see niagara.optimizer.colombia.PhysicalOp#FindLocalCost(ICatalog, LogicalProperty, LogicalProperty[])
     */
    public Cost findLocalCost(ICatalog catalog, LogicalProperty[] inputLogProp) {
	double trc = catalog.getDouble("tuple_reading_cost");
	double sumCards = 0;
	for (int i = 0; i < inputLogProp.length; i++)
	    sumCards += inputLogProp[i].getCardinality();
	return new Cost(trc * sumCards);
    }
    

    public boolean equals(Object o) {
	 if (o == null || !(o instanceof PhysicalBucketOperator))
	     return false;
	 if (o.getClass() != PhysicalBucketOperator.class)
	     return o.equals(this);
	return getArity() == ((PhysicalUnionOperator) o).getArity();
     }


    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
	return getArity();
    }    
    /**
     * @see niagara.optimizer.colombia.PhysicalOp#FindPhysProp(PhysicalProperty[])
     */
    public PhysicalProperty findPhysProp(PhysicalProperty[] input_phys_props) {
	return input_phys_props[0];
    }
    
    public void setResultDocument(Document doc) {
	this.doc = doc;
    }    
    
	 /**
	  * @see niagara.query_engine.SchemaProducer#constructTupleSchema(TupleSchema[])
	  */
	 public void constructTupleSchema(TupleSchema[] inputSchemas) {
		 inputTupleSchemas = inputSchemas;
	 	outputTupleSchema = inputSchemas[0];
	 	Attribute attrFrom = logProp.getAttr("wid_from");
	 	Attribute attrTo = logProp.getAttr("wid_to");
	 	outputTupleSchema.addMapping(attrFrom);
	 	outputTupleSchema.addMapping(attrTo);
	 }    
    
}


