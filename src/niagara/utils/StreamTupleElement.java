
/**********************************************************************
  $Id: StreamTupleElement.java,v 1.4 2002/04/19 20:49:54 tufte Exp $


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


package niagara.utils;

/**
 * This is the <code>StreamTupleElement</code> class that is the unit
 * of transfer of tuples across a stream. Tuples are arrays of Nodes.
 *
 * @version 1.0
 *
 * @see Stream
 */

import java.util.Vector;
import org.w3c.dom.*;

public final class StreamTupleElement extends StreamElement {

    // Members to create an expandable array of nodes
    private Node tuple[];
    private int allocSize;
    private int tupleSize;

    // A boolean flag that indicates whether this tuple represents
    // a (potentially) partial result
    //
    private boolean partial;
    private long timeStamp;

    /*
     * Constructor that initializes a tuple
     *
     * @param partial If this is true, the tuple represents a partial result;
     *                else it represents a final result
     */

    public StreamTupleElement (boolean partial) {
	// Initialize the tuple vector with the capacity
	createTuple(8);
        timeStamp = 0;
	this.partial = partial;
    }


    /*
     * The constructor that initializes the tuple with initial capacity
     *
     * @param partial If this is true, the tuple represents a partial result;
     *                else it represents a final result
     * @param capacity The initial capacity of the tuple
     */

    public StreamTupleElement (boolean partial, int capacity) {
	
	// Initialize the tuple vector with the capacity
	createTuple(capacity);
        timeStamp = 0;
	this.partial = partial;

	// Initialize the old version of the tuple to null
	//
	//this.oldVersion = null;
    }

    private void createTuple(int capacity) {
	allocSize = capacity;
	tuple = new Node[allocSize];
	tupleSize = 0;
    }
     
    /**
     * The constructor that initializes the tuple and stores the old version
     * of the tuple
     *
     * @param partial If this is true, the tuple represents a partial result;
     *                else it represents a final result
     * @param oldVersion The old version of the current tuple
     */
    /*
    public StreamTupleElement (boolean partial, StreamTupleElement oldVersion) {

	// Initialize the tuple vector with capacity equal to size of old
	// version, if it exists
	//
	if (oldVersion != null) {
	    tuple = new Vector(oldVersion.size());
	}
	else {

	    // Initialize it with default size
	    //
	    tuple = new Vector();
	}

	// Initialize whether the tuple is a partial result
	//
	this.partial = partial;

	// Initialize the old version of the tuple
	//
	this.oldVersion = oldVersion;
        this.timeStamp = 0;
    }
    */

    /**
     * This function return the number of attributes in the tuple element
     *
     * @return The number of attributes
     */

    public int size () {
	return tupleSize;
    }


    /**
     * This function is used to determine whether the tuple represents a
     * partial result
     *
     * @return True if the tuple represents a partial result; false otherwise
     */

    public boolean isPartial () {
	return partial;
    }

    public long getTimeStamp() {
        return timeStamp;
    }
    public void setTimeStamp(long ts) {
        this.timeStamp = ts;
    }

    /**
     * This function appends an attribute to the tuple element
     *
     * @param attribute The attribute to be appended
     */

    public void appendAttribute (Node attribute) {
	if(tupleSize == allocSize) {
	    expandTuple();
	}
	tuple[tupleSize] = attribute;
	tupleSize++;
    }


    /**
     * This function appends all the attributes of the tuple element passed
     * as the parameter to the current tuple
     *
     * @param tupleElement The tuple whose attributes are to be appended to
     *                     the current tuple
     */

    public void appendAttributes (StreamTupleElement otherTuple) {

	// Loop over all the attributes of the other tuple and append them
	while(tupleSize+otherTuple.tupleSize > allocSize) {
	    expandTuple();
	}

	for (int i = 0; i< otherTuple.tupleSize; i++) {
	    tuple[tupleSize+i] = otherTuple.tuple[i];
	}
	tupleSize+=otherTuple.tupleSize;;
    }


    /**
     * This function returns an attribute given its position
     *
     * @param position The position of the desired attribute
     *
     * @return The desired attribute
     */

    public Node getAttribute (int position) {
	return tuple[position];
    }


    public void setAttribute(int position, Node value) {
	while(position >= allocSize) {
	    expandTuple();
	    tupleSize = position+1;
	}

        tuple[position] = value;
    }

    /**
     * This function returns the old version of the tuple
     *
     * @return Old version of the tuple; If no old version, returns null.
     */
    /*
    public StreamTupleElement getOldVersion () {

	return oldVersion;
    }
    */

    /**
     * This function clones a stream tuple element and returns the clone
     *
     * @return a clone of the stream tuple element
     */

    public Object clone() {

	// Create a new stream tuple element with the same partial semantics
	//
	StreamTupleElement returnElement = 
	    new StreamTupleElement(this.partial, tupleSize);

	// Add all the attributes of the current tuple to the clone
	//
	returnElement.appendAttributes(this);
        returnElement.setTimeStamp(timeStamp);

	// Return the clone
	//
	return returnElement;
    }


    /**
     * This function returns a string representation of the stream tuple element
     *
     * @return The string representation
     */
    public String toString () {

	return (tuple.toString() + "; Partial = " + partial);
    }

    /**
     * This function removes all the last N attributes of the tuple element
     * used only in Trigger Grouping. 
     *
     * @param the number of last elements which are going to be removed. 
     */

    public void removeLastNAttributes (int N) {

        for (int i = tupleSize-1; i >= tupleSize-N; i--) {
		tuple[i] = null;
        }
	tupleSize = tupleSize-N;
    }


    public Element toEle(Document doc) {
        Element ret = doc.createElement("StreamTupleElement");
        if(partial) 
	    ret.setAttribute("PARTIAL", "TRUE");
        else 
	    ret.setAttribute("PARTIAL", "FALSE");
        ret.setAttribute("TIMESTAMP", ""+timeStamp);
        
        for(int i=0; i<tupleSize; i++) {
            Element tele = doc.createElement("Entry");
            Node tmp = tuple[i];
            if(tmp instanceof Element) {
                tele.setAttribute("Type", "Element");
                tele.appendChild(tmp);
            } else if(tmp instanceof Text) {
                tele.setAttribute("Type", "Text");
		tele.appendChild(tmp); //KT -added this, think it is needed
            }  else {
		throw new PEException("Non Elem/String Attr in TupleElement");
            }
            ret.appendChild(tele);
        }
        return ret;
    }

    public StreamTupleElement(Element ele) {
        if(ele.getAttribute("PARTIAL").equals("TRUE")) 
            partial = true;
        else partial = false;

        String ts = ele.getAttribute("TIMESTAMP");
        timeStamp = Long.parseLong(ts);

	createTuple(8);

        for(Node c = ele.getFirstChild(); c!=null; c=c.getNextSibling()) {
            Element e = (Element)c;
            String type = e.getAttribute("Type");
            if(type.equals("Element") || type.equals("Text")) {
                appendAttribute(e.getFirstChild());
            } else {
                throw new PEException("KT this shouldn't happen!");
	    }
        }
    }

    private void expandTuple() {
	Node[] newTuple = new Node[allocSize*2];
	for(int i=0; i<tupleSize; i++) {
	    newTuple[i] = tuple[i];
	}
	tuple = newTuple;
	allocSize = allocSize*2;
    }

}






