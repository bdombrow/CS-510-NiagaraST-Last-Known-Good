
/**********************************************************************
  $Id: StreamPunctuationElement.java,v 1.2 2002/10/02 22:13:27 ptucker Exp $


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
 * This is the <code>StreamPunctuationElement</code> class that is the unit
 * of transfer of punctuation across a stream. Like tuples, punctuations
 * are arrays of Nodes.
 *
 * @version 1.0
 *
 * @see Stream
 */

import org.w3c.dom.*;

public final class StreamPunctuationElement extends StreamTupleElement {
    public static final String STPUNCTNS = "PUNCT";

    public StreamPunctuationElement (boolean partial) {
		super(partial);
    }

    public StreamPunctuationElement (boolean partial, int capacity) {
		super(partial, capacity);
    }

    public StreamPunctuationElement(Element ele) {
		super(ele);
    }

    public boolean isPunctuation() {
		return true;
    }

    /**
     * This function determines if the given punctuation is equal to this
     * punctuation
     *
     * @return true if the two punctuations are equal
     */

    public boolean equals(StreamPunctuationElement punct) {
	//Only compare the 'document' nodes

	return nodeEquals(this.getAttribute(0), punct.getAttribute(0));
    }

    private boolean nodeEquals(Node nd1, Node nd2) {
	if (nd1.getNodeName().equals(nd2.getNodeName()) == false)
	    return false;

	if (nd1.getNodeType() != nd2.getNodeType())
	    return false;

	String stValue = nd1.getNodeValue();
	if (stValue == null) {
	    if (nd2.getNodeValue() != null)
		return false;
	} else {
	    if (stValue.equals(nd2.getNodeValue()) == false)
		return false;
	}

	NodeList nl1 = nd1.getChildNodes();
	NodeList nl2 = nd2.getChildNodes();
	if (nl1.getLength() != nl2.getLength())
	    return false;

	boolean fEquals = true;
	for (int i=0; i < nl1.getLength() && fEquals; i++) {
	    fEquals = nodeEquals(nl1.item(i), nl2.item(i));
	}

	return fEquals;
    }

    /**
     * This function clones a stream tuple element and returns the clone
     *
     * @return a clone of the stream tuple element
     */

    public Object clone() {

	// Create a new stream punctuation element with the same partial
	// semantics
	StreamPunctuationElement returnElement = 
	    new StreamPunctuationElement(this.partial, tupleSize);

	// Add all the attributes of the current tuple to the clone
	//
	returnElement.appendAttributes(this);
        returnElement.setTimeStamp(timeStamp);

	// Return the clone
	//
	return returnElement;
    }

    public boolean match(StreamTupleElement ste) {
	//Punctuations do not match each other
	if (ste.isPunctuation())
	    return false;

	//This punctuation should be verified against the first
	// node in the tuple (the "document" node)
	return matchNode(this.getAttribute(0), ste.getAttribute(0));
    }

    private boolean matchNode(Node ndPunct, Node ndTuple) {
	String stPunct = ndPunct.getNodeValue();
	boolean fMatch = true;

	if (stPunct == null) {
	    //Need to compare children of this node
	    //for now, assume order matters between the punctuation
	    // and the tuple.
	    NodeList nlPunct = ndPunct.getChildNodes();
	    int cChild = nlPunct.getLength();

	    //Special case: if the punct has only a text node, check if
	    // it is a wildcard. If so, we can exit with 'true'
	    if (cChild == 1 &&
		nlPunct.item(0).getNodeType() == Node.TEXT_NODE) {
		String st = nlPunct.item(0).getNodeValue();
		if (st.equals("*"))
		    return true;
	    }

	    NodeList nlTuple = ndTuple.getChildNodes();
	    if (cChild != nlTuple.getLength())
		fMatch = false;

	    for (int iChild = 0; iChild < cChild && fMatch == true; iChild++) {
		fMatch = matchNode(nlPunct.item(iChild), nlTuple.item(iChild));
	    }
	} else {
	    fMatch = matchValue(stPunct, ndTuple.getNodeValue());
	}

	return fMatch;
    }

    public static boolean matchValue(String stPunct, String stValue) {
	boolean fMatch = false;

	if (stPunct == null)
	    return false;

	if (stPunct.equals("*"))
	    //wildcard, everything matches that.
	    fMatch = true;
	else if (stPunct.charAt(0) == '(' || stPunct.charAt(0) == '[') {
	    //range of values. See if that value we have is in that range.
	} else if (stPunct.charAt(0) == '{') {
	    //list of items. See if the tuple value matches an item in the list
	} else {
	    //must be a constant. They should be equal
	    fMatch = stPunct.equals(stValue);
	}

	return fMatch;
    }
}





