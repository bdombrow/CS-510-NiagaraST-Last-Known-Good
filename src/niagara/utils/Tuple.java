/**********************************************************************
  $Id: Tuple.java,v 1.3 2006/11/28 05:23:34 jinli Exp $


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
 * This is the <code>Tuple</code> class that is the unit
 * of transfer of tuples across a stream. Tuples are arrays of Nodes.
 *
 * @see Stream
 */
import niagara.magic.MagicBaseNode;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.Text;

public class Tuple {
    // Members to create an expandable array of attributes
	protected Object tuple[];
    protected int allocSize;
    protected int tupleSize;
    // A boolean flag that indicates whether this tuple represents
    // a (potentially) partial result
    protected boolean partial;

    /*
     * Constructor that initializes a tuple
     *
     * @param partial If this is true, the tuple represents a partial result;
     *                else it represents a final result
     */
    public Tuple(boolean partial) {
        // Initialize the tuple vector with the capacity
        createTuple(8);
        this.partial = partial;
    }

    /*
     * The constructor that initializes the tuple with initial capacity
     *
     * @param partial If this is true, the tuple represents a partial result;
     *                else it represents a final result
     * @param capacity The initial capacity of the tuple
     */
    public Tuple(boolean partial, int capacity) {
        // Initialize the tuple vector with the capacity
        if (capacity <= 0) {
            createTuple(8);
        } else {
            createTuple(capacity);
        }
        this.partial = partial;
    }

    private void createTuple(int capacity) {
        allocSize = capacity;
        tuple = new Object[allocSize];
        tupleSize = 0;
    }

    /**
     * This function return the number of attributes in the tuple element
     *
     * @return The number of attributes
     */
    public int size() {
        return tupleSize;
    }

    /**
     * This function is used to determine whether the tuple represents a
     * partial result
     *
     * @return True if the tuple represents a partial result; false otherwise
     */
    public boolean isPartial() {
        return partial;
    }

    public boolean isPunctuation() {
        return false;
    }

    /**
     * This function appends an attribute to the tuple element
     *
     * @param attribute The attribute to be appended
     */
    public void appendAttribute(Object attribute) {
        if (tupleSize >= allocSize)
            expandTuple(tupleSize);
        tuple[tupleSize++] = attribute;
    }

    /**
     * This function appends all the attributes of the tuple element passed
     * as the parameter to the current tuple
     *
     * @param tupleElement The tuple whose attributes are to be appended to
     *                     the current tuple
     */
    public void appendTuple(Tuple otherTuple) {
        // Loop over all the attributes of the other tuple and append them
        if (tupleSize + otherTuple.tupleSize > allocSize)
            expandTuple(tupleSize + otherTuple.tupleSize);

        for (int i = 0; i < otherTuple.tupleSize; i++)
            tuple[tupleSize + i] = otherTuple.tuple[i];

        tupleSize += otherTuple.tupleSize;
    }

    /**
     * This function returns an attribute given its position
     *
     * @param position The position of the desired attribute
     *
     * @return The desired attribute
     */
    public Object getAttribute(int position) {
        assert position
            < tupleSize : "Invalid position "
                + position
                + " tuple size is "
                + tupleSize;
        return tuple[position];
    }

    public void setAttribute(int position, Object value) {
        while (position >= allocSize)
            expandTuple(position);
        if (tupleSize <= position)
            tupleSize = position + 1;

        tuple[position] = value;
    }

    /**
     * This function clones a stream tuple element and returns the clone
     *
     * @return a clone of the stream tuple element
     */
    public Object clone() {
        return copy(tupleSize);
    }

    /** Copy of this tuple, with space reserved for up to `size` attributes */
    public Tuple copy(int size) {
        // XML-QL query plans will come in with size = 0
        if (tupleSize > size)
            size = tupleSize;
        // Create a new stream tuple element with the same partial semantics
        Tuple returnElement =
            new Tuple(partial, size);

        // Add all the attributes of the current tuple to the clone
        System.arraycopy(tuple, 0, returnElement.tuple, 0, tupleSize);
        returnElement.tupleSize = tupleSize;

        // Return the clone
        return returnElement;
    }

    /** Copy parts of this tuple to a new tuple, with space reserved for up to
     * <code>size</code> attributes */
    public Tuple copy(int size, int attributeMap[]) {
        assert size >= attributeMap.length : "Insufficient tuple capacity";
        // Create a new stream tuple element with the same partial semantics
        Tuple returnElement =
            new Tuple(partial, size);

        Object[] newTuple = returnElement.tuple;
        for (int to = 0; to < attributeMap.length; to++) {
            int from = attributeMap[to];
            if (from >= 0)
                newTuple[to] = tuple[from];
        }
        returnElement.tupleSize = attributeMap.length;

        return returnElement;
    }

    /** Copy parts of this tuple into another tuple, starting at offset */
    public void copyInto(
        Tuple ste,
        int offset,
        int attributeMap[]) {
        // If this tuple is partial, the result should be partial        
        if (partial)
            ste.partial = true;

        for (int i = 0; i < attributeMap.length; i++)
            ste.tuple[offset + i] = tuple[attributeMap[i]];
        ste.tupleSize += attributeMap.length;
    }

    /**
     * This function returns a string representation of the stream tuple element
     *
     * @return The string representation
     */
    public String toString() {
        return (tuple.toString() + "; Partial = " + partial);
    }

    public Element toEle(Document doc) {
        Element ret = doc.createElement("StreamTupleElement");
        if (partial)
            ret.setAttribute("PARTIAL", "TRUE");
        else
            ret.setAttribute("PARTIAL", "FALSE");

        for (int i = 0; i < tupleSize; i++) {
            Element tele = doc.createElement("Entry");
            assert tuple[i] instanceof XMLAttr:
            	"Don't know how to deal with a non-XML attribute - Jenny";
            Node tmp = ((XMLAttr) tuple[i]).getNodeValue();
            assert tmp instanceof Element
                || tmp instanceof Text : "KT non elem/string attr in TupleElement";

            if (tmp instanceof Element) {
                tele.setAttribute("Type", "Element");
                tele.appendChild(tmp);
            } else if (tmp instanceof Text) {
                tele.setAttribute("Type", "Text");
                tele.appendChild(tmp); //KT -added this, think it is needed
            }
            ret.appendChild(tele);
        }
        return ret;
    }

    public Tuple(Element ele) {
        if (ele.getAttribute("PARTIAL").equals("TRUE"))
            partial = true;
        else
            partial = false;

        createTuple(8);

        for (Node c = ele.getFirstChild(); c != null; c = c.getNextSibling()) {
            Element e = (Element) c;
            String type = e.getAttribute("Type");
            assert type.equals("Element")
                || type.equals("Text") : "KT invalid type " + type;
            appendAttribute(e.getFirstChild());
        }
    }

    private void expandTuple(int newSize) {
        if (newSize < allocSize)
            return;
        int newAllocSize = allocSize * 2;
        while (newSize >= newAllocSize)
            newAllocSize *= 2;

        Object[] newTuple = new Object[newAllocSize];
        for (int i = 0; i < tupleSize; i++) {
            newTuple[i] = tuple[i];
        }
        tuple = newTuple;
        allocSize = newAllocSize;
    }

    public void setMagicTuple() {
        for (int i = 0; i < tupleSize; i++) {
            if (tuple[i] instanceof MagicBaseNode) {
                ((MagicBaseNode) tuple[i]).setTuple(this);
            }
        }
    }
}
