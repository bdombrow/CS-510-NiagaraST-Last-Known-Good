
/**********************************************************************
  $Id: SlidingCountOp.java,v 1.5 2003/07/03 19:29:59 tufte Exp $


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


/**
 * This is the class for the count operator, that is a type of group
 * operator.
 *
 * @version 1.0
 *
 */

package niagara.xmlql_parser.op_tree;

import org.w3c.dom.*;

import java.util.Vector;
import java.util.StringTokenizer;

import niagara.connection_server.InvalidPlanException;
import niagara.logical.Variable;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.xmlql_parser.syntax_tree.*;

public class SlidingCountOp extends SlidingWindowOp {

    /////////////////////////////////////////////////////////////////
    // These are the private members of the count operator       //
    /////////////////////////////////////////////////////////////////

    // This is the attribute on which counting is done
    //
    Attribute countingAttribute;
    int range;
    int every;

    /////////////////////////////////////////////////////////////////
    // These are the methods of the class                          //
    /////////////////////////////////////////////////////////////////

    /**
     * This function sets the skolem attributes on which grouping is
     * done, and the attribute that is counted
     *
     * @param skolemAttributes Attributes on which grouping is done
     * @param countAttribute Attribute on which counting is done
     */

    public void setCountingInfo (skolem groupingAttrs,
				Attribute countingAttribute) {

	// Set the counting attribute
	//
	this.countingAttribute = countingAttribute;

	// Set the skolem attributes in the super class
	//
	this.groupingAttrs = groupingAttrs;
    }


    /**
     * This function returns the counting attributes
     *
     * @return Counting attribute of the operator
     */

    public Attribute getCountingAttribute () {

	// Return the counting attribute
	//
	return countingAttribute;
    }

    public void dump() {
	System.out.println("SlidingCountOp");
	groupingAttrs.dump();
	System.err.println(countingAttribute.getName());
    }

    public void setWindowInfo (int range, int every) {
    this.range = range;
    this.every = every;
    }
    
    public int getWindowRange () {
    return this.range;
    }
    
    public int getWindowEvery () {
    return this.every;
    }   
    
    public Op opCopy() {
        SlidingCountOp op = new SlidingCountOp();
        op.setCountingInfo(groupingAttrs, countingAttribute);
        op.setWindowInfo(range, every);
        return op;
    }

    public int hashCode() {
        return groupingAttrs.hashCode() ^ countingAttribute.hashCode();
    }

    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof SlidingCountOp)) return false;
        if (obj.getClass() != SlidingCountOp.class) return obj.equals(this);
        SlidingCountOp other = (SlidingCountOp) obj;
        return groupingAttrs.equals(other.groupingAttrs) &&
                countingAttribute.equals(other.countingAttribute);
    }
         
    public void loadFromXML(Element e, LogicalProperty[] inputProperties)
        throws InvalidPlanException {
        String id = e.getAttribute("id");
        String groupby = e.getAttribute("groupby");
        String countattr = e.getAttribute("countattr");
        String range = e.getAttribute("range");
        String every = e.getAttribute("every");

        // set the range and every parameter for the sliding window;
        Integer rangeValue;
        Integer everyValue;
        if (range != "") {
            rangeValue = new Integer(range);
            if (rangeValue.intValue() <= 0)
                throw new InvalidPlanException("range must greater than zero");
        } else
            throw new InvalidPlanException("range ???");
        if (every != "") {
            everyValue = new Integer(every);
            if (everyValue.intValue() <= 0)
                throw new InvalidPlanException("every must greater than zero");
        } else
            throw new InvalidPlanException("every ???");

        setWindowInfo(rangeValue.intValue(), everyValue.intValue());

        LogicalProperty inputLogProp = inputProperties[0];

        // Parse the groupby attribute to see what to group on
        Vector groupbyAttrs = new Vector();
        StringTokenizer st = new StringTokenizer(groupby);
        while (st.hasMoreTokens()) {
            String varName = st.nextToken();
            Attribute attr = Variable.findVariable(inputLogProp, varName);
            groupbyAttrs.addElement(attr);
        }

        Attribute countingAttribute =
            Variable.findVariable(inputLogProp, countattr);
        setCountingInfo(new skolem(id, groupbyAttrs), countingAttribute);
    }
}
