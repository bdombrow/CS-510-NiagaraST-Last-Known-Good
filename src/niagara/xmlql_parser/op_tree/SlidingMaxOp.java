
/**********************************************************************
  $Id: SlidingMaxOp.java,v 1.4 2003/03/19 00:35:26 tufte Exp $


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
 * This is the class for the max operator, that is a type of group
 * operator.
 *
 * @version 1.0
 *
 */

package niagara.xmlql_parser.op_tree;


import org.w3c.dom.*;

import java.util.*;

import niagara.connection_server.InvalidPlanException;
import niagara.logical.Variable;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.xmlql_parser.syntax_tree.*;

public class SlidingMaxOp extends SlidingWindowOp {

    // This is the attribute on which maxing is done
    Attribute maxingAttribute;
    int range;
    int every;

    /**
     * This function sets the skolem attributes on which grouping is
     * done, and the attribute that is summed
     *
     * @param groupingAttrs Attributes on which grouping is done
     * @param summingAttribute Attribute on which summing is done
     */

    public void setMaxingInfo (skolem groupingAttrs,
				Attribute maxingAttribute) {

	// Set the maxing attribute
	//
	this.maxingAttribute = maxingAttribute;

	// Set the skolem attributes in the super class
	//
	this.groupingAttrs = groupingAttrs;
    }


    /**
     * This function returns the averaging attributes
     *
     * @return Maxing attribute of the operator
     */

    public Attribute getMaxingAttribute () {
	return maxingAttribute;
    }

    public void dump() {
	System.out.println("SlidingMaxOp");
	groupingAttrs.dump();
	System.err.println(maxingAttribute.getName());
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
    
    
    public Op copy() {
        SlidingMaxOp op = new SlidingMaxOp();
        op.setMaxingInfo(groupingAttrs, maxingAttribute);
        op.setWindowInfo(range, every);
        return op;
    }

    public int hashCode() {
        return groupingAttrs.hashCode() ^ maxingAttribute.hashCode();
    }

    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof SlidingMaxOp)) return false;
        if (obj.getClass() != SlidingMaxOp.class) return obj.equals(this);
        SlidingMaxOp other = (SlidingMaxOp) obj;
        return groupingAttrs.equals(other.groupingAttrs) &&
                maxingAttribute.equals(other.maxingAttribute);
    }

    public void loadFromXML(Element e, LogicalProperty[] inputProperties)
        throws InvalidPlanException {
        String id = e.getAttribute("id");
        String groupby = e.getAttribute("groupby");
        String maxattr = e.getAttribute("maxattr");
        String range = e.getAttribute("range");
        String every = e.getAttribute("every");

        // set the range and every parameter for the sliding window;
        //
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

        Attribute maxingAttribute =
            Variable.findVariable(inputLogProp, maxattr);
        setMaxingInfo(new skolem(id, groupbyAttrs), maxingAttribute);
    }
}
