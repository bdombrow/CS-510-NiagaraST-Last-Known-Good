
/**********************************************************************
  $Id: SlidingSum.java,v 1.1 2003/12/24 02:08:29 vpapad Exp $


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

package niagara.logical;

import org.w3c.dom.*;

import java.util.*;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.xmlql_parser.*;

public class SlidingSum extends SlidingWindow {
    // This is the attribute on which summing is done
    Attribute summingAttribute;
    int range;
    int every;

    /**
     * This function sets the skolem attributes on which grouping is
     * done, and the attribute that is summed
     *
     * @param groupingAttrs Attributes on which grouping is done
     * @param summingAttribute Attribute on which summing is done
     */

    public void setSummingInfo (skolem groupingAttrs,
				Attribute summingAttribute) {

	// Set the summing attribute
	//
	this.summingAttribute = summingAttribute;

	// Set the skolem attributes in the super class
	//
	this.groupingAttrs = groupingAttrs;
    }


    /**
     * This function returns the averaging attributes
     *
     * @return Averaging attribute of the operator
     */

    public Attribute getSummingAttribute () {
	return summingAttribute;
    }

    public void dump() {
	System.out.println("SlidingSumOp");
	groupingAttrs.dump();
	System.err.println(summingAttribute.getName());
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
        SlidingSum op = new SlidingSum();
        op.setSummingInfo(groupingAttrs, summingAttribute);
        op.setWindowInfo(range, every);
        return op;
    }

    public int hashCode() {
        return groupingAttrs.hashCode() ^ summingAttribute.hashCode();
    }

    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof SlidingSum)) return false;
        if (obj.getClass() != SlidingSum.class) return obj.equals(this);
        SlidingSum other = (SlidingSum) obj;
        return groupingAttrs.equals(other.groupingAttrs) &&
                summingAttribute.equals(other.summingAttribute);
    }
    
    public void loadFromXML(Element e, LogicalProperty[] inputProperties, Catalog catalog)
        throws InvalidPlanException {
        String id = e.getAttribute("id");
        String groupby = e.getAttribute("groupby");
        String sumattr = e.getAttribute("sumattr");
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

        Attribute summingAttribute =
            Variable.findVariable(inputLogProp, sumattr);
        setSummingInfo(new skolem(id, groupbyAttrs), summingAttribute);
    }
}
