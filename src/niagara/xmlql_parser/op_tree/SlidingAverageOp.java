
/**********************************************************************
  $Id: SlidingAverageOp.java,v 1.5 2003/07/03 19:29:59 tufte Exp $


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

public class SlidingAverageOp extends SlidingWindowOp {
    // This is the attribute on which averaging is done
    Attribute averageAttribute;
	int range;
	int every;

    /**
     * This function sets the skolem attributes on which grouping is
     * done, and the attribute that is average
     *
     * @param skolemAttributes Attributes on which grouping is done
     * @param averageAttribute Attribute on which averaging is done
     */

    public void setAverageInfo (skolem skolemAttributes,
				Attribute averageAttribute) {

	// Set the average attribute
	//
	this.averageAttribute = averageAttribute;

	// Set the skolem attributes in the super class
	//
	this.groupingAttrs = skolemAttributes;
    }


    /**
     * This function returns the averaging attributes
     *
     * @return Averaging attribute of the operator
     */

    public Attribute getAveragingAttribute () {

	// Return the averaging attribute
	//
	return averageAttribute;
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
    
    public Attribute getAverageAttribute () {
	return averageAttribute;
    }

    public void dump() {
	System.out.println("SlidingAverageOp");
	groupingAttrs.dump();
	System.err.println(averageAttribute.getName());
    }
    
    public Op opCopy() {
        SlidingAverageOp op = new SlidingAverageOp();
        op.setAverageInfo(groupingAttrs, averageAttribute);
        op.setWindowInfo (range, every);
        return op;
    }

    public int hashCode() {
        return groupingAttrs.hashCode() ^ averageAttribute.hashCode();
    }

    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof SlidingAverageOp)) return false;
        if (obj.getClass() != SlidingAverageOp.class) return obj.equals(this);
        SlidingAverageOp other = (SlidingAverageOp) obj;
        return groupingAttrs.equals(other.groupingAttrs) &&
                averageAttribute.equals(other.averageAttribute);
    }

    public void loadFromXML(Element e, LogicalProperty[] inputProperties)
        throws InvalidPlanException {
        String id = e.getAttribute("id");
        String groupby = e.getAttribute("groupby");
        String avgattr = e.getAttribute("avgattr");
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

        LogicalProperty inputLogProp = inputProperties[0];

        // Parse the groupby attribute to see what to group on
        Vector groupbyAttrs = new Vector();
        StringTokenizer st = new StringTokenizer(groupby);
        while (st.hasMoreTokens()) {
            String varName = st.nextToken();
            Attribute attr = Variable.findVariable(inputLogProp, varName);
            groupbyAttrs.addElement(attr);
        }

        Attribute averagingAttribute =
            Variable.findVariable(inputLogProp, avgattr);
        setAverageInfo(new skolem(id, groupbyAttrs), averagingAttribute);
        setWindowInfo(rangeValue.intValue(), everyValue.intValue());
    }
}
