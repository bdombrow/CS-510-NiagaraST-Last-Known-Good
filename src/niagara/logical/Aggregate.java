/**********************************************************************
  $Id: Aggregate.java,v 1.1 2003/12/24 02:08:28 vpapad Exp $


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

package niagara.logical;

import org.w3c.dom.*;

import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.*;

/**
 * This is the class for the logical group operator. This is an abstract
 * class from which various notions of grouping can be derived. The
 * core part of this class is the skolem function attributes that are
 * used for grouping and are common to all the sub-classes
 *
 */
public abstract class Aggregate extends Group {

    // the attribute on which the aggregation function should
    // be performed
    protected Attribute aggrAttr;
    protected abstract Aggregate getInstance();

    protected void loadFromXML(Element e, 
			       LogicalProperty[] inputProperties,
			       String aggrAttrName) 
	throws InvalidPlanException {

        String aggAttrStr = e.getAttribute(aggrAttrName);
        aggrAttr = Variable.findVariable(inputProperties[0], aggAttrStr);
	loadGroupingAttrsFromXML(e, inputProperties[0], "groupby");
    }

    public Attribute getAggrAttr() {
	return aggrAttr;
    }

    protected void dump(String opName) {
	System.out.println("opName");
	System.out.print("Grouping Attrs: ");
	groupingAttrs.dump();
	System.err.println("Aggregate Attr: " + aggrAttr.getName());
    }

    public Op opCopy() {
	Aggregate op =null;
	op = getInstance();
	op.groupingAttrs = this.groupingAttrs;
	op.aggrAttr = this.aggrAttr;
	return op;
    }

    public int hashCode() {
        return groupingAttrs.hashCode() ^ aggrAttr.hashCode();
    }

    public boolean equals(Object obj) {
        if (obj == null)
	    return false;
	if(!this.getClass().isInstance(obj)) 
	    return false;
        if (obj.getClass() != this.getClass()) 
	    return obj.equals(this);
        Aggregate other = (Aggregate) obj;
        return groupingAttrs.equals(other.groupingAttrs) &&
                aggrAttr.equals(other.aggrAttr);
    }    
}
