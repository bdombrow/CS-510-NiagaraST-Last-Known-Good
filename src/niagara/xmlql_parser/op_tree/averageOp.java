
/**********************************************************************
  $Id: averageOp.java,v 1.7 2003/03/07 23:36:42 vpapad Exp $


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
 * This is the class for the average operator, that is a type of group
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

public class averageOp extends groupOp {
    // The attribute on which averaging is done
    Attribute averageAttribute;

    public averageOp() {}
    
    public averageOp(
        Attribute averageAttribute,
        skolem skolemAttributes) {
        this.averageAttribute = averageAttribute;
        this.skolemAttributes = skolemAttributes;
    }

    public averageOp(averageOp op) {
        this(op.averageAttribute, op.skolemAttributes);
    }
    
    public Op copy() {
        return new averageOp(this);
    }

    /**
     * Set the skolem attributes on which grouping is
     * done, and the attribute that is averaged
     *
     * @param skolemAttributes Attributes on which grouping is done
     * @param averageAttribute Attribute on which averaging is done
     */

    public void setAverageInfo (skolem skolemAttributes,
				Attribute averageAttribute) {
	this.averageAttribute = averageAttribute;
	this.setSkolemAttributes(skolemAttributes);
    }


    /**
     * This function returns the averaging attribute
     *
     * @return Averaging attribute of the operator
     */
    public Attribute getAveragingAttribute () {
	return averageAttribute;
    }

    public void dump() {
	System.out.println("AverageOp");
    }
    
    public int hashCode() {
        return skolemAttributes.hashCode() ^ averageAttribute.hashCode();
    }
    
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof averageOp)) return false;
        if (obj.getClass() != averageOp.class) return obj.equals(this);
        averageOp other = (averageOp) obj;
        return skolemAttributes.equals(other.skolemAttributes) &&
                averageAttribute.equals(other.averageAttribute);
    }

    public void loadFromXML(Element e, LogicalProperty[] inputProperties)
        throws InvalidPlanException {
        String id = e.getAttribute("id");
        String groupby = e.getAttribute("groupby");
        String avgattr = e.getAttribute("avgattr");

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
    }
}

