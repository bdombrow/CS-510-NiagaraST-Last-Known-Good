/**********************************************************************
  $Id: SumOp.java,v 1.7 2003/03/07 23:36:43 vpapad Exp $


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
 * This is the class for the sum operator, that is a type of group
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

public class SumOp extends groupOp {
    // This is the attribute on which summing is done
    Attribute summingAttribute;
    /**
     * This function sets the skolem attributes on which grouping is
     * done, and the attribute that is summed
     *
     * @param skolemAttributes Attributes on which grouping is done
     * @param summingAttribute Attribute on which summing is done
     */

    public void setSummingInfo (skolem skolemAttributes,
				Attribute summingAttribute) {
	this.summingAttribute = summingAttribute;
	this.setSkolemAttributes(skolemAttributes);
    }


    public Attribute getSummingAttribute () {
	return summingAttribute;
    }

    public void dump() {
	System.out.println("SumOp");
	skolemAttributes.dump();
	System.err.println(summingAttribute.getName());
    }
    
    public Op copy() {
        SumOp op = new SumOp();
        op.setSummingInfo(skolemAttributes, summingAttribute);
        return op;
    }

    public int hashCode() {
        return skolemAttributes.hashCode() ^ summingAttribute.hashCode();
    }

    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof SumOp)) return false;
        if (obj.getClass() != SumOp.class) return obj.equals(this);
        SumOp other = (SumOp) obj;
        return skolemAttributes.equals(other.skolemAttributes) &&
                summingAttribute.equals(other.summingAttribute);
    }
    
    public void loadFromXML(Element e, LogicalProperty[] inputProperties)
        throws InvalidPlanException {
        String id = e.getAttribute("id");
        String groupby = e.getAttribute("groupby");
        String sumattr = e.getAttribute("sumattr");

        // Parse the groupby attribute to see what to group on
        Vector groupbyAttrs = new Vector();
        StringTokenizer st = new StringTokenizer(groupby);
        while (st.hasMoreTokens()) {
            String varName = st.nextToken();
            Attribute attr = Variable.findVariable(inputProperties[0], varName);
            groupbyAttrs.addElement(attr);
        }

        Attribute summingAttribute =
            Variable.findVariable(inputProperties[0], sumattr);
        setSummingInfo(new skolem(id, groupbyAttrs), summingAttribute);
    }
}
