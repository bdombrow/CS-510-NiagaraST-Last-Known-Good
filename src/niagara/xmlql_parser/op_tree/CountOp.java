/**********************************************************************
  $Id: CountOp.java,v 1.8 2003/03/07 23:36:43 vpapad Exp $


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


import java.util.*;

import org.w3c.dom.*;

import niagara.connection_server.InvalidPlanException;
import niagara.logical.Variable;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.utils.PEException;
import niagara.xmlql_parser.syntax_tree.*;

public class CountOp extends groupOp {
    // This is the attribute on which counting is done
    Attribute countingAttribute;

    /**
     * This function sets the skolem attributes on which grouping is
     * done, and the attribute that is counted
     *
     * @param skolemAttributes Attributes on which grouping is done
     * @param countAttribute Attribute on which counting is done
     */

    public void setCountingInfo (skolem skolemAttributes,
				Attribute countingAttribute) {
	this.countingAttribute = countingAttribute;

	// Set the skolem attributes in the super class
	this.setSkolemAttributes(skolemAttributes);
    }


    /**
     * This function returns the counting attributes
     *
     * @return Counting attribute of the operator
     */
    public Attribute getCountingAttribute () {
	return countingAttribute;
    }

    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op copy() {
        CountOp cop = new CountOp();
        cop.setCountingInfo(skolemAttributes, countingAttribute);
        return cop;
    }

    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof CountOp)) return false;
        if (obj.getClass() != CountOp.class) return obj.equals(this);
        CountOp other = (CountOp) obj;
        return skolemAttributes.equals(other.skolemAttributes) &&
                countingAttribute.equals(other.countingAttribute);
    }
    
    public int hashCode() {
        return skolemAttributes.hashCode() ^ countingAttribute.hashCode();
    }

    public void loadFromXML(Element e, LogicalProperty[] inputProperties)
        throws InvalidPlanException {
        String id = e.getAttribute("id");
        String groupby = e.getAttribute("groupby");
        String countattr = e.getAttribute("countattr");

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
