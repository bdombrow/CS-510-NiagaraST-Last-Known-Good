/**********************************************************************
  $Id: groupOp.java,v 1.6 2003/03/19 22:44:38 tufte Exp $


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

package niagara.xmlql_parser.op_tree;

import java.util.Vector;
import java.util.StringTokenizer;

import org.w3c.dom.Element;

import niagara.logical.Variable;
import niagara.optimizer.colombia.*;
import niagara.xmlql_parser.syntax_tree.*;
import niagara.connection_server.InvalidPlanException;

/**
 * This is the class for the logical group operator. This is an abstract
 * class from which various notions of grouping can be derived. The
 * core part of this class is the skolem function attributes that are
 * used for grouping and are common to all the sub-classes
 *
 */
public abstract class groupOp extends unryOp {
    // The attributes to group on (a.k.a. skolem attributes)
    protected skolem groupingAttrs;

    /**
     * This function returns the skolem attributes associated with the group
     * operator
     *
     * @return The skolem attributes associated with the operator
     */
    public skolem getSkolemAttributes() {
        return groupingAttrs;
    }

    /**
     * @see niagara.optimizer.colombia.LogicalOp#findLogProp(ICatalog, LogicalProperty[])
     */
    public LogicalProperty findLogProp(
        ICatalog catalog,
        LogicalProperty[] input) {
        LogicalProperty inpLogProp = input[0];
        // XXX vpapad: Really crude, fixed restriction factor for groupbys
        float card =
            inpLogProp.getCardinality() / catalog.getInt("restrictivity");
        Vector groupbyAttrs = groupingAttrs.getVarList();
        // We keep the group-by attributes (possibly rearranged)
        // and we add an attribute for the aggregated result
        Attrs attrs = new Attrs(groupbyAttrs.size() + 1);
        for (int i = 0; i < groupbyAttrs.size(); i++) {
            Attribute a = (Attribute) groupbyAttrs.get(i);
            attrs.add(a);
        }
        attrs.add(new Variable(groupingAttrs.getName(), varType.CONTENT_VAR));
        
        return  new LogicalProperty(card, attrs, inpLogProp.isLocal());
    }

    /**
     * create the groupingAttrs object by loading the grouping attributes
     * from an xml element. the grouping attributes are specified in
     * an attribute of element e.
     *
     * @param e The element which contains the grouping attributes 
     *          (as one of its attributes)
     * @param inputLogProp logical properties of the input??
     * @param gpAttrName The attribute of element e which contains
     *                   the grouping attributes.
     */
    protected void loadGroupingAttrsFromXML(Element e,
					    LogicalProperty inputLogProp,
					    String gpAttrName) 
	throws InvalidPlanException {
        String id = e.getAttribute("id");
        String groupbyStr = e.getAttribute(gpAttrName);

        // Parse the groupby attribute to see what to group on
        Vector groupbyAttrs = new Vector();
        StringTokenizer st = new StringTokenizer(groupbyStr);
        while (st.hasMoreTokens()) {
            String varName = st.nextToken();
            Attribute attr = Variable.findVariable(inputLogProp, varName);
            groupbyAttrs.addElement(attr);
        }
	groupingAttrs = new skolem(id, groupbyAttrs);
    }
}
