/**********************************************************************
  $Id: SlidingWindowOp.java,v 1.2 2003/02/05 21:46:03 jinli Exp $


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

import niagara.logical.Variable;
import niagara.optimizer.colombia.*;
import niagara.xmlql_parser.syntax_tree.*;

/**
 * This is the class for the logical group operator. This is an abstract
 * class from which various notions of grouping can be derived. The
 * core part of this class is the skolem function attributes that are
 * used for grouping and are common to all the sub-classes
 *
 */
public abstract class SlidingWindowOp extends groupOp {
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
        Vector groupbyAttrs = skolemAttributes.getVarList();
        // We keep the group-by attributes (possibly rearranged)
        // and we add an attribute for the aggregated result
        Attrs attrs = new Attrs(groupbyAttrs.size() + 1);
        for (int i = 0; i < groupbyAttrs.size(); i++) {
            Attribute a = (Attribute) groupbyAttrs.get(i);
            attrs.add(a);
        }
        attrs.add(new Variable(skolemAttributes.getName(), varType.CONTENT_VAR));
        attrs.add(new Variable("index", varType.CONTENT_VAR));
        
        return  new LogicalProperty(card, attrs, inpLogProp.isLocal());
    }
}
