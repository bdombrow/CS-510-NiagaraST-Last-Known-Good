/**********************************************************************
  $Id: constructOp.java,v 1.6 2002/10/26 04:30:28 vpapad Exp $


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
 * This operator is used to construct XML results. This is analogous to SELECT
 * of SQL.
 *
 */
package niagara.xmlql_parser.op_tree;

import java.util.*;
import niagara.xmlql_parser.syntax_tree.*;

import niagara.logical.NodeDomain;
import niagara.logical.Variable;
import niagara.optimizer.colombia.*;

public class constructOp extends unryOp {
        private Variable variable;
        
	constructBaseNode resultTemplate; // internal node or leaf node
					  // if it is the internal node, then
					  // all its children are leaf node that
					  // represents the schemaAttributes

    public constructOp() {}
                          
    public constructOp(Variable variable, constructBaseNode resultTemplate) {
        this.variable = variable;
        this.resultTemplate = resultTemplate;
    }
    
    public constructOp(constructOp op) {
        this(op.variable, op.resultTemplate);
    }
    
    public Op copy() {
        return new constructOp(this);
    }
    
    /**
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof constructOp)) return false;
        if (obj.getClass() != constructOp.class) return obj.equals(this);
        constructOp other = (constructOp) obj;
        return this.variable.equals(other.variable) 
            // XXX vpapad: constructBaseNode.equals is still Object.equals
            && this.resultTemplate.equals(other.resultTemplate);
    }

    public int hashCode() {
        // XXX vpapad: constructBaseNode.hashCode is still Object.hashCode
        return (variable == null)?0:variable.hashCode() ^ resultTemplate.hashCode();
    }

	/**
	 * @return the constructNode that has information about the tag names
	 *         and children
	 */
	public constructBaseNode getResTemp() {
		return resultTemplate;
	}

	/**
	 * used to set parameter for the construct operator
	 *
	 * @param the construct part (tag names and children if any)
	 */
	public void setConstruct(constructBaseNode temp) {
		resultTemplate = temp;
	}
    
    // XXX vpapad: hack to get CVS to compile
    public void setConstruct(constructBaseNode temp, boolean clear) {
        setConstruct(temp);
    }

    /**
     * print the operator to the standard output
     */
    public void dump() {
	System.out.println("Construct : ");
	resultTemplate.dump(1);
    }

	/**
	 * a dummy toString method
	 *
	 * @return String representation of this operator
	 */
	public String toString() {
	   return "ConstructOp";
        }

    public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
        LogicalProperty result = input[0].copy();
        // XXX vpapad: We don't have a way yet to estimate what the 
        // cardinality will be, assume same as input cardinality.
        result.addAttr(variable);
        return result;
    }
    
    public void setVariable(Variable variable) {
        this.variable = variable;
    }
        /**
         * Returns the variable.
         * @return Variable
         */
        public Variable getVariable() {
            return variable;
        }
}
