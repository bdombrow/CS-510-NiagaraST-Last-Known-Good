/**********************************************************************
  $Id: Construct.java,v 1.1 2003/12/24 02:08:30 vpapad Exp $


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
package niagara.logical;

import java.io.StringReader;
import org.w3c.dom.*;

import niagara.utils.XMLUtils;
import niagara.xmlql_parser.*;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.*;

public class Construct extends UnaryOperator {
    private Variable variable;

    constructBaseNode resultTemplate; // internal node or leaf node
    // if it is the internal node, then
    // all its children are leaf node that
    // represents the schemaAttributes

    /** The attributes we're projecting on (null means keep all attributes) */
    private Attrs projectedAttrs;

    public Construct() {
    }

    public Construct(Variable variable, constructBaseNode resultTemplate, Attrs projectedAttrs) {
        this.variable = variable;
        this.resultTemplate = resultTemplate;
        this.projectedAttrs = projectedAttrs;
    }

    public Construct(Construct op) {
        this(op.variable, op.resultTemplate, op.projectedAttrs);
    }

    public Op opCopy() {
        return new Construct(this);
    }

    /**
     * @see java.lang.Object#equals(Object)
     */
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof Construct))
            return false;
        if (obj.getClass() != Construct.class)
            return obj.equals(this);
        Construct other = (Construct) obj;
        return equalsNullsAllowed(variable, other.variable)
            && equalsNullsAllowed(projectedAttrs, other.projectedAttrs)
        // XXX vpapad: constructBaseNode.equals is still Object.equals
        && this.resultTemplate.equals(other.resultTemplate);
    }

    public int hashCode() {
        // XXX vpapad: constructBaseNode.hashCode is still Object.hashCode
        return hashCodeNullsAllowed(variable)
            ^ hashCodeNullsAllowed(projectedAttrs)
            ^ resultTemplate.hashCode();
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

    public LogicalProperty findLogProp(
        ICatalog catalog,
        LogicalProperty[] input) {
        LogicalProperty result = input[0].copy();
        // XXX vpapad: We don't have a way yet to estimate what the 
        // cardinality will be, assume same as input cardinality,
        // which is only true as long as you don't have skolems etc.
        if (projectedAttrs == null) 
            result.addAttr(variable);
        else
            result.setAttrs(projectedAttrs);
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

    /**
     * @see niagara.xmlql_parser.op_tree.op#projectedOutputAttributes(Attrs)
     */
    public void projectedOutputAttributes(Attrs outputAttrs) {
        projectedAttrs = outputAttrs;
    }

    /**
     * @see niagara.xmlql_parser.op_tree.op#requiredInputAttributes(Attrs)
     */
    public Attrs requiredInputAttributes(Attrs inputAttrs) {
        return resultTemplate.requiredInputAttributes(inputAttrs);
    }

    public Attrs getProjectedAttrs() {
        return projectedAttrs;
    }

    public void loadFromXML(Element e, LogicalProperty[] inputProperties, Catalog catalog)
        throws InvalidPlanException {
        variable = new Variable(e.getAttribute("id"));

        NodeList children = e.getChildNodes();
        String content = "";
        for (int i = 0; i < children.getLength(); i++) {
            int nodeType = children.item(i).getNodeType();
            if (nodeType == Node.ELEMENT_NODE)
                content += XMLUtils.explosiveFlatten(children.item(i));
            else if (nodeType == Node.CDATA_SECTION_NODE)
                content += children.item(i).getNodeValue();
        }

        Scanner scanner;
        resultTemplate = null;

        try {
            scanner = new Scanner(new StringReader(content));
            ConstructParser cep = new ConstructParser(scanner);
            resultTemplate = (constructBaseNode) cep.parse().value;
            cep.done_parsing();
        } catch (Exception ex) {
            throw new InvalidPlanException("Error while parsing: " + content);
        }
    }
}
