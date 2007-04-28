/**********************************************************************
  $Id: Present.java,v 1.1 2007/04/28 21:24:48 jinli Exp $


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
 * This class is used to represent the join operator.
 *
 */
package niagara.logical;

import gnu.regexp.RE;
import gnu.regexp.REException;
import gnu.regexp.REMatch;

import java.util.ArrayList;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.logical.predicates.*;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class Present extends BinaryOperator {

    /** The attributes we're projecting on (null means keep all attributes) */
    protected Attrs projectedAttrs;

    protected int extensionJoin;
    
    private Attrs[] punctAttrs = null;

    public Present() {

    }

    public Present (Attrs[] punctAttrs) {
        this.punctAttrs = punctAttrs;
    }

    /**
     * print the operator to the standard output
     */
    public void dump() {
        System.out.println("Present : ");
    }

    /**
     * dummy toString method
     *
     * @return the String representation of the operator
     */
    public String toString() {
        StringBuffer strBuf = new StringBuffer();
        strBuf.append("Present");

        return strBuf.toString();
    }

    /*public void dumpChildrenInXML(StringBuffer sb) {
        sb.append(">");
        pred.toXML(sb);
        sb.append("</join>");
    }*/

    /**
     * @see niagara.optimizer.colombia.LogicalOp#findLogProp(ICatalog, LogicalProperty[])
     */
    public LogicalProperty findLogProp(
        ICatalog catalog,
        LogicalProperty[] input) {
        LogicalProperty left = input[0];
        LogicalProperty right = input[1];

        LogicalProperty result = input[0].copy();
        return result;

    }

    
    public Op opCopy() {
        return new Present(punctAttrs);
    }

    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof Present))
            return false;
        if (obj.getClass() != Present.class)
            return obj.equals(this);
        Present other = (Present) obj;
        
        if ((punctAttrs==null) ^ (other.punctAttrs==null))
        	return false;
        
		return punctAttrs[0].equals(other.punctAttrs[0]) && 
				punctAttrs[1].equals(other.punctAttrs[1]);
    }

    public int hashCode() {
        return punctAttrs[0].hashCode() ^ punctAttrs[1].hashCode();
    }

    public Attrs requiredInputAttributes(Attrs inputAttrs) {
        Attrs reqd = new Attrs();
        for (int i = 0; i<punctAttrs[0].size(); i++)
        	reqd.add(punctAttrs[0].get(i));
        
        for (int i = 0; i<punctAttrs[1].size(); i++)
        	reqd.add(punctAttrs[1].get(i));
        
        assert inputAttrs.contains(reqd);
        return reqd;
    }

    public Attrs getProjectedAttrs() {
        return projectedAttrs;
    }

    public void loadFromXML(Element e, LogicalProperty[] inputProperties, Catalog catalog)
        throws InvalidPlanException {
        String id = e.getAttribute("id");

        NodeList children = e.getChildNodes();
        Element predElt = null;

        for (int i = 0; i < children.getLength(); i++) {
            if (children.item(i) instanceof Element) {
                predElt = (Element) children.item(i);
                break;
            }
        }

        Predicate pred = Predicate.loadFromXML(predElt, inputProperties);

        // In case of an equijoin we have to parse "left" 
        // and "right" to get additional equality predicates
        String leftattrs = e.getAttribute("left");
        String rightattrs = e.getAttribute("right");

        ArrayList leftVars = new ArrayList();
        ArrayList rightVars = new ArrayList();

        if (leftattrs.length() > 0) {
            try {
                RE re = new RE("(\\$)?[a-zA-Z0-9_]+");
                REMatch[] all_left = re.getAllMatches(leftattrs);
                REMatch[] all_right = re.getAllMatches(rightattrs);
                for (int i = 0; i < all_left.length; i++) {
                    Attribute leftAttr =
                        Variable.findVariable(
                            inputProperties[0],
                            all_left[i].toString());
                    Attribute rightAttr =
                        Variable.findVariable(
                            inputProperties[1],
                            all_right[i].toString());
                    leftVars.add(leftAttr);
                    rightVars.add(rightAttr);
                }
            } catch (REException rx) {
                throw new InvalidPlanException(
                    "Syntax error in equijoin predicate specification for "
                        + id);
            }
        }
        
        String punctattr = e.getAttribute("punctattr");
        if (punctattr=="")
        	return;
        
        String[] punctAttrVals;
        punctAttrVals = punctattr.split("[\t| ]+");
        
        if (punctAttrVals.length != 2) {
        	throw new InvalidPlanException("Present needs punctattr from each side of input");
        }
        
        punctAttrs = new Attrs[2];
        punctAttrs[0] = new Attrs(Variable.findVariable(inputProperties[0], punctAttrVals[0]));
        punctAttrs[1] = new Attrs(Variable.findVariable(inputProperties[1], punctAttrVals[1]));
    }
    public Attrs[] getPunctAttrs() {
    	return punctAttrs;
    }
    
}
