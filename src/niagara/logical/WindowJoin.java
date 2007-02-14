/**********************************************************************
  $Id: WindowJoin.java,v 1.2 2007/02/14 03:30:10 jinli Exp $


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
import niagara.xmlql_parser.varType;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class WindowJoin extends Join {

    
    private String leftInputName;
    private String rightInputName;
       
    private Attribute[] wa;
    
    private int[] winSize;

    private Attribute[] wid;
        
    private int interval, delta;
    
    private boolean pane;
    
    private boolean isOOP;
    
    public WindowJoin () {
    	super();
    }
    
    public WindowJoin(
        Predicate pred,
        EquiJoinPredicateList equiJoinPredicates,
        Attrs projectedAttrs, int extensionJoin, 
        boolean isOOP, 
        Attribute[] wid,
        Attribute[] wa,
        int Interval, int delta) {
        super (pred, equiJoinPredicates, projectedAttrs, extensionJoin, null); 
        this.isOOP = isOOP;
        this.wa = wa;
        this.interval = Interval;
        this.wid = wid;
        this.delta = delta;

    }

    /**
     * print the operator to the standard output
     */
    public void dump() {
        System.out.println("WindowJoin : ");
        super.dump();
    }

    /**
     * dummy toString method
     *
     * @return the String representation of the operator
     */
    public String toString() {
        StringBuffer strBuf = new StringBuffer();
        strBuf.append("WindowJoin");

        return strBuf.toString();
    }

    public void dumpChildrenInXML(StringBuffer sb) {
        sb.append(">");
        pred.toXML(sb);
        sb.append("</WindowJoin>");
    }

    public Op opCopy() {
        return new WindowJoin(
            pred,
            equiJoinPredicates,
            (projectedAttrs == null)?null:projectedAttrs.copy(),
            	extensionJoin,
            isOOP, wid, wa, 
            interval, delta);
    }

    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof WindowJoin))
            return false;
        if (obj.getClass() != WindowJoin.class)
            return obj.equals(this);
        WindowJoin other = (WindowJoin) obj;
        return pred.equals(other.pred)
            && equiJoinPredicates.equals(other.equiJoinPredicates)
            && equalsNullsAllowed(projectedAttrs, other.projectedAttrs)
            && (extensionJoin == other.extensionJoin)	&& wa[0].equals(other.wa[0])
            && wa[1].equals(other.wa[1]);
     }

    public int hashCode() {
        return super.hashCode() ^ wa[0].hashCode() ^ wa[1].hashCode();
    }

    public Attrs requiredInputAttributes(Attrs inputAttrs) {
    	Attrs reqd = super.requiredInputAttributes(inputAttrs);
        
    	if (pane) { 
    		reqd.add(wid[0]);
    		reqd.add(wid[1]);
    	}
        
        reqd.add(wa[0]);
        reqd.add(wa[1]);

        return reqd;
    }

    public void loadFromXML(Element e, LogicalProperty[] inputProperties, Catalog catalog)
        throws InvalidPlanException {
        /*String id = e.getAttribute("id");
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
                //Add equi-wid condition; -Jenny
                //leftVars.add(Variable.findVariable(inputProperties[0], "$wid_from_"+leftName));
                //rightVars.add(Variable.findVariable(inputProperties[1], "wid_from_"+rightName));
                
            } catch (REException rx) {
                throw new InvalidPlanException(
                    "Syntax error in equijoin predicate specification for "
                        + id);
            }
        }
        String extensionJoinAttr = e.getAttribute("extensionjoin");
        int extJoin;
        if (extensionJoinAttr.equals("right")) {
            extJoin = Join.RIGHT;
        } else if (extensionJoinAttr.equals("left")) {
            extJoin = Join.LEFT;
        } else if (extensionJoinAttr.equals("none")) {
            extJoin = Join.NONE;
        } else if (extensionJoinAttr.equals("both")) {
            extJoin = Join.BOTH;
        } else {
            throw new InvalidPlanException(
                "Invalid extension join value " + extensionJoinAttr);
        }
        
        setJoin(pred, leftVars, rightVars, extJoin);*/
    	
        super.loadFromXML(e, inputProperties, catalog);
        String inputName = e.getAttribute("input");
        
        String oop = e.getAttribute("oop");
        if (oop.equals("yes")) {
        	isOOP = true;
        } else {
        	isOOP = false;
        }
        
        String deltaAttr = e.getAttribute("delta");
        delta = Integer.parseInt(deltaAttr);
        
        leftInputName = inputName.substring(0, inputName.indexOf(" "));
        leftInputName.trim();
        rightInputName = inputName.substring(inputName.indexOf(" ")+1);
        rightInputName.trim();
        
       	String leftwin = e.getAttribute("leftwin");
       	String rightwin = e.getAttribute("rightwin");
       	
       	wa = new Attribute[2];
       	winSize = new int[2];
       	String[] elts = leftwin.split("[ \t]+");
       	wa[0] = Variable.findVariable(inputProperties[0],  elts[0]);
       	winSize[0] = Integer.parseInt(elts[1]);
       	
       	elts = rightwin.split("[ \t]+");
       	wa[1] = Variable.findVariable(inputProperties[1], elts[0]);
       	winSize[1] = Integer.parseInt(elts[1]);
       	
       	assert (winSize[0] == winSize[1]): 
       		"We currently don't support asymmetric window join - Jenny";
       	interval = winSize[0];
       	
       	String panedWindow = e.getAttribute("pane");
       	
       	if (panedWindow.equals("yes")) {
            wid = new Attribute [2];
            
            wid[0] = 
                Variable.findVariable(
                    inputProperties[0],
                    "wid_from_"+leftInputName);
            wid [1] =
                Variable.findVariable(
                    inputProperties[1],
                    "wid_from_"+rightInputName);
           	delta = Integer.parseInt(e.getAttribute("delta"));
       		pane = true;
       	} else
       		pane = false;
    }
        
    public boolean oop () {
    	return isOOP;
    }
    
    public boolean iop () {
    	return !isOOP;
    }
            
    public Attribute[] getWa () {
    	return wa;
    }
    
    public int getInterval () {
    	return interval;
    }
       
    public int getDelta () {
    	return delta;
    }
    
    public boolean pane () {
    	return pane;
    }
    
    public boolean nonpane () {
    	return !pane;
    }
}
