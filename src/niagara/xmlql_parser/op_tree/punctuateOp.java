/**********************************************************************
  $Id: punctuateOp.java,v 1.3 2003/07/08 02:11:06 tufte Exp $


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
package niagara.xmlql_parser.op_tree;

import org.w3c.dom.Element;
import niagara.connection_server.InvalidPlanException;
import niagara.xmlql_parser.syntax_tree.*;
import niagara.logical.Variable;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
public class punctuateOp extends binOp {

    regExp re = null;

    //Track which input keeps the timer, and which keeps the data.
    // Default to 1.
    private int iDataInput = 1;
    //The timer value attribute
    private Attribute attrTimer;
    //The data value corresponding to the timer value
    private String stDataTimer;
    //The data root attribute
    private Attribute attrDataRoot;

    private Attrs projectedAttrs;

    public punctuateOp() {
    }

    public punctuateOp(int iDI, Attribute aTimer, String stDT,
                       Attribute aDRoot) {
	this.iDataInput = iDI;
	this.attrTimer = aTimer;
	this.stDataTimer = stDT;
	this.attrDataRoot = aDRoot;
    }

    /**
     * print the operator to the standard output
     */
    public void dump() {
        System.out.println("Punctuate : ");
    }

    /**
     * dummy toString method
     *
     * @return the String representation of the operator
     */
    public String toString() {
        StringBuffer strBuf = new StringBuffer();
        strBuf.append("Punctuate");

        return strBuf.toString();
    }

    public int getDataInput() {
	return iDataInput;
    }

    public Attribute getTimerAttr() {
	return attrTimer;
    }

    public String getDataTimer() {
	return stDataTimer;
    }

    public Attribute getDataRoot() {
        return attrDataRoot;
    }

    public void dumpAttributesInXML(StringBuffer sb) {

    }
    public void dumpChildrenInXML(StringBuffer sb) {
        sb.append(">");

        sb.append("</punctuate>");
    }

    /**
     * @see niagara.optimizer.colombia.LogicalOp#findLogProp(ICatalog, LogicalProperty[])
     */
    public LogicalProperty findLogProp(
        ICatalog catalog,
        LogicalProperty[] input) {
        LogicalProperty timer = input[1-iDataInput];
        LogicalProperty data = input[iDataInput];

        LogicalProperty result = data.copy();

        result.setHasLocal(timer.hasLocal() || data.hasLocal());
        result.setHasRemote(data.hasRemote() || data.hasRemote());

        // The output schema is exactly the schema of the data input
        return result;
    }

    public Op opCopy() {
        return new punctuateOp(this.iDataInput, this.attrTimer,
			       this.stDataTimer, this.attrDataRoot);
    }

    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof punctuateOp))
            return false;
        if (obj.getClass() != punctuateOp.class)
            return obj.equals(this);

	return false;
    }

    public int hashCode() {
        return System.identityHashCode(this);
    }

    public void loadFromXML(Element e, LogicalProperty[] inputProperties)
        throws InvalidPlanException {
        String id = e.getAttribute("id");

	String stDataInput = e.getAttribute("datainput");
	if (stDataInput.length() != 0)
	    iDataInput = Integer.parseInt(stDataInput);

	String stAttr = e.getAttribute("timer");
	if (stAttr.length() == 0)
	    throw new InvalidPlanException("Bad value for 'timer' for : "
					   + id);
	attrTimer = Variable.findVariable(inputProperties[1 - iDataInput],
					  stAttr);

	stDataTimer = e.getAttribute("dataattr");

	stAttr = e.getAttribute("dataroot");
	if (stAttr.length() == 0)
	   throw new InvalidPlanException("Invalid root for :" + id);
	   attrDataRoot = Variable.findVariable(inputProperties[iDataInput],
						stAttr);
    }
}
