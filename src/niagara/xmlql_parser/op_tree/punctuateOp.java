/**********************************************************************
  $Id: punctuateOp.java,v 1.5 2003/07/18 00:57:04 tufte Exp $


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
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
public class punctuateOp extends binOp {

    //The global timestamp attribute
    public final static String STTIMESTAMPATTR = "TIMESTAMP";

    regExp re = null;

    //Track which input keeps the timer, and which keeps the data.
    // Default to 1.
    private int iDataInput = 1;
    //The timer value attribute
    private Attribute attrTimer;
    //The data value corresponding to the timer value
    private Attribute attrDataTimer;

    public punctuateOp() {
    }

    public punctuateOp(int iDI, Attribute aTimer,Attribute aDT) { 
	this.iDataInput = iDI;
	this.attrTimer = aTimer;
	this.attrDataTimer = aDT;
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

    public Attribute getDataTimer() {
	return attrDataTimer;
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
        result.setHasRemote(timer.hasRemote() || data.hasRemote());

        // The output schema is exactly the schema of the data input
	//  plus the TIMESTAMP attribute

	result.addAttr(new Variable(STTIMESTAMPATTR, varType.CONTENT_VAR));
	
        return result;
    }

    public Op opCopy() {
        return new punctuateOp(this.iDataInput, this.attrTimer,
			       this.attrDataTimer);
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

	stAttr = e.getAttribute("dataattr");
	if (stAttr.length() == 0)
	    throw new InvalidPlanException("Invalid datattr: " + id);
	//If they want the system timestamp to be punctuated,
	// leave this null
	if (stAttr.endsWith(STTIMESTAMPATTR) == false)
    	    attrDataTimer =
	        Variable.findVariable(inputProperties[iDataInput], stAttr);

    }
}
