/**********************************************************************
  $Id: WindowGroup.java,v 1.1 2003/12/24 02:08:29 vpapad Exp $


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

package niagara.logical;

import java.util.Vector;
import java.util.StringTokenizer;

import org.w3c.dom.Element;

import niagara.optimizer.colombia.*;
import niagara.xmlql_parser.*;
import niagara.connection_server.InvalidPlanException;

/**
 * This is the class for the logical group operator. This is an abstract
 * class from which various notions of grouping can be derived. The
 * core part of this class is the skolem function attributes that are
 * used for grouping and are common to all the sub-classes
 *
 */
public abstract class WindowGroup extends Group {
	private final static int SEC_AS_MILLISECS = 1000;
	private final static int MIN_AS_MILLISECS = 60 * SEC_AS_MILLISECS;
	private final static int HOUR_AS_MILLISECS = 60 * MIN_AS_MILLISECS;
	private final static int DAY_AS_MILLISECS = 24 * HOUR_AS_MILLISECS;	
	
	// The attributes to group on (a.k.a. skolem attributes)
	protected skolem groupingAttrs;
	protected Attribute windowAttr;
	protected int windowType;
	protected int range;
	protected int slide;
    

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
		
		Attrs attrs = new Attrs(groupbyAttrs.size() + 1);
		for (int i = 0; i < groupbyAttrs.size(); i++) {
			Attribute a = (Attribute) groupbyAttrs.get(i);
			attrs.add(a);
		}
		//and we add an attribute for the aggregated result
		attrs.add(new Variable(groupingAttrs.getName(), varType.CONTENT_VAR));
		//and we add an attribute for the TIMESTAMP, if it's a time-based window
		//if (windowType == 1)
		//	attrs.add(new Variable("TIMESTAMP", varType.CONTENT_VAR));
		//and we add an attribute for window id - don't need this because wid_from is already a grouping attribute
		//attrs.add(new Variable("wid_from", varType.ELEMENT_VAR));
        
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
		//add the window_from (window_id) to the grouping attributes
		Attribute attr = Variable.findVariable(inputLogProp, "wid_from");
		groupbyAttrs.addElement(attr);
		groupingAttrs = new skolem(id, groupbyAttrs);
	
	//loadWindowAttrsFromXML(e, inputLogProp);
	}
    
	protected void loadWindowAttrsFromXML(Element e, 
						LogicalProperty inputLogProp) 
	throws InvalidPlanException {
	String windowAttribute = e.getAttribute("winattr");
	String type = e.getAttribute("wintype");
	String windowRange = e.getAttribute("range");
	String windowSlide = e.getAttribute("slide");
	
	if (type.length() == 0) {
		range = 0;
		slide = 0;
		windowType = -1;
	}
	else {
		windowType = new Integer(type).intValue();
		if (windowType == 0) {
			range = (Integer.valueOf(windowRange)).intValue();
			slide = (Integer.valueOf(windowSlide)).intValue();
		} else {
			range = parseTimeInterval(windowRange);
			slide = parseTimeInterval(windowSlide);
		}
		
		if(windowAttribute.length() != 0)
			windowAttr = Variable.findVariable(inputLogProp, windowAttribute);					    					
	}			
	}

	public int parseTimeInterval(String intervalStr)
		throws InvalidPlanException {
		StringTokenizer strtok = new StringTokenizer(intervalStr);
		boolean expectNumber = true;
		int currentNumber = 0;
		int total = 0;
		while (strtok.hasMoreTokens()) {
			String tok = strtok.nextToken();
			if (expectNumber) {
				try {
					currentNumber = Integer.parseInt(tok);
					expectNumber = false;
				} catch (NumberFormatException nfe) {
					throw new InvalidPlanException(
						"Expected integer, found "
							+ tok
							+ "while parsing "
							+ id);
				}
			} else {
				tok = tok.toLowerCase();
				if (tok.indexOf("day") >= 0)
					total += currentNumber * DAY_AS_MILLISECS;
				else if (tok.indexOf("hour") >= 0)
					total += currentNumber * HOUR_AS_MILLISECS;
				else if (tok.indexOf("minute") >= 0)
					total += currentNumber * MIN_AS_MILLISECS;
				else if (tok.indexOf("second") >= 0)
					total += currentNumber * SEC_AS_MILLISECS;
				else if (tok.indexOf("millisecond") >= 0)
					total += currentNumber;
				else
					throw new InvalidPlanException(
						"Expected time term, found "
							+ tok
							+ " while parsing "
							+ id);
			}
		}
		return total;
	}
    
    
/*    protected void loadWindowAttrsFromXML(Element e, 
							LogicalProperty inputLogProp) 
		throws InvalidPlanException {
	String windowAttribute = e.getAttribute("winattr");
	String type = e.getAttribute("wintype");
	String windowRange = e.getAttribute("range");
	String windowSlide = e.getAttribute("slide");
	
	if (windowAttribute.length() == 0) {
		range = 0;
		slide = 0;
		windowType = -1;
	}
	else {
		range = new Integer(windowRange).intValue();
		slide = new Integer(windowSlide).intValue();
		windowType = new Integer(type).intValue();
		windowAttr = Variable.findVariable(inputLogProp, windowAttribute);	
		
		// Create a new physical bucket operator object
		PhysicalOperator physicalOperator = new PhysicalBucketOperator();
		
		physicalOperator.initFrom(this);
		    					
	}			
	} */
    
    

/*    
	protected void setWindowAttr(Attribute attr) {
		windowAttr = attr;
	}
	protected void setWindowType(int type) {
		windowType = type;
	}
	protected void setWindowRange(int range) {
		this.range = range;
	}
	protected void setWindowSlide(int slide) {
		this.slide = slide;
	}*/
	public Attribute getWindowAttr() {
		return windowAttr;
	}
	public int getWindowType() {
		return windowType;
	}
	public int getWindowRange() {
		return range;
	}
	public int getWindowSlide() {
		return slide;
	}
    
}
