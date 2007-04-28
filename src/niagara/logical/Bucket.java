/**********************************************************************
  $Id: Bucket.java,v 1.5 2007/04/28 21:26:13 jinli Exp $


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
 * The class <code>dupOp</code> is the class for operator Duplicate.
 * 
 * @version 1.0
 *
 * @see op 
 */
package niagara.logical;

import org.w3c.dom.Element;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.*;
import niagara.xmlql_parser.*;

import java.util.StringTokenizer;

public class Bucket extends UnaryOperator {
	protected Attribute windowAttr;
	protected int windowType;
	protected String widName;
	protected int range;
	protected int slide;	
	protected String stInput;
	private String name;
	private long starttime;

    public void dump() {
	System.out.println("BucketOp");
    }
   
    public Op opCopy() {
	Bucket op = new Bucket();
	op.windowAttr = windowAttr;
	op.windowType = windowType;
	op.range = range;
	op.slide = slide;
	op.starttime = starttime;
	//op.attrInput = attrInput;
	op.stInput = stInput;
	op.name = name;
	op.widName = widName;
	return op;
    }
    
    public int hashCode() {
	return range ^ slide ^ windowType ^ windowAttr.hashCode() ^ hashCodeNullsAllowed(stInput);
    }
    
    public boolean equals(Object obj) {
	if (obj == null || !(obj instanceof Bucket)) return false;
	if (obj.getClass() != Bucket.class) return obj.equals(this);
	Bucket other = (Bucket) obj;
	if (windowAttr != null) 
		if (!windowAttr.equals(other.windowAttr))
			return false;

	if (stInput != null)
		if(!stInput.equals(other.stInput))
			return false;		
	if (widName != null)
		if(!widName.equals(other.widName))
			return false;
	return (windowType == other.windowType) &&
		(range == other.range) &&
		(slide == other.slide) && (starttime == other.starttime);
    }

    protected void loadWindowAttrsFromXML(Element e, 
						LogicalProperty inputLogProp) 
	throws InvalidPlanException {
	String windowAttribute = e.getAttribute("winattr");
	String type = e.getAttribute("wintype");
	String windowRange = e.getAttribute("range");
	String windowSlide = e.getAttribute("slide");
	widName = e.getAttribute("wid");
	
	if (widName.length() == 0)
		widName = name;
	
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
			range = Timer.parseTimeInterval(windowRange);
			slide = Timer.parseTimeInterval(windowSlide);
		}
		
		if(windowAttribute.length() != 0)
			windowAttr = Variable.findVariable(inputLogProp, windowAttribute);					    					
	}			
    }
    
    public void loadFromXML(Element e, LogicalProperty[] inputProperties, Catalog catalog)    
	throws InvalidPlanException {
		name = e.getAttribute("id");
		stInput = e.getAttribute("input");
		if (e.getAttribute("start") != "")
			starttime = Long.valueOf(e.getAttribute("start"));
		else
			starttime = Long.MIN_VALUE;
		loadWindowAttrsFromXML(e, inputProperties[0]);
    }
    
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
    public String getInput() {
    	return stInput;
    }
    public String getWid () {
    	return widName;
    }
    public long getStarttime () {
    	return starttime;
    }
    /**
     * 
     * @see niagara.optimizer.colombia.LogicalOp#findLogProp(ICatalog, LogicalProperty[])
     */
    
	public LogicalProperty findLogProp(
		ICatalog catalog,
		LogicalProperty[] input) {
		
		LogicalProperty inpLogProp = input[0];
		// XXX vpapad: Really crude, fixed restriction factor for groupbys
		float card =
			inpLogProp.getCardinality() / catalog.getInt("restrictivity");

		LogicalProperty result = inpLogProp.copy();
		if (!widName.equals("")) {
			result.addAttr(new Variable("wid_from_"+widName, varType.ELEMENT_VAR));
			result.addAttr(new Variable("wid_to_"+widName, varType.ELEMENT_VAR));
		} else {
			result.addAttr(new Variable("wid_from_"+name, varType.ELEMENT_VAR));
			result.addAttr(new Variable("wid_to_"+name, varType.ELEMENT_VAR));
		}
		return result;

		//Vector groupbyAttrs = groupingAttrs.getVarList();
		// We keep the group-by attributes (possibly rearranged)
		// and we add an attribute for the aggregated result
		/*Attrs data = inpLogProp.getAttrs();
		Attrs attrs = new Attrs(data.size() + 1);
		for (int i = 0; i < data.size(); i++) {			
			Attribute a = (Attribute) data.get(i);
			attrs.add(a);
		}
		
		if (!widName.equals("")) {
			attrs.add(new Variable("wid_from_"+widName, varType.ELEMENT_VAR));
			attrs.add(new Variable("wid_to_"+widName, varType.ELEMENT_VAR));			
		} else {
			attrs.add(new Variable("wid_from_"+name, varType.ELEMENT_VAR));
			attrs.add(new Variable("wid_to_"+name, varType.ELEMENT_VAR));
		}
        
		return  new LogicalProperty(card, attrs, inpLogProp.isLocal());*/
	}
}

