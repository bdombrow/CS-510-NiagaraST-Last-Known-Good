
/**********************************************************************
  $Id: UnionOp.java,v 1.9 2003/07/27 02:45:30 tufte Exp $


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

import java.util.*;
import java.lang.reflect.Array;

import org.w3c.dom.Element;

import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.Domain;
import niagara.optimizer.colombia.Attribute;
import niagara.logical.Variable;
import niagara.utils.DOMHelper;

/**
 * This class is used to represent the Union operator.
 */
public class UnionOp extends op {

    private int arity;
    private Attrs outputAttrs;
    private Attrs[] inputAttrs;
    private int numMappings;
    
    public void setArity(int arity) {
        this.arity = arity;
    }
     
    public int getArity() {
        return arity;
    }

    public Attrs getOutputAttrs() {
	return outputAttrs;
    }

    public Attrs[] getInputAttrs() {
	return inputAttrs;
    }

    public int numMappings() {
	return numMappings;
    }
       
   /**
    * print the operator to the standard output
    */
   public void dump() {
      System.out.println(this);
   }

   /**
    *
    * @return the String representation of the operator
    */
   public String toString() {
      return "Union";
   }

    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op opCopy() {
        UnionOp op = new UnionOp();
        op.setArity(arity);
	op.inputAttrs = inputAttrs;
	op.outputAttrs = outputAttrs;
	op.numMappings = numMappings;
        return op;
    }
    
    /**
     * @see niagara.optimizer.colombia.LogicalOp#findLogProp(ICatalog, ArrayList)
     */
    public LogicalProperty findLogProp(ICatalog catalog, 
				       LogicalProperty[] input) {
        // We only propagate the variables from the first input
        // XXX vpapad: We should be very careful when pushing
        // project through union: before projecting out a variable from
        // the first input we should make sure to project out the
        // respective variables from all the other inputs

	// KT - change to support mappings - loadFromXML is
	// always called before this function
	if(numMappings == 0) {
	    return input[0].copy();
	} else {
	    LogicalProperty outLogProp = 
		new LogicalProperty(numMappings, outputAttrs, true);

	    // set up remote and local access
	    boolean hasLocal = false;
	    boolean hasRemote = false;
	    for(int i = 0; i<arity && 
		    (hasLocal == false || hasRemote == false); i++) {
		if(input[i].hasRemote())
		    hasRemote = true;
		if(input[i].hasLocal())
		    hasLocal = true;
	    }
	    outLogProp.setHasRemote(hasRemote);
	    outLogProp.setHasLocal(hasLocal);
	    return outLogProp;
	}
    }

    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof UnionOp)) return false;
        if (obj.getClass() != UnionOp.class) return obj.equals(this);
        UnionOp other = (UnionOp) obj;
        return arity == other.arity &&
	    numMappings == other.numMappings &&
	    outputAttrs.equals(other.outputAttrs) &&
	    inputAttrs.equals(other.inputAttrs);
    }

    public int hashCode() {
	if(numMappings > 0)
	    return arity ^
		outputAttrs.hashCode() ^
		inputAttrs.hashCode();
	else
	    return arity;
    }
    
   
    /**
     * @see niagara.optimizer.colombia.Op#matches(Op)
     */
    public boolean matches(Op other) {
        if (arity == 0) // Special case arity = 0 => match any Union
            return (other instanceof UnionOp);
        return super.matches(other);
    }

    public void loadFromXML(Element e, LogicalProperty[] inputProperties)
        throws InvalidPlanException {
        arity = inputProperties.length;
	
	outputAttrs = new Attrs(); // len will be numMappings, if have mappings
	inputAttrs = new Attrs[arity];

	// get first child that is an element
	Element mapping = DOMHelper.getFirstChildElement(e);

	if(mapping == null) {
	    numMappings = 0;
	    // no mapping, verify union compatibility
	    for (int i = 0; i < arity; i++) {
		if (inputProperties[i].getDegree()
		    != inputProperties[0].getDegree()) {
		    throw new InvalidPlanException("Union inputs are not union-compatible and no mapping specified");
		}
	    }
	} else { // have mapping
	    assert mapping.getNodeName().equals("mapping");
	    numMappings = 1;
	    while(mapping != null) {
		String outputAttrName = mapping.getAttribute("outputattr");
		String[] inputAttrNames = 
		    parseInputAttrs(mapping.getAttribute("inputattrs"));

		if(arity != Array.getLength(inputAttrNames))
		    throw new InvalidPlanException("Bad arity in mapping");

		Domain outputDom = null;
		for(int i = 0; i<arity; i++) {
		    // Get attribute for input - this also serves
		    // to verify that the input attr is valid
		    // findVariable throws InvalidPlan if it can't find variable
		    Attribute attr;
		    if(inputAttrNames[i].equalsIgnoreCase("NONE")) {
		    	attr = null;
		    } else {
		    	attr = Variable.findVariable(inputProperties[i], 
										      inputAttrNames[i]);
		    }
		    if(inputAttrs[i] == null)
			inputAttrs[i] = new Attrs();
		    inputAttrs[i].add(attr);
		    if(outputDom == null && attr != null) {
			    outputDom = attr.getDomain();
		    } else {
			if(attr != null && outputDom != null &&
			       !outputDom.equals(attr.getDomain())) {}
			    //throw new InvalidPlanException("Input types are not union compatible");
		    }
		}
		outputAttrs.add(new Variable(outputAttrName, outputDom));
		// get next sibling, but get only elements
		mapping = DOMHelper.getNextSiblingElement(mapping);
		numMappings++;
	    }
	}
    }

    private String[] parseInputAttrs(String inputAttrString) {
	StringTokenizer tokenizer = new StringTokenizer(inputAttrString, ", ");
	String[] parsedAttrs = new String[tokenizer.countTokens()];
	int i = 0;
	while(tokenizer.hasMoreTokens()) {
	    parsedAttrs[i] = tokenizer.nextToken();
	    i++;
	}
	return parsedAttrs;
    }
}
