
/**********************************************************************
  $Id: OrderOp.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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


//OrderOp.java
package niagara.search_engine.operators;

import java.util.*;
import java.io.*;
import java.net.*;

import niagara.search_engine.indexmgr.*;
import niagara.search_engine.seql.sym;

/** 
 * OrderOp handles the comparison between a numeric element and a value. 
 * The comparison symbols can be >, >=, <, <=, or = .
 *
 * OrderOp takes an IVL of a numeric element and a numeric value as operands
 * It returns an IVL of qualified IVL entries of that element.
 * 
 */

public class OrderOp extends AbstractOperator 
{
    private IndexMgr indexMgr = null;
    
    private double value;
   
    String attr;

    int op;

    public OrderOp(Vector operands) 
    {
	super(operands);
	indexMgr = (IndexMgr)operands.elementAt(0);
	attr = (String)operands.elementAt(1);
	op = ((Integer)operands.elementAt(2)).intValue();	
	value = ((Double)operands.elementAt(3)).doubleValue();
    }
    
    public OrderOp(String attr, int op, double val,IndexMgr idxmgr) 
    {
      this.attr = attr;
      this.op = op;
      this.value = val;
      this.indexMgr = idxmgr;
    }

    public String toString() {
      String opstr = null;
     
      switch(op) {
      case sym.EQ:
	  opstr = "="; break;
      case sym.LT:
	  opstr = "<"; break;
      case sym.GT:
	  opstr = ">"; break;
      case sym.LEQ:
	  opstr = "<="; break;
      case sym.GEQ:
	  opstr = ">="; break;
      }
    
      return "OrderOp("+attr+opstr+value+")";
    }

    /* scan the textLexicon to find all qualified value IVLs */
    private Vector getValueIVLs(int op, double value)
    {
	return indexMgr.getIVLs(op,value);
    }
    
    public void evaluate() throws IMException 
    {
	isEvaluated = true;
	
	/* The IVL of the elment */
	Vector elemIVL = indexMgr.getInvertedList(attr, true);
	
	if (elemIVL == null) {
	    System.out.println("Null IVLs");	    
	    return;
	}
	
	/* a vector of all qualified value IVLs */ 
	Vector valueIVLs = getValueIVLs(op,value);

	if (valueIVLs == null) {
	    return;
	}
	
	//The first contain operator
	ContainOp containOp = new ContainOp();

	//operands for the first contain op
	Vector contOperands = new Vector();  

	//the container is always the elment IVL
	contOperands.addElement(elemIVL);    

	//At first the containee is the first value IVL 
	contOperands.addElement((Vector)valueIVLs.elementAt(0));

	containOp.addIVLs(contOperands);
	
	//the result of the first containOp as the leftIVL of OR op.
	Vector leftIVL = containOp.getResult();

 	//running result of OR ops
	Vector targetIVL = leftIVL;
	
	//deal with the rest value IVLs
	for (int i = 1; i< valueIVLs.size();i++) {

	    //the next contain operator
	    ContainOp contain2Op = new ContainOp();
	    
	    Vector cont2Operands = new Vector();
	    
	    //the container is always the elment IVL
	    cont2Operands.addElement(elemIVL);    

	    //the containee is the current value IVL 
	    cont2Operands.addElement((Vector)valueIVLs.elementAt(i));
	    
	    contain2Op.addIVLs(cont2Operands);
	    
	    Vector rightIVL = contain2Op.getResult();
	    
	    OROp orOp = new OROp();
	    
	    //operands for OR ops
	    Vector orOperands = new Vector();    

	    orOperands.addElement(leftIVL);

	    orOperands.addElement(rightIVL);
	    
	    orOp.addIVLs(orOperands);
	    
	    targetIVL = orOp.getResult();
	    	    
	    leftIVL = targetIVL;
	}

	resultIVL = targetIVL;	

    }  //end of evaluate()    

    /**
     * Test driver
     */
    public static void main(String args[]) 
    {
	try {
	    /*
	    IndexMgr.idxmgr.index (new URL
		("http://www.cs.wisc.edu/~czhang/xml/bib_1.xml"));
	    IndexMgr.idxmgr.index (new URL
		("http://www.cs.wisc.edu/~czhang/xml/bib_3.xml"));
	    */
	    IndexMgr.idxmgr.index
		("/u/c/z/czhang/public/html/xml/bib_1.xml", null, null);
	    
	    IndexMgr.idxmgr.index
		("/u/c/z/czhang/public/html/xml/bib_3.xml", null, null);
 	     	    
	    IndexMgr.idxmgr.index
		("/u/c/z/czhang/public/html/xml/bib_4.xml", null, null);
	    /*
	    OrderOp orderOp = new OrderOp
		("price", sym.EQ,20.83,IndexMgr.idxmgr);
	    */
	    OrderOp orderOp = new OrderOp
		("price", sym.GT,90,IndexMgr.idxmgr);
	    orderOp.evaluate();
	    orderOp.printResult();
	    IndexMgr.idxmgr.flush();	    
	}
	catch (Exception e) {
	    e.printStackTrace();
	}
    }
    
}

