
/**********************************************************************
  $Id: TestOps.java,v 1.2 2003/07/08 02:12:09 tufte Exp $


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


//TestOps.java
package niagara.search_engine.operators;

import java.util.*;
import niagara.search_engine.indexmgr.*;

/**
 * test driver for various opreators 
 */

public class TestOps 
{
    
    public TestOps() throws IMException
    {
	//indexMgr = new IndexMgr();
    }

    /**
     * A Tuple is a vector.  The first element is a string of url, and
     * the second element is a vector of qualified strings.
     */
    /*   
    public Tuple getTuple(IVLEntry ivlEntry) 
    {
	Tuple tuple = new Tuple();
	int docno = ivlEntry.getDocNo();
	
	String docName = (IndexMgr.idxmgr).getDocName(docno);

	//doc name as the first element. 
	tuple.addElement(docName);

	//contents will be the second element of the tuple
	Vector contents = new Vector();

	Vector poslist = ivlEntry.getPositionList();
	contents.addElement(IndexMgr.idxmgr.retrieve(docno,poslist));
	tuple.addElement(contents);

	return tuple;
	
    }  //end of getTuple()
      
    public Table getTable(Vector ivl) 
    {
	Table tb = new Table();
	//(tb.header).addElement("docURL");
	//(tb.header).addElement("content");
	
	for (int i = 0; i< ivl.size(); i++) {
	    IVLEntry ivlEntry = (IVLEntry)ivl.elementAt(i);
	    Tuple tuple = getTuple(ivlEntry);
	    tb.addTuple(tuple);
	}
	return tb;
    }
    
*/
    /** evaluate String operator and return the result IVL
     */
    public Vector evalStringOp(String str) throws IMException 
    {
	/* parameters of the operator */
	Vector parameters = new Vector();
	parameters.addElement(IndexMgr.idxmgr);

	parameters.addElement(str);
	    
	StringOp stringOp = new StringOp(parameters);
	stringOp.evaluate();

	System.out.println(stringOp.getOpType());
	
	//stringOp.printResult();

	return stringOp.getResult();
    }
    
    public Vector evalContainOp(Vector containerIVL, Vector containeeIVL)
     throws IMException
    {
	/* parameters of the operator */
	Vector parameters = new Vector();
	parameters.addElement(containerIVL);
	parameters.addElement(containeeIVL);

	ContainOp containOp = new ContainOp(parameters);
	containOp.evaluate();
	
	System.out.println(containOp.getOpType());
	
	containOp.printResult();

	return containOp.getResult();
	       	    
    }
    public Vector evalContainedOp(Vector containeeIVL, Vector containerIVL)
     throws IMException
    {
	/* parameters of the operator */
	Vector parameters = new Vector();
	parameters.addElement(containeeIVL);
	parameters.addElement(containerIVL);

	ContainedOp containedOp = new ContainedOp(parameters);
	containedOp.evaluate();
	
	System.out.println(containedOp.getOpType());
	
	containedOp.printResult();

	return containedOp.getResult();
	       	    
    }
    
    
    public Vector evalANDOp(Vector leftIVL,Vector rightIVL) throws IMException
    {
	Vector sourceIVLs = new Vector();

	sourceIVLs.addElement(leftIVL);
	sourceIVLs.addElement(rightIVL);
		
	ANDOp andOp = new ANDOp(sourceIVLs);
	andOp.evaluate();
	
	System.out.println(andOp.getOpType());
	andOp.printResult();

	return andOp.getResult();
	
    }
    
     public Vector evalOROp(Vector leftIVL,Vector rightIVL) throws IMException
    {
	Vector sourceIVLs = new Vector();

	sourceIVLs.addElement(leftIVL);
	sourceIVLs.addElement(rightIVL);
		
	OROp orOp = new OROp(sourceIVLs);
	orOp.evaluate();
	
	System.out.println(orOp.getOpType());
	orOp.printResult();

	return orOp.getResult();
	
    }
    
     public Vector evalEXCEPTOp(Vector leftIVL,Vector rightIVL)
	 throws IMException
    {
	Vector sourceIVLs = new Vector();

	sourceIVLs.addElement(leftIVL);
	sourceIVLs.addElement(rightIVL);
		
	EXCEPTOp exceptOp = new EXCEPTOp(sourceIVLs);
	exceptOp.evaluate();
	
	System.out.println(exceptOp.getOpType());
	exceptOp.printResult();

	return exceptOp.getResult();
	
    }
     
     public Vector evalISOp(Vector elemIVL,Vector strIVL)
	 throws IMException
    {
	Vector sourceIVLs = new Vector();

	sourceIVLs.addElement(elemIVL);
	sourceIVLs.addElement(strIVL);
		
	ISOp isOp = new ISOp(sourceIVLs);
	isOp.evaluate();
	
	System.out.println(isOp.getOpType());
	isOp.printResult();

	return isOp.getResult();
	
    }

    public static void main(String[] args) 
    {
	String fname = "/u/q/i/qiongluo/public/dblp/books/mg/SilberschatzKS97";
	
	if (args.length >= 1) {
	    fname = args[0];
	}
	
	AbstractOperator op;
	
	try {
	    TestOps testOps = new TestOps();
	
	    (IndexMgr.idxmgr).index(fname, null, null);
	    (IndexMgr.idxmgr).index("/u/q/i/qiongluo/public/dblp/books/aw/kimL89/Russinoff89", null, null);
	    
	    
	    //let's test the following queries:
	    
	    // name contains "Holloway" OR/EXCEPT/AND name contains "Bob"

	    // firstname CONTAINS "Bob" OR/EXCEPT/AND firstname IS "Bob"

	    // firstname contains "Bob" OR/EXCEPT/AND firstname contained "staff"
	    //firstname contains "Bob" OR/EXCEPT/AND firstname contained <Doc>

	    //<Doc> contains "Bob" OR/EXCEPT/AND <Doc> contains "Dewitt"

	    //name contains "Bob" OR/EXCEPT/AND name contains "TAMURA"

	    String str1 = "System";	    
	    String str2 = "databases";
	    String elemName = "@Doc";
	    String elem2Name = "book";
	    
	    	    
	    Vector str1IVL = testOps.evalStringOp(str1);
	    Vector str2IVL = testOps.evalStringOp(str2);	    

	    //parameters for elemName
	    Vector parameters = new Vector();
	    parameters.addElement(IndexMgr.idxmgr);	    
	    parameters.addElement(elemName);
	    parameters.addElement(new Boolean(true));
	    //isElement true

	    IVLOp ivlOp = new IVLOp(parameters);
	    
	    Vector elemIVL = ivlOp.getResult();
	    
	    //parameters for elem2Name
	    parameters = new Vector();
	    parameters.addElement(IndexMgr.idxmgr);	    
	    parameters.addElement(elem2Name);
	    parameters.addElement(new Boolean(true));
	    //isElement true

	    IVLOp ivlOp2 = new IVLOp(parameters);
	    
	    Vector elem2IVL = ivlOp2.getResult();
	    
	    Vector leftIVL = testOps.evalContainOp(elemIVL,str2IVL);
	    //Vector rightIVL = testOps.evalISOp(elemIVL,str2IVL);
	    Vector rightIVL = testOps.evalContainOp(elemIVL,str1IVL);
	    

	    //finally test OR/AND/EXCEPT
	    //Vector ivl = testOps.evalANDOp(leftIVL,rightIVL);
/*
	    Table tb = ((Table)getTable(ivl)).clone();
	    
	    System.out.println(tb.toString());
*/	    
	    //testOps.evalANDOp(rightIVL,leftIVL);

	    //testOps.evalOROp(leftIVL,rightIVL);
	    //testOps.evalOROp(rightIVL,leftIVL);

	    //testOps.evalContainOp(elemIVL, 
	    //		  testOps.evalEXCEPTOp(leftIVL,rightIVL));
	    testOps.evalEXCEPTOp(rightIVL,leftIVL);
	    
	    IndexMgr.idxmgr.flush();

	}
	catch (IMException e) {
	    e.printStackTrace();
	}
	
    } //end of main()

} //end of TestOps

     
    
       
