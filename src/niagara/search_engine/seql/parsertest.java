
/**********************************************************************
  $Id: parsertest.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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


package niagara.search_engine.seql;

import java.io.*;
import java_cup.runtime.*;
import java.net.*;

import niagara.search_engine.indexmgr.*;
import niagara.search_engine.operators.*;


/**
 * class for testing parser.
 *
 *
 */
class parsertest {
  public static void
  main(String args[]) throws java.io.IOException,  Exception {

	if (args.length != 1) {
       		System.out.println(
			"Error: Input file must be named on command line." );
		System.exit(-1);
    	}

	
    	java.io.FileInputStream yyin = null;

    	try {
    		yyin = new java.io.FileInputStream(args[0]);
    	} catch (FileNotFoundException notFound){
       		System.out.println ("Error: unable to open input file.");
		System.exit(-1);
    	}

	//    Scanner.init(yyin); // Initialize Scanner class for parser

    parser seqlParser = new parser(new Yylex(yyin));

    Symbol root=null;

    try {
        root = seqlParser.parse(); // do the parse
        System.out.println ("SEQL parsed correctly.");

    } catch (SyntaxErrorException e){
        System.out.println ("Compilation terminated due to syntax errors.");
        System.exit(0);
    }
    //    System.out.println ("Here is its unparsing:");
    //    ((queryNode)root.value).Unparse(0);
    QueryPlan plan = ((queryNode)root.value).genPlan();
    plan.dump();
    plan.writeDot(plan.makeDot(), new FileWriter("QEP.dot"));
    System.out.println("Query Plan dot file written to QEP.dot");
    System.out.println("To materialize:");
    System.out.println("\tdot -Tps QEP.dot > QEP.ps");
    System.out.println("\tghostview QEP.ps");

    

    IndexMgr.idxmgr.index(new URL("department.xml"));
    IndexMgr.idxmgr.index(new URL("personal.xml"));
    
    plan.eval();
    AbstractOperator op = plan.getOperator();
    //Vector v = op.getResult();
    op.printResult();
    
    IndexMgr.idxmgr.flush();
    
    return;
  }
}
	
