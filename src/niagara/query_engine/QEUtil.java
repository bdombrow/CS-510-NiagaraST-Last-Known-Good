/**********************************************************************
  $Id: QEUtil.java,v 1.8 2003/03/08 02:23:04 vpapad Exp $


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


package niagara.query_engine;

import org.w3c.dom.*;
import java.io.*;
import java.util.*;

import niagara.logical.Predicate;
import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;

/**
 *  The QEUtil class has some static methods for doing utility stuff
 *  to trees, such as pruning the empty node that are present in 
 *  trees produced by the old parser.  Call the functions here like:
 *  <pre>
 *       QEUtil.pruneEmptyNodes(Element);
 *  
 *  </pre>
 *
 *  To remove the empty "\n" nodes.
 */
public class QEUtil {
    
    private final static String PCDATA = "#PCDATA";


    /**
     *  Get a regular expression given a filename.  Used for hardcoding queries
     *  
     */
    public static regExp getRegExp(String filename) 
	throws java.io.FileNotFoundException, ParseException {
	
	condition c;
	pattern pa;
	regExp rexp= null;
	
	// Parse the query in file on command line
	Scanner s = new Scanner(new EscapedUnicodeReader(new FileReader(filename)));
	QueryParser p = new QueryParser(s);

	java_cup.runtime.Symbol parse_tree;
	try{ // ugly, cup parser returns "Exception"
	    parse_tree = p.parse();
	} catch (Exception e) {
	    throw new ParseException("Error parsing " + filename);
	}
	    
	// Get the conditions
	query q = (query) parse_tree.value;
	if(q == null) return null;
	Vector v = q.getConditions();
	
	// Find the first in clause
	for(int i= 0; i< v.size(); i++) {
	    c = (condition)v.elementAt(i);
	    if(c instanceof inClause) {
		pa = ((inClause)c).getPattern();
		rexp = pa.getRegExp();
		break;
	    }
	}
	return rexp;
    }



    /**
     *  Get a regular expression given a filename.  Used for hardcoding queries
     *  
     */
    public static Predicate getPredicate(String filename) 
	throws java.io.FileNotFoundException, ParseException {
	
	condition c;
	Predicate pred = null;
	
	// Parse the query in file on command line
	Scanner s = new Scanner(new EscapedUnicodeReader(new FileReader(filename)));
	QueryParser p = new QueryParser(s);

	java_cup.runtime.Symbol parse_tree;
	try {
	    parse_tree = p.parse();
	} catch (Exception e) { // cup throws "Exception"
	    throw new ParseException("Error parsing " + filename);
	}
	    
	// Get the conditions
	//
	query q = (query) parse_tree.value;
	if(q == null) return null;
	Vector v = q.getConditions();
	
	// Find the first predicate and return itmake
	//
	for(int i= 0; i< v.size(); i++) {
            Object oc = v.elementAt(i);
	    if(oc instanceof Predicate) {
		return (Predicate)oc;
	    }
	}
	return pred;
    }

}




