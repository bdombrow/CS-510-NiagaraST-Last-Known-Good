
/**********************************************************************
  $Id: MyQuery.java,v 1.2 2003/07/08 02:08:21 tufte Exp $


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


package niagara.client.qbe;
//done up to (No) nest
// have applied the join index method 

// Input a vector of Type Input, each input has same inWhichElement (xml)
// join: let's first assume join is always appied to the leaf node
// join: need to pass the children tree in? or knowing how to traverse
// nest 

import java.util.*;


/**
 *
 *
 */
public class MyQuery
{
    
    // The query String
    //
    public String query;

    // Private vars
    //
    private boolean nesting;
    private boolean hasMoreConstruct;
    private Vector parsedInV;
    private Vector parsedPredV;
    private Vector parsedConstrV;
    private int indent;

    /**
     *
     *
     */
    public MyQuery()
    {
	query = new String();
	indent = 0;
	nesting = false;
	hasMoreConstruct = false;
    }

    /**
     *
     *
     */
    int getIndent()
    {
	return indent;
    }

    /**
     *
     *
     */
    public static String genIndent(int size)
    {
	String s=new String();
	for(int i=0; i<size; i++){
	    s += "   ";
	}
	return s;
    }


    //a top level func to call by my test Main() or the UI
    
    /**
     *  This is the function called by the GUI to generate the XML-QL query
     *
     */
    public boolean genQuery(Vector inV)
    {
	
	// Parse the information input from the GUI
	//
	parseInput(inV);
	
	// Create the WHERE block
	//
	genWhereBlock();
	
	
	// Generate each CONSTRUCT block
	//
	do{
	    genConstructBlock();
	    
	    // NO NESTED QUERIES YET
	    // NO NESTED QUERIES YET
	    //if(nesting){
	    //indent++;
	    //TODO: an mechanism (maybe notify-wait?) with UI for nesting
	    //getInput(inV);
	    //genQuery(inV);
	    //}
	    
	} while(hasMoreConstruct);
	return true;
    }
    
    

    /**
     *
     *
     */
    private boolean parseInput(Vector inV)
    {
	// 
	//
	parsedInV = new Vector();
	parsedPredV = new Vector();
	parsedConstrV = new Vector();
	if(inV == null) return false;

	MyTree oneTree = null;

	for(int i = 0; i < inV.size(); i++){

	    Input input = (Input) inV.elementAt(i);
		 //a tree instance for an input 
	    if(input.entries.size()==0)
		continue;
	    oneTree = new MyTree(input.inWhich, indent);

	    if(i==0){
		// for adjust the indentation of the first WHERE IN block
		oneTree.setFirstTree();
	    }

	    for(int j=0; j<input.entries.size(); j++){
		oneTree.insert((Entry)input.entries.elementAt(j));
	    }

	    // 
	    //
	    oneTree.print();

	    //TODO: IN .xml...
	    String tmp=oneTree.getIn().trim();
	    if(tmp.length()!=0)
		parsedInV.addElement(oneTree.getIn());

	    tmp=oneTree.getPredicates().trim();
	    if(tmp.length()!=0){
		parsedPredV.addElement(oneTree.getPredicates());
	    }
	    tmp=oneTree.getConstructs().trim();
	    if(tmp.length()!=0)
		parsedConstrV.addElement(oneTree.getConstructs());
	    else
		System.out.println("Warning: empty construct part would NOT be parsed");

	}
	return true;
    }


    /**
     *
     *
     */
    boolean genWhereBlock()
    {
	int i=0;
	query += (genIndent(indent)+"WHERE ");

	for(i=0; i<parsedInV.size(); i++){
	    query += (String) parsedInV.elementAt(i);
	    if(i==(parsedInV.size()-1)){
		if(!parsedPredV.isEmpty())
		    query += (",\n");
		else
		    query += "\n";
	    }
	    else{
		query += (",\n"); 
	    }
	}

	// just added
	System.out.println(query);

	for(i=0; i<parsedPredV.size(); i++){
	    String tmp= (String) parsedPredV.elementAt(i);
	    //System.out.println("DEBUG: tmp="+tmp.tostring());
	    int last;
	    if(i==(parsedPredV.size()-1)){
		//the predicates were aassembled as element per tree instance
		//unfortunately the last predicate contains an extra ","
		last=tmp.lastIndexOf(",");
		if(last<=0){
		    System.out.println("Warning: Wrong with the predicates string");
		}
		else
		    query+=tmp.substring(0, last)+"\n";
	    }
	    else
		query += tmp;

	}
	return true;
    }
    

    /**
     *
     *
     */
/*
    boolean genConstructBlock()
    {
	//TODO: change for nesting 
	query += (genIndent(indent)+"CONSTRUCT <result>\n");
	for(int i=0; i<parsedConstrV.size(); i++){
	    query += (String) parsedConstrV.elementAt(i);
	}
	query += (genIndent(indent+1)+"</>\n");
	return true;
    }
*/
	/**
	 * Modified by Leonidas
	 */
	boolean genConstructBlock()
		{
			if(parsedConstrV.size() == 0){
				return true;
			}
			else if(parsedConstrV.size() == 1){
				query += (genIndent(indent)+"CONSTRUCT ");
				
				// for the moment there is only one element
				String s = (String) parsedConstrV.elementAt(0);
				StringTokenizer st = new StringTokenizer(s, "\n");
				
				if(st.countTokens() > 1){
					query += "<result>\n" + s + (genIndent(indent+1)+"</>\n");
				} else {
					query += "\n" + s;
				}
			}
			else if(parsedConstrV.size() > 1){
				query += (genIndent(indent)+"CONSTRUCT <result>\n");
				for(int i=0; i<parsedConstrV.size(); i++){
					query += (String) parsedConstrV.elementAt(i);
				}
				query += (genIndent(indent+1)+"</>\n");
				return true;
			}
			return true;
		}
}
