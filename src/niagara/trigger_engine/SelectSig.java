
/**********************************************************************
  $Id: SelectSig.java,v 1.2 2001/08/08 21:29:02 tufte Exp $


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


package niagara.trigger_engine;

/**
 * The class <code>SelectSig</code> is the class for representing
 * select signature. It derives from Signature class.
 * 
 * @version 1.0
 *
 *
 * @see Signature 
 */

import java.io.*;
import java.lang.*;
import java.util.*;
import org.w3c.dom.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;
import niagara.utils.*;

public class SelectSig extends Signature {

    SelectSig(logNode node, String plan, TriggerManager tm, GroupQueryOptimizer gOpt) {
        op op1, op2;
        schemaAttribute attr1, attr2, attr3;    
        logNode curnode1, curnode2, prevnode1, prevnode2;
        data d,d2;
        regExp reg;
        //int xmlInt;

	//set the plan
	this.plan = plan;

	//get an uniqe id for group
        groupId = tm.getNextId();

        //create the signature logical plan
        try {	
            // System.err.println("Select Group : create a dtdscan node");
            op1 = (dtdScanOp)operators.DtdScan.clone();
            //int tmpint = tm.getNextId();
            Vector v = new Vector();
            v.addElement("SYS/constTbl"+groupId+".xml");
            // v.addElement(constTblName);
            String s = new String("selectConstTbl.dtd");
            op1.setDtdScan(v,s);
            curnode1 = new logNode(op1);
            //curnode1.Id = tmpint;

            // System.err.println("create a scan node for attribute pairs");
            attr1 = new schemaAttribute(0);
            op1 = (scanOp)operators.Scan.clone();
            attr2 = new schemaAttribute(1);
            // d=new data(dataType.ATTR, attr2);
            d = new data(dataType.STRING, "pairs");
            reg=new regExpDataNode(d);
            ((scanOp)op1).setScan(attr1,reg);
            prevnode1 = curnode1;
            curnode1 = new logNode(op1,prevnode1);
            //curnode1.Id = tm.getNextId();

            // System.err.println("create a scan node for attribute pair");
            attr1 = new schemaAttribute(1);
            op1 = (scanOp)operators.Scan.clone();
            attr2 = new schemaAttribute(2);
            // d=new data(dataType.ATTR, attr2);
            d = new data(dataType.STRING, "pair");
            reg=new regExpDataNode(d);
            ((scanOp)op1).setScan(attr1,reg);
            prevnode1 = curnode1;
            curnode1 = new logNode(op1,prevnode1);
            //curnode1.Id = tm.getNextId();

            // System.err.println("create a scan node for attribute value");
            attr1 = new schemaAttribute(2);
            op1 = (scanOp)operators.Scan.clone();
            attr2 = new schemaAttribute(3);
            //  d2=new data(dataType.ATTR, attr2);
            d2=new data(dataType.STRING, "value");
            reg=new regExpDataNode(d2);	
            ((scanOp)op1).setScan(attr1,reg);
            prevnode1 = curnode1;
            curnode1 = new logNode(op1,prevnode1);
            //curnode1.Id = tm.getNextId();

            // System.err.println("create a scan node for attribute tmpFileName");
            attr1 = new schemaAttribute(2);
            op1 = (scanOp)operators.Scan.clone();
            // attr2 = new schemaAttribute(4);
            // d=new data(dataType.ATTR, attr2);
            d=new data(dataType.STRING, "tmpFileName");
            reg=new regExpDataNode(d);	
            ((scanOp)op1).setScan(attr1,reg);
            prevnode1 = curnode1;
            curnode1 = new logNode(op1,prevnode1);
            //curnode1.Id = tm.getNextId();

            // System.err.println("create a join node");
            op1 = (joinOp)operators.Join.clone();
            prevnode2 = curnode1;
            prevnode1 = node.input(0); //the scan node under select

            //note we only consider select predicate with the format 
            //attr op constant now. Thus the getLeftExp() will return
            //the data associated with that attribute
            attr2 = new schemaAttribute(3);
            attr2.setStreamId(1);
	    
	    /* code for generating join which be picked up for Hash join algorithm
	    //left attribute vector
	    Vector lv=new Vector();
	    d=((predArithOpNode)((selectOp)sigNode.getOperator()).getPredicate()).getLeftExp();
	    debug.mesg("*****************dumping schemaAttribute.....");
	    ((schemaAttribute)d.getValue()).dump();
	    lv.addElement(d.getValue()); //get the schemaAttribute

	    //right attribute vector
	    Vector rv=new Vector();
	    rv.addElement(attr2);
	    
	    //set the join
	    ((joinOp)op1).setJoin((predicate)null,lv,rv);
	    */
	
	    predicate tmp = ((selectOp)node.getOperator()).getPredicate();
	    if (tmp instanceof predLogOpNode) {
		try {
		    tmp=gOpt.getLeftMostConjunct(tmp);
		}
		catch (Exception ex) {
		    //never reach here
		}
	    }	 
            d=((predArithOpNode)tmp).getLeftExp();	
	    
            d2=new data(dataType.ATTR, attr2);

            predicate pred=new predArithOpNode(tmp.getOperator(), d, d2);

            //useless allocation
            //this is because setJoin code does not allow left predicate vecotor
            //to be null
            Vector fooV=new Vector();

            ((joinOp)op1).setJoin(pred,fooV,null);
	    
            curnode1 = new logNode(op1,prevnode1,prevnode2);
            //curnode1.Id = tm.getNextId();
            //create a split node
            splitOp topOp=(splitOp)operators.Split.clone();	
            //topOp.initMappingTbl();

            root = new logNode(topOp,curnode1);

	    // addMember(node, gOpt);
            // System.err.println("GROUP SELECT DONE !!! ");

        } catch (java.lang.CloneNotSupportedException e) {           
            System.out.println("Error in generating select group logical plan");
            e.printStackTrace();
        }

    }

    /**
     * add a member to this select group
     * @param pred the predicate used for grouping
     * @return the tmp file for this trigger
     **/
    public String addMember(predArithOpNode pred, GroupQueryOptimizer gOpt) {

        //get the const value of the predicate in this trigger 

        //selectOp sop = (selectOp)node.getOperator();
        //debug.var(sop, 1);
        //predArithOpNode pred = (predArithOpNode)sop.getPredicate();
        //debug.var(pred, 1);
        String value = (String)pred.getRightExp().getValue();
        debug.var(value, 1);

        //store the value and cid pair into const table
        // debug.var(exeNode, 2);

	String tmpFileName = gOpt.insertConst(groupId, value);

	return tmpFileName;
    }

}


