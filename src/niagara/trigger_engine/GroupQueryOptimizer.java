
/**********************************************************************
  $Id: GroupQueryOptimizer.java,v 1.1 2000/05/30 21:03:29 tufte Exp $


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
 * The class <code>GroupQueryOptimizer</code> is the class for incremental
 * group optimizing continuous queries. 
 * 
 * @version 1.0
 *
 * 
 */

import com.ibm.xml.parser.*;
import org.w3c.dom.*;
import java.io.*;
import java.util.*;
import niagara.query_engine.*;
import niagara.data_manager.*;
import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;


public class GroupQueryOptimizer {

    //common query optimizer is used do the common query optimization
    private QueryOptimizer qOpt;  

    //instance of trigger file manager, which handles adding and removing
    //entries to constant table
    private TrigFileMgr trigFM;
   
    //used to hold all existing group information
    private GroupTbl SYSGroupTbl;

    //instance of trigger manager
    private TriggerManager tm;

    
    /**
     * constructor
     * @param QueryEngine 
     **/
    GroupQueryOptimizer(QueryOptimizer qo, TriggerManager tm) {
	
        qOpt = qo;

	this.tm = tm;

        trigFM = new TrigFileMgr(tm.getDataMgr());
               
        SYSGroupTbl = new GroupTbl();       
	
    }

    /**
     * This function returns the instance of trigger file manager
     *@return TrigFileMgr
     **/
    public TrigFileMgr getTrigFileMgr() {
	return trigFM;
    }

    /*This is the interface for performing group optimization
     *@param trigPlan root of trigger plan
     *@return Vector GroupOptResult
     *@see GroupOptResult
     */
    public Vector groupOptimize(logNode trigPlan) 
	throws Exception {
                
	//Vector childHashValues = new Vector();
	Vector childPlanStrings = new Vector();

	//these 3 vectors are used keep the information to return
	//to the TriggerManager
	Vector idV = new Vector(); //trigger id vector
	Vector nodeV = new Vector(); //trigger root vector
	Vector srcV = new Vector(); //src file name vector

        // System.err.println("Now Computing HashCode for Sig");
	try {
	calcSignatures(trigPlan, /*childHashValues, */childPlanStrings, 0, null,idV,nodeV,srcV);
	}
	catch (Exception ex) {
	    ex.printStackTrace();
	    System.err.println("Exception in calculating signature");
	}

	int trigId = tm.getNextId(); 
	idV.addElement(new Integer(trigId));

	//this is the root of the top level trigger
	nodeV.addElement(trigPlan);

	debug.mesg("%%%%%%%%%%%%%%%%%%%%%%");
	debug.mesg(childPlanStrings.toString());
	debug.mesg(idV.toString());
	debug.mesg(nodeV.toString());
	debug.mesg(srcV.toString());

	//group optimization result vector
	Vector resultV = new Vector();

	int size = idV.size();
	for (int i=0; i<size; i++) {
	    GroupOptResult gOptRes = new GroupOptResult((Integer)idV.elementAt(i), (logNode)nodeV.elementAt(i), (Vector)srcV.elementAt(i));
	    resultV.addElement(gOptRes);	    
	}
	
	return resultV;
    }

    /**
     * This is the interface for adding a new trigger constant to a 
     * constant table. If this constant existed in the constant table,
     * corresponding tmpFileName is returned. Otherwise, a new tmpFileName
     * is allocated and returned.
     * @param groupId group query id 
     * @param value the constant value to be inserted
     * @return tmpFileName
     **/
    public String insertConst(int groupId, String value) {
        try {
            return trigFM.insertConst(groupId, value);
        } catch (IOException ioe) {
            ioe.printStackTrace();
	    return null;
        }
    }

    /**
     * this method is used to compute a signature for each logical operator.
     * A signature is based on its information and its children's
     * information, that is a linearlized ascii representation. For 
     * selection operator, a tmporary hack is to take the 
     * first predicate as its signature. Other predicates are
     * left the other predicates in the selector operator. In the future,
     * we take the most selective predicate as the signature.
     * @param node current logNode 
     * @param childPlanStrings store the signature of child plan strings,
     *                         used as a stack.
     * @param idV split trigger ids
     * @param nodeV split trigger roots
     * @param srcV split trigger source files
     **/
    private void computeSig(logNode node, Vector childPlanStrings,Vector idV, Vector nodeV, Vector srcV) throws Exception {
        op currOp= node.getOperator();

        if (currOp instanceof dtdScanOp) {
            Vector docs=((dtdScanOp)currOp).getDocs(); 
	    System.out.println("$$$docs are "+docs.toString());

            String dtdName=((dtdScanOp)currOp).getDtdType();
	    int size = docs.size();
	    String tmp="dtdScan "+dtdName;
            
	    Vector sources = new Vector();
     
	    data d=(data)docs.elementAt(0);
	    String srcFileName=(String)d.getValue();
	    if (srcFileName.equals("*")) {
		Vector urls=tm.getSourceForDTD(dtdName);	
		((dtdScanOp)currOp).setDocs(urls);
		docs=((dtdScanOp)currOp).getDocs();     
		System.out.println("$$$docs are "+docs.toString());
		size = docs.size();
	    }
	 
	    for (int i=0; i<size; i++) {
		if (docs.elementAt(i) instanceof data) {
		    d=(data)docs.elementAt(i);
		    srcFileName=(String)d.getValue();
		}
		else {
		    srcFileName=(String)docs.elementAt(i);
		}

		tmp=tmp+" "+srcFileName; 
		sources.addElement(srcFileName);		
	    }
	    
	    srcV.addElement(sources);
	    childPlanStrings.addElement(tmp);
	    
        }

        else if (currOp instanceof scanOp) {

            regExp scanRegExp=((scanOp)currOp).getRegExpToScan();

	    int size=childPlanStrings.size();
	    String tmp=(String)childPlanStrings.elementAt(size-1);
	    childPlanStrings.removeElementAt(size-1);
            childPlanStrings.addElement("Scan "+scanRegExp.toString()+" "+tmp);
        }	

        else if (currOp instanceof selectOp) {

            predicate pred=((selectOp)currOp).getPredicate();
	    
	    int size=childPlanStrings.size();
	    String tmp=(String)childPlanStrings.elementAt(size-1);
	    childPlanStrings.removeElementAt(size-1);
	    String predStr = null;
            //childPlanStrings.addElement(tmp+" Select: "+pred.toString());
	    try {		    
		predStr = compSigForPred(node.getSchema(),pred);
            } catch(TRIGPredNotSuppException ex) {
                ex.printStackTrace();
                // System.err.println("TRIGPredNotSuppException: " + 
                // ex.getMessage());
		throw new Exception();
		
            }	      
	    
            childPlanStrings.addElement("Select "+ predStr+" "+tmp);

        }
        else if (currOp instanceof joinOp) {

	    Vector lv=((joinOp)currOp).getLeftEqJoinAttr();
	    Vector rv=((joinOp)currOp).getRightEqJoinAttr();

	    if ((lv.size()!=1)||(rv.size()!=1))
		System.err.println("Join predicate not supported");

	    Schema schema = node.getSchema();

	    //get the left join attribute name, 
	    int lAttrId=((schemaAttribute)(lv.elementAt(0))).getAttrId();
	    //only return the regExp of last level: lastname
	    regExp lExp=schema.getSchemaUnit(lAttrId).getRegExp();
	    String lJoinAttrName=lExp.toString();
            
	    //get the right join attribute name, 
	    int rAttrId=((schemaAttribute)(rv.elementAt(0))).getAttrId();
	    //only return the regExp of last level: lastname
	    regExp rExp=schema.getSchemaUnit(rAttrId).getRegExp();
	    String rJoinAttrName=rExp.toString();

	    int size=childPlanStrings.size();
	    String tmp1=(String)childPlanStrings.elementAt(size-1);
	    String tmp2=(String)childPlanStrings.elementAt(size-2);
	    childPlanStrings.removeElementAt(size-1);
	    childPlanStrings.removeElementAt(size-2);

            childPlanStrings.addElement("Join "+lJoinAttrName +"="+rJoinAttrName+" "+tmp1+" "+tmp2);
        }

	//other operator, do nothing now
    }

    /**
     * This method calculates and matches the signature by one pass
     * depth-first traversal of the tree.
     * The Vector childPlanStrings contains the dumped string plan under
     * this node. it is used for comparation after the hashcode is matched
     * note we don't use the hashcode any more, since the string can
     * uniquely determine it
     * @param node current logNode
     * @param childPlanStrings store the signature of child plan strings,
     *                         used as a stack.
     * @param index the ith child in its parent
     * @param parent its parent node
     * @param idV split trigger ids
     * @param nodeV split trigger roots
     * @param srcV split trigger source files
     **/
    private void calcSignatures(logNode node, Vector childPlanStrings, int index, logNode parent, Vector idV, Vector nodeV, Vector srcV) throws Exception {

        op currOp = node.getOperator();
	
        if(!(currOp instanceof dtdScanOp)) {
            for(int i=0; i<node.numInputs(); i++) {
                calcSignatures(node.input(i), childPlanStrings, i, node, idV, nodeV, srcV);
		
	    }
	}
	
	computeSig(node, childPlanStrings,idV,nodeV,srcV);
	    
	int size = childPlanStrings.size();
	String subPlan = (String)(childPlanStrings.elementAt(size-1));

	debug.mesg("subPlan is: "+subPlan);

	//now we do matching signature while we 
	//calculating the signature. One pass traversal.
	if ((currOp instanceof selectOp)||(currOp instanceof joinOp)) {
	    matchSignatures(node, /* hashValue,*/ subPlan, idV, nodeV, srcV);


	    //get the tmp file name for creating a dtdScanOp for upper level
	    //trigger
	    String tmpFileName = (String)((Vector)(srcV.lastElement())).elementAt(0);

	    //We need to remove the old subplan of the new or matched
	    //signature. And we put the signature of the dtdScan which
	    //is new added into the upper part query into the subPlan
	    //for further grouping.
	    childPlanStrings.removeElementAt(size-1);
            childPlanStrings.addElement("dtdScan "+ tmpFileName);
	    

	    //this is the id for the upper level trigger
	    //int trigId = tm.getNextId(); 
	    //idV.addElement(new Integer(trigId));
	    if (currOp instanceof selectOp) {
		predicate pred = ((selectOp)currOp).getPredicate();
		if (pred instanceof predLogOpNode) {
		    Vector tmpV = new Vector();
		    //tmpV.addElement(((predLogOpNode)pred).getRightChild());
		    getPreds(pred,tmpV);
		    tmpV.removeElementAt(0);
		    ((selectOp)currOp).setSelect(tmpV);
		    splitPlan(node, 0, tmpFileName);
		    return;
		}
	    }
	    //split the plan
	    //splitPlan(parent, index, (String)srcV.lastElement()+"&"+trigId);
	    splitPlan(parent, index, tmpFileName);
	}

    }

    /**
     * Append a dtdScan operator on the new tmp file to the upper part
     * of trigger plan above split.
     * @param parent parent node
     * @param index the ith child in its parent
     * @param tmpFileName
     */
    private void splitPlan(logNode parent, int index, String tmpFileName) {
	dtdScanOp op1=null;
	logNode curnode1;

	//create a dtdScan operator
	try {
	    op1 = (dtdScanOp)operators.DtdScan.clone();	
	} catch (java.lang.CloneNotSupportedException e) {           
            System.out.println("Error in generating dtdScan operator");
            e.printStackTrace();
        }
	
	Vector v = new Vector();
	//here is a "hack", we use tmpFileName&tid for tmp trigger 
	//file name. DM will use the id to get related time info
	//and will remove the id part when doing the real dtdScan
	v.addElement(tmpFileName);
	op1.setDocs(v);
	curnode1 = new logNode(op1);

	//set the parent to its new child
	parent.setInput(curnode1,index);	
    }

    //This function merges the last two elements of a vector,
    //which are vectors by themselves
    //
    private void mergeTwoVector(Vector srcV) {
	int size=srcV.size();
	Vector v1=(Vector)srcV.elementAt(size-1);
	Vector v2=(Vector)srcV.elementAt(size-2);
	int size1 = v1.size();
	for (int i=0; i<size1; i++) {
	    v2.addElement(v1.elementAt(i));
	}
	srcV.removeElementAt(size-1);
    }

    /**
     * This method  matches a selection or join signature
     * with existing group signatures. If match is found,
     * this trigger is added into an existing group. Otherwise
     * a new group is created.
     * @param node current logNode
     * @param subPlan the current expression signature
     * @param idV split trigger ids
     * @param nodeV split trigger roots
     * @param srcV split trigger source files
     **/
    private void  matchSignatures(logNode node, String subPlan, Vector idV, Vector nodeV, Vector srcV) {
	String tmpFileName=null;

        op tmpOp = node.getOperator();
	
	Signature sig = SYSGroupTbl.findSignature(subPlan);
	
	if(sig!=null) { // sig FOUND 
		debug.mesg("!!! A Group is matched!!!");
		if (tmpOp instanceof selectOp) {
		    predicate pred=((selectOp)tmpOp).getPredicate();
		    
		    if (pred instanceof predLogOpNode) {
			try {
			    pred=getLeftMostConjunct(pred);
			}
			catch (Exception ex) {
			    //never happens 
			}			
		    }	 
		    
		    tmpFileName = ((SelectSig)sig).addMember((predArithOpNode)pred, this);
		}		
		if (tmpOp instanceof joinOp) {
		    //there are two vectors for the two sources of the join
		    //merge them into one big vector
		    mergeTwoVector(srcV);
		    tmpFileName = ((JoinSig)sig).addMember(node, this);	    
		}
		//mergePlan(sigNode, index, sig);
	}	
	else { // A new Group.
	    if (tmpOp instanceof selectOp) {
		debug.mesg("!!! A new SelectOp Group!!!");
		sig = new SelectSig(node, subPlan, tm, this);
		SYSGroupTbl.addSignature(sig);
		predicate pred=((selectOp)tmpOp).getPredicate();
		
		if (pred instanceof predLogOpNode) {
		    try {
			pred=getLeftMostConjunct(pred);
		    }
		    catch (Exception ex) {
			//never happens 
		    }		
		    
		}	 
		tmpFileName = ((SelectSig)sig).addMember((predArithOpNode)pred, this);
	    }
	    else if (tmpOp instanceof joinOp) {
		debug.mesg("!!! A new JoinOp Group!!!");
		sig = new JoinSig(node, subPlan, tm, this);
		SYSGroupTbl.addSignature(sig);
		
		//there are two vectors for the two sources of the join
		//merge them into one big vector
		mergeTwoVector(srcV);

		tmpFileName = ((JoinSig)sig).addMember(node, this);
	    }
	    else {		
		System.err.println("New Sig Not implemented yet");
	    }
	    //mergePlan(sigNode, index, sigNode);
	}

       
	idV.addElement(new Integer(sig.getGroupId()));
	nodeV.addElement(sig.getRootNode());

	debug.mesg("tmpFileName= "+tmpFileName);
	Vector fileV = new Vector();
	fileV.addElement(tmpFileName);
	srcV.addElement(fileV);

    }
    
    //this function returns the left most conjunt of a predicate
    //in the form of p1 AND p2 AND...AND Pm. If the predicate 
    //contains OR or NOT, it throws a TRIGPredNotSuppException.
    predicate getLeftMostConjunct(predicate pred)
	throws TRIGPredNotSuppException {
	//If it is in the form of p1 AND p2 AND ..., we will
	//choose p1 as the signature. If it contains NOT or OR,
	//an exception will be thrown. Future work!!!
	while (true) {
	    if (pred instanceof predArithOpNode)
		return pred;
	    else {
		switch(pred.getOperator()){
		case opType.AND: 
		    debug.mesg("AND");
		    pred=((predLogOpNode)pred).getLeftChild();
		    break;
		case opType.NOT: 
		    debug.mesg("NOT"); 
		case opType.OR: 
		    debug.mesg("OR");
		throw new TRIGPredNotSuppException();
		} 
	    }
	}
    }

    //this function returns a vector which contains all conjuncts 
    //of a predicate in the format of p1 AND p2 AND ..pm
    private void getPreds(predicate pred, Vector preds) {
	if (pred instanceof predArithOpNode) {
	    preds.addElement(pred);
	}
	else {
	//recursively get predicates on its left child
	getPreds(((predLogOpNode)pred).getLeftChild(), preds);
	//recursively get predicates on its right child
	getPreds(((predLogOpNode)pred).getRightChild(),preds);
	}
    }

    /**
     * calculate the signature string of a select operator.  
     * currently we only can handle very simple predicates, 
     * For example, serveral "attr op constant" terms connected
     * with AND. In the future, we will choose the most selective
     * conjunct (which does not contain OR) as signature in a CNF. 
     * 
     * @param schema 
     * @param predicate
     * @return string representation of predicate
     **/
    private String compSigForPred(Schema schema, predicate pred)
        throws TRIGPredNotSuppException {

	if (pred instanceof predLogOpNode) {
	    //If it is in the form of p1 AND p2 AND ..., we will
	    //choose p1 as the signature. If it contains NOT or OR,
	    //an exception will be thrown. Future work!!!
	    pred = getLeftMostConjunct(pred);
	}	
	//else {
	
	data lexpr=((predArithOpNode)pred).getLeftExp();
	data rexpr=((predArithOpNode)pred).getRightExp();
	
	//our signature is in the form "attr op constant"
	if ((lexpr.getType() == dataType.ATTR) && (rexpr.getValue() instanceof String)) {
	    int attrId=((schemaAttribute)(lexpr.getValue())).getAttrId();
	    
	    //only return the regExp of last level: firstname
	    regExp tmp=schema.getSchemaUnit(attrId).getRegExp();
	    
	    //return the path expression from the root: person.name.firstname
	    //regExp tmp=schema.getRegExp(attrId);
	    
	    //it is empty of the path expression in schemaAttribute
	    //regExp tmp=((schemaAttribute)(lexpr.getValue())).getPath();
	    
	    //int currHashValue;
	    
	    //try {
	    //currHashValue=compSigForRegExp(tmp);
	    String tmpString=tmp.toString();
	    /*
	      } catch(TRIGRegExpNotSuppException ex) {
	      System.err.println("TRIGRegExpNotSuppException: " + ex.getMessage());
	      return null;		    
	      }		*/
	    //return currHashValue;
	    return tmpString;
	}
	else {		
	    throw new TRIGPredNotSuppException();
	    
	}
	
	//}
	
    }
    
    /**
     * calculate the hash value for regExp 
     * 
     * @param regExp
     * @return hash value for the regExp
     **/
    /*    private int compSigForRegExp(regExp reg)
        throws TRIGRegExpNotSuppException {
            //currently we only handle the simplest regular expression, 
            //where it is a regExpDataNode, no regExpOpNode.

            if (reg instanceof regExpOpNode) {
                //in the future we consider more complex case
                throw new TRIGRegExpNotSuppException();

            }	
            else {

                data myData=((regExpDataNode)reg).getData();

                if ((myData.getType() == dataType.IDEN)||(myData.getType() == dataType.STRING)) {		
                    debug.mesg((String)myData.getValue());

                    return ((String)myData.getValue()).hashCode()/10;		
                }
                else {		
                    throw new TRIGRegExpNotSuppException();		
                }

            }

        }   
*/ 
}
