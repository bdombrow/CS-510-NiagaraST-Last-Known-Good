
/**********************************************************************
  $Id: TypeChk.java,v 1.2 2000/08/21 00:38:37 vpapad Exp $


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


//   NIAGRA Project
//
//   Jianjun Chen 
//
//   Validate a path expression is valid
//
//   ---------------
//   Methods
//   ---------------
//   public int validate(Vector path) 
//   return value: -1 valid, i if not valid at ith element   
//
//   check childName is a direct child of parentName 
//   private int check_1_level(String parentName, String childName)
//   return value: 1 valid, 0 invalid   
//   
//   check whether child is in CMNode cmn
//   private  int find_in_CM(CMNode cmn, String child)
//   return value: 1 valid, 0 invalid   
//   
//   check whether a regular path expression is valid or not  
//   public int validate(regExp myRegExp)
//   return value: 1 valid, 0 invalid   
//
//
////////////////////////////////////////////////////////////////////////////////
////
package niagara.xmlql_parser.op_tree;


import org.w3c.dom.*;
import java.io.*;
import java.net.*;
import java.lang.*;
import java.util.*;

import niagara.xmlql_parser.syntax_tree.*;

public class TypeChk {
private DTD myDtd;

//I tried to load the DTD information without loading a document
//but it seems not work. Thus I load the DTD as usual with the
//document together. There is also a limitation for directly loading
//DTD without loading document is that the DTD is an external set.
//I don't know how to load internal DTD set without loading document. -jianjun 
//public TypeChk(String dtdFileName) {

    /**
     * Constructor
     *
     * @param  String filename The name of the document 
     */
public TypeChk(String filename) {
/*
    Parser parse = new Parser(dtdFileName);
    try{
    	if (dtdFileName.indexOf(':')==-1) {
	    // Read file from the filesystem
	    BufferedReader reader = new BufferedReader(new FileReader(dtdFileName));
	    myDtd=parse.readDTDStream( reader );
    	}
    	else {
	    // Read file from a URL
	    InputStream is = (new URL(dtdFileName)).openStream();
	    myDtd=parse.readDTDStream( is );
        }
    } catch(Exception e){
        System.out.println("Ooops, bad filename or URL: "+e);
    }
*/

    // Parse the DTD (if not found or not valid, myDtd will be null
    //
    myDtd = CUtil.parseDTD(filename);

    //BufferedReader reader=null;
   
    //try {
    //reader=new BufferedReader(new FileReader(filename));
    // } catch (FileNotFoundException notFound) {
    //System.err.println(notFound);
    //return;
    //}
    //Parser P = new Parser(filename);
    //P.setWarningNoDoctypeDecl(true);
    //P.setWarningNoXMLDecl(true);
    //P.setKeepComment(true);
    //TXDocument doc = new TXDocument();
    //P.setElementFactory(doc);
    //P.readStream(reader);
    //myDtd=doc.getDTD();

};

//We need to validate the regular expression too. 
//Currently I consider regular expression tree with 
//identifier and '|' and ',' operators
//return 0 if not valid, otherwise 1
//The top and bottom set of the regular expression will be returned too.
public int validate(regExp myRegExp, Vector topSet, Vector bottomSet) {  
    getTopSet(myRegExp,topSet);
    return (constructSet(myRegExp, bottomSet));
}

    //constructSet does the main thing of typechecking a regular
    //expression. It can handle '|' and ',' operators now. Note,
    // if one path is valid, then the whold expression would
    // valid.
    //return 0 if not valid, otherwise 1
private int constructSet(regExp myRegExp, Vector currSet) {
    if (myRegExp instanceof regExpDataNode) {
	switch (((regExpDataNode)myRegExp).getData().getType()) {
	    case dataType.IDEN:
		currSet.addElement(((regExpDataNode)myRegExp).getData().getValue());
		break;
	    default: 
		System.err.println("invalid type for dataRegExpr node");
                System.exit(1);
	}
    }
    else { //regExpOpNode
	switch (((regExpOpNode)myRegExp).getOperator()) {
	    case opType.DOT:
		Vector parentSet=new Vector();
		constructSet(((regExpOpNode)myRegExp).getLeftChild(), parentSet);
		constructSet(((regExpOpNode)myRegExp).getRightChild(), currSet);
		if (checkTwoSets(parentSet, currSet)==0)
		    return 0;		
		break;
            case opType.BAR:
		constructSet(((regExpOpNode)myRegExp).getLeftChild(), currSet);
		constructSet(((regExpOpNode)myRegExp).getRightChild(), currSet);
		break;
	    default: 
		System.err.println("invalid type for dataRegExpr node");
                System.exit(1);
	}	
    }
    return 1;
}

//return the top set of the regular expression
private void getTopSet(regExp myRegExp, Vector topSet) {
    if (myRegExp instanceof regExpDataNode) {
	switch (((regExpDataNode)myRegExp).getData().getType()) {
	    case dataType.IDEN:
		topSet.addElement(((regExpDataNode)myRegExp).getData().getValue());
		break;
	    default: 
		System.err.println("invalid type for dataRegExpr node");
                System.exit(1);
	}
    }
    else { //regExpOpNode
	switch (((regExpOpNode)myRegExp).getOperator()) {
	    case opType.DOT:
		getTopSet(((regExpOpNode)myRegExp).getLeftChild(), topSet);
		break;
            case opType.BAR:
		getTopSet(((regExpOpNode)myRegExp).getLeftChild(), topSet);
		getTopSet(((regExpOpNode)myRegExp).getRightChild(), topSet);
		break;
	    default: 
		System.err.println("invalid type for dataRegExpr node");
                System.exit(1);
	}	
    }
}


//check whether childSet is under one level of parentSet. For any 
//child which is not valid under any parent, it will be deleted
//from the childSet.
//return 0 if no path is valid, otherwise 1
private int checkTwoSets(Vector parentSet, Vector childSet) {
System.out.println("parentSet"+parentSet.toString());
System.out.println("parentSet"+childSet.toString());
    int count=0;
    int childSetSize=childSet.size();
    int [] deleteSet=new int[childSetSize];
    int deleteSetInd=0;
    for (int i=0; i<childSetSize; i++) {
	int j=0;
	System.out.println("check_1_level "+check_1_level((String)parentSet.elementAt(j), (String)childSet.elementAt(i)));
	
	while ((j<parentSet.size())&&(check_1_level((String)parentSet.elementAt(j), (String)childSet.elementAt(i))==0))
	    j++;	
System.out.println("j "+j);
	if (j==parentSet.size()) {
	    deleteSet[deleteSetInd]=j;
	    deleteSetInd++;
	}    
    }

System.out.println("childSetSize "+childSetSize);

System.out.println("deleteSetInd "+deleteSetInd);
    //if all children are deleted, return 0
    if (deleteSetInd==childSetSize)
	return 0;
    
    //remove those children which are not belong to any parent
    for (int i= deleteSetInd; i>0; i--)
	childSet.removeElementAt(deleteSet[i]);
    return 1;
    
}
    
    
//check childName is a direct child of parentName  
//return 1 if matching, otherwise 0    
 
private int check_1_level(String parentName, String childName) {

      ContentModel parentCM=myDtd.getContentModel(parentName);
      System.out.println(" "+parentCM);
      CMNode parentCMNode=parentCM.getContentModelNode();
      return (find_in_CM(parentCMNode, childName));
}

//return 1 if matching, otherwise 0    
private  int find_in_CM(CMNode cmn, String child) {
       
      if (cmn instanceof CMLeaf){
	  System.out.println("Leaf ELEMENT: "+((CMLeaf)cmn).toString());
	  if (child.equals(((CMLeaf)cmn).toString())) {
      	      System.out.println("matching " + child);
	      return 1;
	  }
	  return 0;
      }
      else if (cmn instanceof CM1op){
	  System.out.println("CM1op ELEMENT: "+((CM1op)cmn).toString());
	  System.out.println("CM1op TYPE: "+((CM1op)cmn).getType());
	  return(find_in_CM(((CM1op)cmn).getNode(), child));
      }
      else if (cmn instanceof CM2op){
	  if (find_in_CM(((CM2op)cmn).getLeft(), child)==1)
	      return 1;
	  else {
	      System.out.println("CM2op TYPE: "+((CM2op)cmn).getType());
	      return(find_in_CM(((CM2op)cmn).getRight(), child));
	  }
	  
      }

      System.err.println("invalid type for CMnode");
      System.exit(0);
      return 0;
}

    /**
     * validate a vector of SchemaUnit generated by parser 
     *
     * @param path the vector contains the path information 
     * @return -1 if validation is successful; otherwise i which is the index which element is failed in the test 
     * @see SchemaUnit
     **/

public int validate(Vector path) {   


    int curr=path.size()-1;
    while (curr>=0) {
    
	Vector currTopSet, currBotSet,  parentTopSet, parentBotSet;
	currTopSet=new Vector();
	currBotSet=new Vector();
	parentTopSet=new Vector();
	parentBotSet=new Vector();
    	
	regExp currExp=((SchemaUnit)(path.elementAt(curr))).getRegExp();
	//String currName=currExp.getVar();
	//System.out.println("currName "+currName);
	if (validate(currExp, currTopSet, currBotSet)==0)
	    return curr;
	
	int parent=((SchemaUnit)(path.elementAt(curr))).getIndex();
	System.out.println("parent "+parent);
	if (parent==-1) {
	    int counter=0;	    
	    for (int i=0; i<currTopSet.size(); i++) {		
		if (!myDtd.isElementDeclared((String)currTopSet.elementAt(i)))
		    counter++;
	    }

	    //no top set element is valid in DTD, invalid path expression
	    if (counter==currTopSet.size())
		return -1;	    
	}
	else {
	    regExp parentExp=((SchemaUnit)(path.elementAt(parent))).getRegExp();
	    if (validate(parentExp, parentTopSet, parentBotSet)==0)
		return curr;
	    //String parentName=parentExp.getVar();
	    //System.out.println("parentName "+parentName);
	    //if (check_1_level(parentName,currName)==0)
	    //return curr;

 int tmp= checkTwoSets(parentBotSet, currTopSet);
System.out.println("result of check two sets"+tmp); 
//if (checkTwoSets(parentBotSet, currTopSet)==0)

        if (tmp==0)	
	    return curr;		
System.out.println(" check two sets is ok");
	}
	curr--;
    }

    //everything is fine
    return -1;
}
    
  public static void main(String[] args) {
    
    if (args.length!=1) {
      System.out.println("specify dtd file");
      System.exit(1);
    }    
    
    data [] d=new data[3];
    d[0]=new data(dataType.IDEN,"cspeople");
    d[1]=new data(dataType.IDEN,"gradstudent");
    d[2]=new data(dataType.IDEN,"url");

    regExp [] reg=new regExp[3];
    reg[0]=new regExpDataNode(d[0]);
    reg[1]=new regExpDataNode(d[1]);
    reg[2]=new regExpDataNode(d[2]);

    Vector sche = new Vector();
    sche.addElement(new SchemaUnit(reg[0],-1));
    sche.addElement(new SchemaUnit(reg[1],0));
    sche.addElement(new SchemaUnit(reg[2],1));
      
    TypeChk typechk=new TypeChk(args[0]);
    System.out.println(typechk.validate(sche));    
  }
}



