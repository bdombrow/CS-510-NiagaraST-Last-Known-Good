
/**********************************************************************
  $Id: MyTree.java,v 1.1 2000/05/30 21:03:25 tufte Exp $


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

import java.io.*;
import java.util.*;

/**
 *
 *
 */
public class MyTree
{

    MyNode root;
    String inBlock;       // todo: rearrange
    String predBlock;
    String constructBlock;
    String inWhichElement;
    boolean isFirst;      // first tree instance of a subquery
    int blockIndent;
    static MyTable joinIndexSymbolTable=new MyTable();;
    public static int count=0;

    
    /**
     *
     *
     */
    public MyTree(String inWhich, int indent)
    {
	blockIndent=indent;
	inWhichElement = inWhich;
	isFirst = false;
	//	joinIndexSymbolTable = new MyTable();
    }

    public void setFirstTree(){
	isFirst=true;
    }

 
    String getIn(){
	return inBlock;
    }
    String getPredicates(){
	return predBlock;
    }
    String getConstructs(){
	return constructBlock;
    }

    void printPredicateBlock(String symbol, String pred){
		if( predBlock == null){
			predBlock = new String();
		}
		
		// Leonidas Hack
		String prd = processPred(symbol, pred);

		predBlock += MyQuery.genIndent(blockIndent+1) + 
		    (prd +",\n");
    }

	// Primitive Predicate Processing
	String processPred(String symbol, String pred)
		{
			StringTokenizer st = new StringTokenizer(pred);
			
			String newPred = symbol + " ";
			
			while(st.hasMoreTokens()){
				String nt = st.nextToken();
				if(nt.equalsIgnoreCase("and") || nt.equalsIgnoreCase("or")){
					newPred += nt +" " + symbol;
				} else {
					newPred += nt + " ";
				}
			}
			
			return newPred;
		}
	
    // there should be a higher level func to call this, which will be revursive for nesting
    void printConstructBlock(String name, String symbol){
		if( constructBlock == null)
			constructBlock = new String();
		constructBlock += MyQuery.genIndent(blockIndent+3) +
			("<" + name + ">" + " " + symbol + " </>\n");
    }


    String setSymbol(MyNode t){
	if(t.joinIndex<=0){
	    count++;
	    return new String("$v"+ String.valueOf(count));
	}
	else{
	    int v=joinIndexSymbolTable.lookUp(t.joinIndex);
	    if(v<0){
		joinIndexSymbolTable.set(t.joinIndex, ++count);
		return new String("$v"+ String.valueOf(count));
	    }
	    return new String("$v"+ String.valueOf(v));
	}
	//symbol = ("$v"+ String.valueOf(count));  
    }
    //for attr, impossible to have join
    String setSymbol(){
        count++;
        return new String("$v"+ String.valueOf(count));
        //symbol = ("$v"+ String.valueOf(count));  
    }
    
    boolean print()
    {
	inBlock = new String();
	predBlock = new String();
	constructBlock = new String();	
	traverse(root, blockIndent);

	//inBlock += " IN  \"" + inWhichElement + "\"";
	//conform to the raveen Grammar

	inBlock += " IN  \"*\" conform_to \"" + inWhichElement + "\"";
	return true;
    }


    //String convertName(String xmlFn)
    //{
    //String tmpFn = new String();
    //int tmpIndex = xmlFn.indexOf(".xml");
    //int tmp2=xmlFn.lastIndexOf("/");
    //if(tmpIndex==-1)
    //    return "";
    //if(tmp2==-1) tmp2=0;

    //tmpFn = xmlFn.substring(tmp2, tmpIndex);
    //tmpFn = "http://www.cs.wisc.edu/~czhang/xml"+ tmpFn + ".dtd";
	//tmpFn = "http://www.cs.wisc.edu/~jianming/xml"+ tmpFn + ".dtd";
    //return tmpFn;
    //}

    boolean traverse(MyNode t, int localIndent)
		{
			if(t==null || t.isAttribute) return false;
			if(t!=root){
	    
				if(isFirst){
					isFirst=false;
				}
				else{
					inBlock += "\n"+MyQuery.genIndent(localIndent);
					//System.out.println("genIndent(localIndent)="+MyQuery.genIndent(localIndent)+"end");
					//inBlock += genIndent(localIndent);
				}
	    
				inBlock += "<";
				inBlock += t.name;
				handleAttr(t.children);
				inBlock += ">";
				boolean doElse=true;
	    
				if( (t.predicate != null) && !t.isAttribute){
					String tmp=t.predicate.trim();
					if(tmp.startsWith("=")){
						// inBlock += " " + tmp.substring(1)+ " ";
						doElse=true;// Leonidas Hack added doElse was true
					}
					else if(tmp.startsWith("<") || tmp.startsWith(">")){
						//doElse=true;
					}
					else{
						inBlock += " " + tmp + " ";   
						doElse=false;
					}
				}
				if(doElse){
					if( t.isProjected || (t.predicate != null) || (t.joinIndex>0)){
						t.globalSymbol= setSymbol(t);
						if(! t.isConstructed){
							// if it is not a leaf do not output a variable
							if(t.children == null){
								inBlock += " " + t.globalSymbol + " ";
							}
							if(t.children != null){
								// impossible
								System.out.println("Warning: something wrong with insertion!");
								// return false;
							}
						}
						if(t.isProjected ){
							printConstructBlock(t.name, t.globalSymbol);
						}
		    
						if(t.predicate != null){
							//store mapping ($v, pred)
							printPredicateBlock(t.globalSymbol, t.predicate);
						}
					}
				}
				/*
				  else{
				  inBlock += "\n";
				  }*/
	    
			}
			if(t.children!=null){
				for( int i = 0; i < t.children.size(); i++ ){
					traverse((MyNode) t.children.elementAt(i), (1+localIndent));
				}
			}
			if(t!=root){
				if( (!( t.isProjected || (t.predicate != null) || (t.joinIndex>0)))
					|| t.isConstructed)
				{    
					inBlock += "\n"+MyQuery.genIndent(localIndent);      
				} 
				inBlock += "</>";
				// Leonidas hack for projection
				if(t.children != null && t.isProjected){
					inBlock += " content_as " + t.globalSymbol;
				}
				
				//deal with CONTENT AS, this node has to the direct son of the root.
				if(t.isConstructed){
					if(!( t.isProjected || (t.predicate != null) || (t.joinIndex>0)))
						t.globalSymbol=setSymbol();
					inBlock += " CONTENT_AS "+t.globalSymbol;
				}
			}
			return true;
		}


    /**
     *
     *
     */
    boolean insert(Entry e)
    {
	String name=e.path;
	boolean isDtdLeaf=e.isLeaf;
	boolean isAttribute=e.isAttribute;  // attribute
	boolean isProjected=e.isProjected;
	String  predicate=e.predicate;    // predicate
	int joinIndex=e.joinIndex;
	boolean isConstructed=e.isConstructed;

	// name is of format "a.b.c.d.."
	String nodeName=null;
	StringTokenizer st = new StringTokenizer(name, ".");

	if(root==null){
	    root=new MyNode();
	}
	MyNode curNode=root;
	while (st.hasMoreTokens()) {
	    nodeName=st.nextToken();
	    // System.out.println("name="+nodeName);

	   int index=find(curNode.children, nodeName);

	    if(index==-1){

		if(curNode.children==null){
		    curNode.children=new Vector();
		}

		MyNode toAdd=new MyNode();
		toAdd.name=nodeName;
		if(!st.hasMoreTokens()){
		    toAdd.isDtdLeaf=isDtdLeaf;
		    toAdd.isAttribute=isAttribute;
		    toAdd.predicate=predicate;
		    toAdd.isProjected=isProjected;
		    toAdd.joinIndex=joinIndex;
		    toAdd.isConstructed=isConstructed;

		}

		curNode.children.addElement(toAdd);
		curNode=toAdd;
	    }
	    else{
			curNode=(MyNode) curNode.children.elementAt(index);
	    }
	}
	return true;
    }
    

    /**
     *
     *
     */
    private int find(Vector v, String s)
    {
	if(v==null) return -1;
	for( int i = 0; i < v.size(); i++ ){
	  if((s.compareTo( ((MyNode)(v.elementAt(i))).name ) )==0){
	      return i;
	  }
	}
	return -1;
    }


    /**
     *
     *
     */
    private void handleAttr(Vector v)
    {
	//attr=$v, set ($v, pred) pair
	if( v==null ) return;
	for( int i = 0; i < v.size(); i++ ){
	    MyNode attr = (MyNode)v.elementAt(i);
	    if( attr.isAttribute ){
		attr.globalSymbol=setSymbol();
		String pred = " " + attr.name + "=" + attr.globalSymbol;
		inBlock += pred;
		
		if(attr.predicate==null){
		    // impossible
		    System.out.println("Warning: User selected attribute, but no predicate.");
		}
		else{
		    printPredicateBlock(attr.globalSymbol, attr.predicate);
		}
	    }
	}
    }

    /**
     *
     *
     */
    public String genIndent(int size)
    {
	String s=new String();
	for(int i=0; i<size; i++){
	    s += "\t";
	}
	return s;
    }    
}

