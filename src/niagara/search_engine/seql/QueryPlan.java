
/**********************************************************************
  $Id: QueryPlan.java,v 1.1 2000/05/30 21:03:28 tufte Exp $


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

import java.util.*;
import java.io.*;
import niagara.search_engine.operators.*;
import niagara.search_engine.indexmgr.*;

/**
 * query execution plan.
 *
 *
 */
public class QueryPlan {
    AbstractOperator op;
    Vector children=new Vector();

    public QueryPlan() {}
    public QueryPlan(AbstractOperator operator) {
	op = operator;
    }

    public void setOperator(AbstractOperator operator) {
	op=operator;
    }

    public void addChild(QueryPlan node) {
	children.addElement(node);
    }
  
    public int numOfChildren() {
	return children.size();
    }
  
    public AbstractOperator getOperator() {
	return op;
    }

    public QueryPlan getChildAt(int index) {
	return (QueryPlan)children.elementAt(index);
    }
   
    public Vector getChildren() {
	return children;
    }

    public void dump() {
	System.out.println(op.toString());
	for(int i=0;i<children.size();i++) {
	    ((QueryPlan)children.elementAt(i)).dump();
	}
    }

    public String makeDot() {
	String dot="";
	String thisNode = "\""+op.toString()+"\"";
	dot+=op.hashCode()+" [label="+thisNode+"];\n";

	for(int i=0;i<children.size();i++) {
	    QueryPlan child=(QueryPlan)children.elementAt(i);
	    dot+=op.hashCode()+"->"+child.getOperator().hashCode()+";\n";
	    dot+=child.makeDot();
	}
	return dot;
    }
    /*
      public String makeDot(int order) {
      String dot="";
      String thisNode = "\""+op.toString()+"\"";
      for(int i=0;i<children.size();i++) {
      QueryPlan child=(QueryPlan)children.elementAt(i);
      String childNode="\""+child.getOperator().toString()+"\"";
      dot+=thisNode+"->"+childNode+";\n";
      dot+=child.makeDot();
      }
      return dot;
      }
    */ 
    public static void writeDot(String dot, Writer writer) {
	PrintWriter pw=null;
	try {
	    pw = new PrintWriter(writer);
	    pw.println("digraph QueryPlan {");
	    //      pw.println("rankdir=LR;\n");
	    pw.println(dot);
	    pw.println("}");
	    pw.close();
	} catch (Exception e) { 
	    e.printStackTrace(); 
	} finally {
	    try {
		if (pw!=null)
		    pw.close();
	    } catch (Exception e) {}
	}
    }

    /*
      public void eval() {
      AbstractOperator[] subops = new AbstractOperator[children.size()];
      for (int i=0;i<children.size();i++) {
      QueryPlan child = (QueryPlan)children.elementAt(i);
      child.eval();
      subops[i]=child.getOperator();
      }

      op.setChildren(subops);
      Thread runner = new Thread(op);
      runner.start();
      }
    */

    public void eval() throws IMException {
	Vector subops = new Vector();
	//System.out.println("eval.."+children.size());
	//System.out.println("children.size()"+children);
	for (int i=0;i<children.size();i++) {

	    QueryPlan child = (QueryPlan)children.elementAt(i);
	    //	    System.out.println(child+" "+i);
      
	    child.eval();
	    subops.addElement(child.getOperator().getResult());
	    //System.out.println(child.getOperator().getResult());
      
	}

	op.addIVLs(subops);
	op.evaluate();
	//System.out.println("op.result()="+op.getResult());

    }
}
  
    






