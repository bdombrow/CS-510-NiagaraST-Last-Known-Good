
/**********************************************************************
  $Id: logNode.java,v 1.6 2002/05/07 03:11:27 tufte Exp $


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


/**
 * This class is used to represent a node in the logical operator tree. Has
 * data members that helps in book-keeping when generation the plan.
 *
 */
package niagara.xmlql_parser.op_tree;

import java.util.*;
import java.io.*;
import java.lang.reflect.Array;

import niagara.xmlql_parser.syntax_tree.*;
import niagara.utils.*;

public class logNode implements java.io.Serializable {
   protected op operator;	// operator

   protected Schema tupleDes;	// describes the tuple
				// captures the parent-child relationship
				// among the different elements scanned so far
   protected varTbl varList;	// list of var used in the subtree rooted at
				// this node. Maps variables to their 
				// schemaAttributes

   protected logNode[] inputs;	// array of inputs or logNode
   
   protected int[] inputsId;    // For Trigger ONLY!
   protected int Id;            // for trig use.  Not trig system Id = -1

    private String location;

    private String name;

    protected boolean isHead;

   /**
    * Constructor without any parameter
    */
   public logNode() {
       operator = null;
       inputs = new logNode[1];
       Id = -1;
   }

   /**
    * Constructor
    *
    * @param the unary operator
    * @param the only input to this operator
    */
   public logNode(op _op, logNode in) {
	inputs = new logNode[1];
	inputs[0] = in;
	operator = _op;
   }
   
   /**
    * Constructor
    *
    * @param the operator
    */
   public logNode(op _op) {
	operator = _op;
        Id = -1;
	inputs = new logNode[] {};
	tupleDes = null;
	varList = null;
   }

   /**
    * Constructor
    *
    * @param the binary operator
    * @param the left subtree
    * @param the right subtree
    */
   public logNode(op _op, logNode leftin, logNode rightin) {
	inputs = new logNode[2];
        Id = -1;
	inputs[0] = leftin;
	inputs[1] = rightin;
	operator = _op;
   }

    public logNode(op operator, logNode[] inputs) {
	this.operator = operator;
	Id = -1;
	this.inputs = inputs;
    }

   public int getId() {
        return Id;
   }

   public void setId(int id) {
       Id = id;
   }

   /**
    * @return the operator
    */
   public op getOperator() {
	return operator;
   }

   public void setOperator(op operator) {
       this.operator = operator;
   }

    public logNode[] getInputs() {
        return inputs;
    }

   /**
    * This function returns the number of inputs to this logical node
    *
    * @return The number of inputs to this logical node
    */

   public int numInputs () {
        return inputs.length;
   }

   /**
    * @return the left subtree
    */
   public logNode leftInput() {
	return input(0);
   }

   /**
    * @return the right subtree
    */
   public logNode rightInput() {
	return input(1);
   }

   public logNode input(int i) {
        // OK.  Trigger trick comes in
        if(inputs==null) return null;
        if(i>=inputs.length) return null; // should throw sth?
        return inputs[i];
        // if(trig==null) return null; // should not happen
        // inputs[i] = trig.findLogNode(inputsId[i]);
        // return inputs[i];
   }

   /**
    * @return the first subtree or the only subtree in case of unary operators
    */
   public logNode input() {
	return input(0);
   }

   /**
    * to set the Nth child
    *
    * @param the root of the subtree
    * @param the position of this child
    */
   public void setInput(logNode newChild, int index) {
	if (index>=Array.getLength(inputs))
		System.err.println("index out of range");
	inputs[index]=newChild;
        if(inputsId==null) return;
        inputsId[index] = newChild.Id;
   }

    public void setInputs(logNode[] inputs) {
        this.inputs = inputs;
    }

   /**
    * @param the Schema of the tuples at this node
    */
   public void setSchema(Schema _td) {
	tupleDes = _td;
   }

   /**
    * @param variable table with the variables encountered so far
    */
   public void setVarTbl(varTbl _vt) {
	varList = _vt;
   }

   /**
    * @return true if the given set of variables is contained in the variables
    *         encountered in the subtree rooted at this node, false otherwise
    */
   public boolean contains(Vector variables) {
	if(varList == null)
		return false;
	return varList.contains(variables);
   }

   /**
    * @return the variable table
    */
   public varTbl getVarTbl() {
	return varList;
   }

   /**
    * @return the Schema created from the elements encountered so far
    */
   public Schema getSchema() {
	return tupleDes;
   }

   /**
    * used for creating a postscript representation of this logical plan
    * tree using the 'dot' command. called recursively on the child nodes.
    *
    * @return String representation for the 'dot' command
    */
   public String makeDot() {
      String dot="";
      String thisNode = "\""+operator.toString()+"\"";
      dot+=operator.hashCode()+" [label="+thisNode+"];\n";
      if(inputs != null)
         for(int i=0;i<inputs.length;i++) {
	    dot+=operator.hashCode()+"->"+inputs[i].getOperator().hashCode()+";\n";
	    dot+=inputs[i].makeDot();
         }
      return dot;
   }

   /**
    * saves the String representation of this tree for the dot command into a
    * file that can be used to generate a postscript file with the graph.
    *
    * @param the String
    * @param the output 
    */
   public static void writeDot(String dot, Writer writer) {
      PrintWriter pw = null;
      try {
	 pw = new PrintWriter(writer);
	 pw.println("digraph QueryPlan {");
	 pw.println(dot);
	 pw.println("}");
	 pw.close();
      } catch (Exception e) {
	 e.printStackTrace();
      } finally {
	 try {
	   if (pw != null)
	      pw.close();
         } catch (Exception e) {}
      }
   }

   /**
    * prints this node to the standard output
    */
   public void dump() {
       dump(new Hashtable());
   }

    protected void dump(Hashtable nodesDumped) {
	if (nodesDumped.containsKey(this))
	    return;
	nodesDumped.put(this, this);

	operator.dump();

	if(inputs != null)
	   for(int i=0; i<inputs.length; i++)
		input(i).dump(nodesDumped);
    }


    // A node is schedulable if none of its
    // inputs depends on an abstract resource
    public boolean isSchedulable() {
        if (inputs.length == 0 && operator instanceof ResourceOp)
            return false;
        for (int i = 0; i < inputs.length; i++) {
            if (!inputs[i].isSchedulable())
                return false;
        }
        return true;
    }

    /**
     * XML representation of this node
     *
     * @return a <code>String</code> with the XML 
     * representation of this operator
     */
    public String toXML() {
        // XXX convert to StringBuffer
        String eltname = operators.getName(operator);
        String ret = "<" + eltname;
        // id = last variable in the variable table
        ret += " id='" + getName() + "'";
        // location
        if (location != null) 
            ret += " location='" + location + "'";
        // inputs
        if (inputs.length != 0) {
            ret += " input='";
            for (int i = 0; i < inputs.length; i++) {
                ret += inputs[i].getName() + " ";
            }
            ret = ret.substring(0, ret.length()-1);
            ret += "'";
        }
        // other attributes for operator
        ret += operator.dumpAttributesInXML();
        String children = operator.dumpChildrenInXML();
        if (children.length() != 0)
            ret += ">" + children + "</" + eltname + ">";
        else
            ret += "/>";
        return ret;
    }

    public String planToXML() {
        StringBuffer buf = new StringBuffer("<plan top='");
        buf.append(getName());
        buf.append("'>");
        subplanToXML(new Hashtable(), buf);
        buf.append("</plan>");
        return buf.toString();
    }

    public String subplanToXML() {
        StringBuffer buf = new StringBuffer("<plan top='send'><send id='send' input='" + getName() + "'/>");
        subplanToXML(new Hashtable(), buf);
        buf.append("</plan>");
        return buf.toString();
    }

    public void subplanToXML(Hashtable seen, StringBuffer buf) {
        if (seen.containsKey(name)) 
            return;
        buf.append(toXML());
        seen.put(name, name);
        for (int i = 0; i < inputs.length; i++) {
            inputs[i].subplanToXML(seen, buf);
        }
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public boolean isAccumulateOp() {
	return (operator instanceof AccumulateOp);
    }

    public String getAccumFileName() {
	if(!isAccumulateOp()) {
	    throw new PEException("Can't get AccumFile name from non-accumulate operator");
	}
	return ((AccumulateOp)operator).getAccumFileName();
    }

    public boolean isHead() {
	return isHead;
    }

    public void setIsHead() {
	isHead = true;
    }

}



