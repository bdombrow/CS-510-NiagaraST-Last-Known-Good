/**********************************************************************
  $Id: PhysicalExpressionOperator.java,v 1.8 2002/10/24 23:27:53 vpapad Exp $


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

import niagara.ndom.*;
import niagara.optimizer.colombia.LogicalOp;

import org.w3c.dom.*;
import java.util.*;
import java.io.StringReader;
import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;

import koala.dynamicjava.interpreter.*;
import koala.dynamicjava.parser.wrapper.*;

/**
 * The <code>PhysicalExpressionOperator</code> class is derived from the abstract class
 * <code>PhysicalOperator</code>. It implements evaluating an arbitrary Expression 
 * on an incoming tuple, producing a new wider outgoing tuple.
 *
 * @version 1.0
 */

public class PhysicalExpressionOperator extends PhysicalOperator {
    // This is the array having information about blocking and non-blocking
    // streams
    //
    private static final boolean[] blockingSourceStreams = { false };

    private ExpressionIF expressionObject; // An object of a class that implements ExpressionIF
    private String expression; // A string to be interpreted for every tuple
    boolean interpreted;

    public PhysicalExpressionOperator() {
        setBlockingSourceStreams(blockingSourceStreams);
    }
    
    public void initFrom(LogicalOp logicalOperator) {
	// Type cast the logical operator to a Expression operator
	//
	ExpressionOp logicalExpressionOperator = (ExpressionOp) logicalOperator;
	interpreted = logicalExpressionOperator.isInterpreted();

	if (interpreted) {
	    expression = logicalExpressionOperator.getExpression();
	    try {

		String source = "package; import niagara.utils.XMLUtils; import java.util.*; import niagara.ndom.*; import org.w3c.dom.*;  public class UserExpression extends XMLUtils implements " 
		    + " niagara.xmlql_parser.op_tree.ExpressionIF {\n"  
		    + "public void setupVarTable(HashMap hm) {};\n"
		    + " public org.w3c.dom.Node processTuple(niagara.utils.StreamTupleElement ste) throws Exception {\n";
		HashMap hm = logicalExpressionOperator.getVarTable(); 
		Iterator variables = hm.keySet().iterator();

		while (variables.hasNext()) {
		    String varname = (String) variables.next();
		    int attrpos = ((Integer) hm.get(varname)).intValue();
		    String vname = varname.substring(1);
		    source += " int "  + vname + " = XMLUtils.getInt(ste, " + attrpos + ");\n";
		}
		source += "int result; " + expression;
  		source += "; org.w3c.dom.Element e = (org.w3c.dom.Element) ste.getAttribute(1); return e.getOwnerDocument().createTextNode(Integer.toString(result));\n"
  		    + "}} new UserExpression();";

		Interpreter interpreter = new TreeInterpreter(new JavaCCParserFactory());
		expressionObject = (ExpressionIF) 
		    interpreter.interpret(new StringReader(source), "user.java");
	    } catch(koala.dynamicjava.interpreter.InterpreterException ie) {
		throw new UserErrorException("invalid expression " +
					      ie.getMessage());
	    }
	}
	else {
	    Class expressionClass = logicalExpressionOperator.getExpressionClass();
	    // Create an object of the class specified in the logical op
	    try {
		expressionObject = (ExpressionIF) expressionClass.newInstance();
		expressionObject.setupVarTable(logicalExpressionOperator.getVarTable());
	    } catch(InstantiationException ie) {
		System.err.println("ExpressionOp: An error occured while constructing an object of the class:\n" 
				   + expressionClass +
				   " " + ie.getMessage());
	    } catch (IllegalAccessException iae) {
		System.err.println("ExpressionOp: An error occured while constructing an object of the class:\n" 
				   + expressionClass +
				   " " + iae.getMessage());
	    }
	}
    }


    /**
     * This function processes a tuple element read from a source stream
     * when the operator is non-blocking. This over-rides the corresponding
     * function in the base class.
     *
     * @param inputTuple The tuple element read from a source stream
     * @param streamId The source stream from which the tuple was read
     *
     * @exception ShutdownException query shutdown by user or execution error
     */

    protected void nonblockingProcessSourceTupleElement (
					StreamTupleElement inputTuple,
					int streamId)
	throws ShutdownException, InterruptedException {
	Node res = expressionObject.processTuple(inputTuple);
	StreamTupleElement outputTuple = (StreamTupleElement) inputTuple.clone();
	outputTuple.appendAttribute(res);
	// Add the output tuple to the result
	putTuple(outputTuple, 0);
    }    

    public boolean isStateful() {
	return false;
    }
}
