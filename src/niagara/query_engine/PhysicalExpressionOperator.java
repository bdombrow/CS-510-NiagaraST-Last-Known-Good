
/**********************************************************************
  $Id: PhysicalExpressionOperator.java,v 1.2 2000/08/28 21:57:54 vpapad Exp $


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

import com.ibm.xml.parser.*;
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

    //////////////////////////////////////////////////////////////////////////
    // These are the private data members of the PhysicalExpressionOperator class //
    //////////////////////////////////////////////////////////////////////////

    // This is the array having information about blocking and non-blocking
    // streams
    //
    private static final boolean[] blockingSourceStreams = { false };

    private ExpressionIF expressionObject; // An object of a clas that implements ExpressionIF
    private String expression; // A string to be interpreted for every tuple
    boolean interpreted;

    ///////////////////////////////////////////////////////////////////////////
    // These are the methods of the PhysicalExpressionOperatorClass                //
    ///////////////////////////////////////////////////////////////////////////

    /**
     * This is the constructor for the PhysicalExpressionOperator class that
     * initializes it with the appropriate logical operator, source streams,
     * destination streams, and responsiveness to control information.
     *
     * @param logicalOperator The logical operator that this operator implements
     * @param sourceStreams The Source Streams associated with the operator
     * @param destinationStreams The Destination Streams associated with the
     *                           operator
     * @param blocking True if the operator is blocking and false if it is
     *                 non-blocking
     * @param responsiveness The responsiveness, in milliseconds, to control
     *                       messages
     */

    public PhysicalExpressionOperator (op logicalOperator,
				 Stream[] sourceStreams,
				 Stream[] destinationStreams,
				 Integer responsiveness) {

	// Call the constructor on the super class
	//
	super(sourceStreams,
	      destinationStreams,
	      blockingSourceStreams,
	      responsiveness);

	// Type cast the logical operator to a Expression operator
	//
	ExpressionOp logicalExpressionOperator = (ExpressionOp) logicalOperator;
	interpreted = logicalExpressionOperator.isInterpreted();

	if (interpreted) {
	    expression = logicalExpressionOperator.getExpression();
	    try {

		String source = "package; import niagara.utils.XMLUtils; import java.util.*; import com.ibm.xml.parser.*; import org.w3c.dom.*;  public class UserExpression extends XMLUtils implements " 
		    + " niagara.xmlql_parser.op_tree.ExpressionIF {\n"  
		    + "public void setupVarTable(HashMap hm) {};\n"
		    + " public org.w3c.dom.Node processTuple(niagara.utils.StreamTupleElement ste) {\n";
		HashMap hm = logicalExpressionOperator.getVarTable(); 
		Iterator variables = hm.keySet().iterator();

		while (variables.hasNext()) {
		    String varname = (String) variables.next();
		    int attrpos = ((Integer) hm.get(varname)).intValue();
		    String vname = varname.substring(1);
		    source += " int "  + vname + " = XMLUtils.getInt(ste, " + attrpos + ");\n";
		}
		source += "int result; " + expression;
		source += "com.ibm.xml.parser.TXElement txe = new com.ibm.xml.parser.TXElement(\"result\");\n" 
		    + "txe.appendChild(new com.ibm.xml.parser.TXText(Integer.toString(result)));\n"
		    + "return txe; }} new UserExpression();";

		// System.out.println("XXX Source is: " + source);
		Interpreter interpreter = new TreeInterpreter(new JavaCCParserFactory());
		expressionObject = (ExpressionIF) 
		    interpreter.interpret(new StringReader(source), "user.java");
	    }

	    catch (Exception e) {
		e.printStackTrace(); // XXX
	    }
	}
	else {
	    Class expressionClass = logicalExpressionOperator.getExpressionClass();
	    // Create an object of the class specified in the logical op
	    try {
		expressionObject = (ExpressionIF) expressionClass.newInstance();
		expressionObject.setupVarTable(logicalExpressionOperator.getVarTable());
	    }
	    catch (Exception e) {
		System.err.println("ExpressionOp: An error occured while constructing an object of the class:\n" 
				   + expressionClass);
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
     * @param result The result is to be filled with tuples to be sent
     *               to destination streams
     *
     * @return true if the operator is to continue and false otherwise
     */

    protected boolean nonblockingProcessSourceTupleElement (
						 StreamTupleElement inputTuple,
						 int streamId,
						 ResultTuples result) {
	Node res = expressionObject.processTuple(inputTuple);
	StreamTupleElement outputTuple = (StreamTupleElement) inputTuple.clone();
	outputTuple.appendAttribute(res);
	// Add the output tuple to the result
	result.add(outputTuple, 0);

	// No problem - continue execution
	return true;
    }    
}
