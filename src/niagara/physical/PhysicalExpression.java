/**********************************************************************
  $Id: PhysicalExpression.java,v 1.1 2003/12/24 01:49:01 vpapad Exp $


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

package niagara.physical;

import niagara.logical.*;
import niagara.optimizer.colombia.*;

import org.w3c.dom.*;
import java.io.StringReader;
import niagara.utils.*;

import koala.dynamicjava.interpreter.*;
import koala.dynamicjava.parser.wrapper.*;

/**
 * The <code>PhysicalExpressionOperator</code> class is derived from the abstract class
 * <code>PhysicalOperator</code>. It implements evaluating an arbitrary Expression 
 * on an incoming tuple, producing a new wider outgoing tuple.
 */

public class PhysicalExpression extends PhysicalOperator {
    // No blocking input streams
    private static final boolean[] blockingSourceStreams = { false };

    private Expression expressionOp;

    /** An object of a class that implements ExpressionIF */
    protected ExpressionIF expressionObject;
    /** A string to be interpreted for every tuple */
    private String expression;

    public PhysicalExpression() {
        setBlockingSourceStreams(blockingSourceStreams);
    }

    public void opInitFrom(LogicalOp logicalOperator) {
        // Type cast the logical operator to a Expression operator
        expressionOp = (Expression) logicalOperator;
    }

    public void opInitialize() 
    throws ShutdownException {
        boolean interpreted = expressionOp.isInterpreted();

        if (interpreted) {
            expression = expressionOp.getExpression();
            try {

                String source =
                    "package; import niagara.physical.TupleSchema; import niagara.utils.XMLUtils; import java.util.*; import niagara.ndom.*; import org.w3c.dom.*;  public class UserExpression extends XMLUtils implements "
                        + " niagara.logical.ExpressionIF {\n"
                        + "org.w3c.dom.Document doc = niagara.ndom.DOMFactory.newDocument();"
                         + " public void setupSchema(TupleSchema ts) {};\n"
                        + " public org.w3c.dom.Node processTuple(niagara.utils.Tuple ste) throws Exception {\n";
                Attrs attrs = expressionOp.getVariablesUsed();

                for (int i = 0; i < attrs.size(); i++) {
                    String varname = attrs.get(i).getName();
                    int attrpos = inputTupleSchemas[0].getPosition(varname);
                    source += " int "
                        + varname
                        + " = XMLUtils.getInt(ste, "
                        + attrpos
                        + ");\n";
                }
                source += "int result; " + expression;
                source
                    += "; return doc.createTextNode(String.valueOf(result));\n"
                    + "}} new UserExpression();";

                Interpreter interpreter =
                    new TreeInterpreter(new JavaCCParserFactory());
                expressionObject =
                    (ExpressionIF) interpreter.interpret(
                        new StringReader(source),
                        "user.java");
            } catch (koala.dynamicjava.interpreter.InterpreterException ie) {
                throw new ShutdownException(
                    "invalid expression " + ie.getMessage());
            }
        } else {
            Class expressionClass = expressionOp.getExpressionClass();
            // Create an object of the class specified in the logical op
            try {
                expressionObject = (ExpressionIF) expressionClass.newInstance();
                expressionObject.setupSchema(inputTupleSchemas[0]);
            } catch (InstantiationException ie) {
                System.err.println(
                    "ExpressionOp: An error occured while constructing an object of the class:\n"
                        + expressionClass
                        + " "
                        + ie.getMessage());
            } catch (IllegalAccessException iae) {
                System.err.println(
                    "ExpressionOp: An error occured while constructing an object of the class:\n"
                        + expressionClass
                        + " "
                        + iae.getMessage());
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

    protected void processTuple(
        Tuple inputTuple,
        int streamId)
        throws ShutdownException, InterruptedException {
        Node res = expressionObject.processTuple(inputTuple);
        if(res != null) {
        	Tuple outputTuple =
            	(Tuple) inputTuple.clone();
        	outputTuple.appendAttribute(res);
        	// Add the output tuple to the result
        	putTuple(outputTuple, 0);
        }
    }

    public boolean isStateful() {
        return false;
    }

    /**
     * @see niagara.optimizer.colombia.Op#copy()
     */
    public Op opCopy() {
        PhysicalExpression op = new PhysicalExpression();
        op.expressionOp = expressionOp;
        return op;
    }

    public boolean equals(Object o) {
        if (o == null || !(o instanceof PhysicalExpression))
            return false;
        if (o.getClass() != PhysicalExpression.class)
            return o.equals(this);
        PhysicalExpression other = (PhysicalExpression) o;
        return expressionOp == other.expressionOp;
    }

    public int hashCode() {
        return expressionOp.hashCode();
    }

    /**
     * @see niagara.optimizer.colombia.PhysicalOp#findLocalCost(ICatalog, LogicalProperty[])
     */
    public Cost findLocalCost(
        ICatalog catalog,
        LogicalProperty[] inputLogProp) {
        return new Cost(
            inputLogProp[0].getCardinality()
                * (catalog.getDouble("tuple_reading_cost")
                    + catalog.getDouble("expression_cost")
                    + catalog.getDouble("tuple_construction_cost")));
    }
}
