/**********************************************************************
  $Id: Expression.java,v 1.1 2003/12/24 02:08:27 vpapad Exp $


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
 * This operator is used to compute an arbitrary expression 
 * over each input tuple.
 *
 */

package niagara.logical;

import java.util.*;

import org.w3c.dom.Element;

import niagara.connection_server.Catalog;
import niagara.connection_server.InvalidPlanException;
import niagara.optimizer.colombia.*;

public class Expression extends UnaryOperator {

    private String variableName;
    private Class expressionClass;
    private Attrs variablesUsed;

    public Expression() {}
    public Expression(String variableName, Attrs variablesUsed, Class expressionClass, String expression) {
        assert (expressionClass == null) ^ (expression == null) 
            : "ExpressionOp needs either an expression, or a class, but not both";
        this.variableName = variableName;
        this.variablesUsed = variablesUsed;
        if (expressionClass != null)
            setExpressionClass(expressionClass);
        if (expression != null)
            setExpression(expression);
    }
    
    /**
     * print the operator to the standard output
     */
    public void dump() {
       System.out.println(this);
    }

    /**
     * dummy toString method
     *
     * @return String representation of the operator
     */
    public String toString() {
       return "Expression: " + expressionClass;
    }

    /**
     * Provide a class that computes the expression
     *
     * @param expressionClass 
     */
    public void setExpressionClass(Class expressionClass) {
	this.expressionClass = expressionClass;
    }

    private boolean interpreted;     
    // If interpreted is set to true, this is the expression
    // (containing variables) that we have to interpret
    String expression; 

    public void setExpression(String expression) {
        interpreted = true;
	this.expression = expression;
    }

    public String getExpression() {
	return expression;
    }

    public boolean isInterpreted() {
	return interpreted;
    }

    public Class getExpressionClass() {
	return expressionClass;
    }

    public Op opCopy() {
        return new Expression(variableName, variablesUsed, expressionClass, expression);
    }

    public void setInterpreted(boolean interpreted) {
        this.interpreted = interpreted;
    }

    /**
     * @see niagara.optimizer.colombia.LogicalOp#findLogProp(ICatalog, ArrayList)
     */
    public LogicalProperty findLogProp(ICatalog catalog, LogicalProperty[] input) {
        LogicalProperty result = input[0].copy();
        Attribute newattr = new Variable(variableName, NodeDomain.getDOMNode());
        result.addAttr(newattr);
        return result;
    }
    
    public boolean equals(Object o) {
        if (o == null || !(o instanceof Expression)) return false;
        if (o.getClass() != Expression.class) return o.equals(this);
        Expression other = (Expression) o;
        if (expression != null && !expression.equals(other.expression)) return false;
        if (expressionClass != null && !expressionClass.equals(other.expressionClass)) return false;
        return variableName.equals(other.variableName) && variablesUsed.equals(other.variablesUsed);
    }
    
    public int hashCode() {
        int hashcode = (expression == null) ? 0 : expression.hashCode();
        hashcode ^= (expressionClass == null) ? 0 : expressionClass.hashCode();
        hashcode ^= variableName.hashCode() ^ variablesUsed.hashCode();
        return hashcode;
    }

    public Attrs getVariablesUsed() {
        return variablesUsed;
    }

    public void loadFromXML(Element e, LogicalProperty[] inputProperties, Catalog catalog)
        throws InvalidPlanException {
        variableName = e.getAttribute("id");
        String classAttr = e.getAttribute("class");
        String exprAttr = e.getAttribute("expression");
        String variablesAttr = e.getAttribute("variables");

        LogicalProperty inputLogProp = inputProperties[0];

        if (!classAttr.equals("")) {
            try {
                expressionClass = Class.forName(classAttr);
            } catch (ClassNotFoundException cnfe) {
                throw new InvalidPlanException(
                    "Class " + classAttr + " could not be found");
            }
        } else if (!exprAttr.equals("")) {
                interpreted = true;
                expression = exprAttr;
        } else
            throw new InvalidPlanException("Either a class, or an expression to be interpreted must be defined for an expression operator");

        variablesUsed = new Attrs();
        if (variablesAttr.equals("*")) {
            variablesUsed = inputLogProp.getAttrs().copy();
        } else {
            StringTokenizer st = new StringTokenizer(variablesAttr);
            while (st.hasMoreTokens()) {
                String varName = st.nextToken();
                Attribute attr = Variable.findVariable(inputLogProp, varName);
                variablesUsed.add(attr);
            }
        }
    }
}

