
/**********************************************************************
  $Id: ExpressionOp.java,v 1.4 2002/10/27 01:20:21 vpapad Exp $


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

package niagara.xmlql_parser.op_tree;

import java.util.ArrayList;
import java.util.HashMap;

import niagara.logical.NodeDomain;
import niagara.logical.Variable;
import niagara.optimizer.colombia.Attribute;
import niagara.optimizer.colombia.ICatalog;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.optimizer.colombia.Op;

public class ExpressionOp extends unryOp {

    private String variableName;
    private Class expressionClass;

    public ExpressionOp() {}
    public ExpressionOp(String variableName, Class expressionClass, String expression) {
        assert (expressionClass == null) ^ (expression == null) 
            : "ExpressionOp needs either an expression, or a class, but not both";
        this.variableName = variableName;
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

    boolean interpreted;     
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

    HashMap varTable;

    public void setVarTable(HashMap varTable) {
	this.varTable = varTable;
    }

    public HashMap getVarTable() {
	return varTable;
    }

    public Class getExpressionClass() {
	return expressionClass;
    }

    public Op copy() {
        return new ExpressionOp(variableName, expressionClass, expression);
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
        if (o == null || !(o instanceof ExpressionOp)) return false;
        if (o.getClass() != ExpressionOp.class) return o.equals(this);
        ExpressionOp other = (ExpressionOp) o;
        if (expression != null && !expression.equals(other.expression)) return false;
        if (expressionClass != null && !expressionClass.equals(other.expressionClass)) return false;
        return variableName.equals(other.variableName);
    }
}

