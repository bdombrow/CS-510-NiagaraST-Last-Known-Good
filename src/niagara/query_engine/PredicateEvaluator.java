/**********************************************************************
  $Id: PredicateEvaluator.java,v 1.9 2002/09/09 16:43:33 ptucker Exp $


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

import org.w3c.dom.*;
import java.io.*;
import java.util.*;
import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;

/**
 *  The predicate evaluator class provides functions to evaluate
 *  operators over xml elements (or attributes).  The predicates
 *  output from the XML parser (non-optimized) will never contain
 *  path expressions (they will not "drill down" into a given field)
 *  However, the optimizer may decide it is cheaper to drill down 
 *  rather than to do scans with associations for every filed.  The 
 *  following example shows how predicates will be used in the 
 *  parser/optimizer
 *
 *
 *  <PRE>
 *
 *       Find all books where (author or editor) name == 'joe';
 *
 *       WHERE <Book>  ELEMENT_AS $b 
 *               <Author|Editor>
 *                  <Name>$n</>
 *               </>
 *             </> IN a.xml
 *       $n = "joe"
 *      
 *       CONSTRUCT <result> $b </>
 *  
 *       The Parser will produce a predicate that looks like:
 *
 *                    ==
 *                  /    \
 *                 /      \
 *                2       "joe"
 *           path=null
 *
 *       and genetare scan ops that will produce a tuple like
 *
 *   
 *           0             1               2
 *       -----------------------------------------
 *       |        |                   |          |
 *       |  Book  |  (Author|Editor)  |  Name    |
 *       |        |                   |          |
 *       -----------------------------------------
 *
 *       The Optimizer may choose to compress the scan ops and apply 
 *       part of the scan in the predicate evaluation, for instance we
 *       may only scan book and apply the following predicate.
 *
 *
 *                       ==
 *                   /        \
 *                  /          \
 *                 /            \
 *                /              \
 *               0               "joe"
 *    path= (Author|Editor).Name
 *
 *
 *           0     
 *       ----------
 *       |        |
 *       |  Book  |
 *       |        |
 *       ----------
 *  </PRE>
 *
 */

public class PredicateEvaluator {

    // KT - I changed this class to having instance variables and not having
    // all static method, to allow the variables below which avoid lots of
    // unnecessary allocations
    private StreamTupleElement[] oneTuple = new StreamTupleElement[1];
    private StreamTupleElement[] twoTuples = new StreamTupleElement[2];

    private Predicate p;

    public PredicateEvaluator(predicate pred) {
        p = parsePredicate(pred);
    }

    /**
     *  Evaluate a predicate over stream tuple element.
     *
     *  @param tuple the tuple to evaluate the predicate on
     *  @param pred the predicate to use in the evaluation
     *
     *  @return true if predicate is satisfied and false otherwise
     */

    public boolean eval(StreamTupleElement tuple) {
	// Add the tuple a newly created array
	oneTuple[0] = tuple;

	return p.evaluate(oneTuple);
    }


    /**
     *  Evaluate a predicate over two stream tuple elements.
     *
     *  @param leftTuple the left tuple to evaluate the predicate on
     *  @param rightTuple the right tuple to evaluate the predicate on
     *  @param pred the predicate to use in the evaluation
     *
     *  @return true if predicate is satisfied and false otherwise
     */

    public boolean eval(StreamTupleElement leftTuple,
			StreamTupleElement rightTuple) {
	// Add the tuples to a newly created array
	//
	twoTuples[0] = leftTuple;
	twoTuples[1] = rightTuple;

	return p.evaluate(twoTuples);
    }


    private Predicate parsePredicate(predicate pred) {
	if (pred instanceof predArithOpNode) {
            predArithOpNode aop = (predArithOpNode) pred;
	    return new Comparison(aop.getOperator(), 
                                  aop.getLeftExp().getValue(), 
                                  aop.getRightExp().getValue());
        }
	else if (pred instanceof predLogOpNode) {
            predLogOpNode lop = (predLogOpNode) pred;
            switch (lop.getOperator()) {
            case opType.AND: 
                return new And(parsePredicate(lop.getLeftChild()), 
                               parsePredicate(lop.getRightChild()));
            case opType.OR: 
                return new Or(parsePredicate(lop.getLeftChild()), 
                               parsePredicate(lop.getRightChild()));
            case opType.NOT: 
                return new Not(parsePredicate(lop.getChild()));
            default: 
                throw new PEException("Unknown boolean operator");
            }
        }
	else if (pred == null) 
	    // Null predicate means cartesian -- always true
	    return new True();
	else throw new PEException("Invalid predicate type");
    }
}


abstract class Predicate {
    abstract boolean evaluate(StreamTupleElement[] tuples);
}

class And extends Predicate {
    Predicate left, right;

    And(Predicate left, Predicate right) {
        this.left = left;
        this.right = right;
    }
    
    boolean evaluate(StreamTupleElement[] tuples) {
        return left.evaluate(tuples) && right.evaluate(tuples);
    }
}

class Or extends Predicate {
    Predicate left, right;

    Or(Predicate left, Predicate right) {
        this.left = left;
        this.right = right;
    }
    
    boolean evaluate(StreamTupleElement[] tuples) {
        return left.evaluate(tuples) || right.evaluate(tuples);
    }
}

class Not extends Predicate {
    Predicate pred;
    
    Not(Predicate pred) {
        this.pred = pred;
    }

    boolean evaluate(StreamTupleElement[] tuples) {
        return (! pred.evaluate(tuples));
    }
}

class True extends Predicate {
    boolean evaluate(StreamTupleElement[] tuples) {
        return true;
    }
}

class Comparison extends Predicate {
    int operator;
    AtomicEvaluator leftAV, rightAV;

    private ArrayList leftValues;
    private ArrayList rightValues;

    Comparison(int operator, Object leftValue, Object rightValue) {
        this.operator = operator;
        leftAV = new AtomicEvaluator(leftValue);
        rightAV = new AtomicEvaluator(rightValue);

        leftValues = new ArrayList();
        rightValues = new ArrayList();
    }

    /**
     * This function evaluate an arithmetic predicate on a tuple
     *
     * @param tuples The tuples on which the predicate is to be evaluated
     * @param arithmeticPred The arithmetic predicate to be evaluated
     *
     * @return True if the predicate is satisfied and false otherwise
     */

    boolean evaluate(StreamTupleElement[] tuples) {
	// Get the vector of atomic values to be compared
	//
        leftValues.clear();
        rightValues.clear();

	leftAV.getAtomicValues(tuples, leftValues);
	rightAV.getAtomicValues(tuples, rightValues);

	// Loop over every combination of values and check whether
	// predicate holds
	//
	int numLeft = leftValues.size();
	int numRight = rightValues.size();

	for (int left = 0; left < numLeft; ++left) {
               for (int right = 0; right < numRight; ++right) {
		if (compareAtomicValues((String)leftValues.get(left),
					(String)rightValues.get(right))) {
		    // The comparison succeeds - return true
		    return true;
		}
	    }
	}

	// The comparison failed - return false
	return false;
    }

    /**
     *  The equality operator.  Compares different types of objects for equality.
     *
     *  @param leftValue The left value for the equality comparison
     *  @param rightValue The right value for the equality comparison
     *
     *  @return True if the two objects are equal, false otherwise
     */

    boolean stringEquals (String leftValue, String rightValue) {
	return leftValue.equals(rightValue);
    }
    
    /**
     *  The greater than operator. ">"
     *
     *  @param leftValue The left value for the equality comparison
     *  @param rightValue The right value for the equality comparison
     *
     *  @return True if left object > right object, false otherwise
     */

    boolean stringGreaterThan (String leftValue, String rightValue) {

	// Now check greater than using string comparison
	if (leftValue.compareTo(rightValue) > 0) {
	    return true;
	} else {
	    return false;
	}
    }
    

    /**
     *  The less than operator. "<"
     *
     *  @param leftValue The left value for the equality comparison
     *  @param rightValue The right value for the equality comparison
     *
     *  @return True if left object < right object, false otherwise
     */

    boolean stringLessThan (String leftValue, String rightValue) {

	// Now check greater than using string comparison
	if (leftValue.compareTo(rightValue) < 0) {
	    return true;
	} else {
	    return false;
	}
    }


    /**
     *  The greater than or equal operator. ">="
     *
     *  @param leftValue The left value for the equality comparison
     *  @param rightValue The right value for the equality comparison
     *
     *  @return True if left object >= right object, false otherwise
     */

    boolean stringGreaterThanEquals (String leftValue, String rightValue) {

	// Either greater than or equal
	return (stringEquals(leftValue, rightValue) ||
		stringGreaterThan(leftValue, rightValue));
    }


    /**
     *  The less than or equal operator. "<="
     *
     *  @param leftValue The left value for the equality comparison
     *  @param rightValue The right value for the equality comparison
     *
     *  @return True if left object <= right object, false otherwise
     */

    boolean stringLessThanEquals (String leftValue, String rightValue) {

	// Either less than or equal
	return (stringEquals(leftValue, rightValue) ||
		stringLessThan(leftValue, rightValue));
    }
    
    /**
     *  The Contains operator. "Contains"
     *
     *  @param leftValue The containing string
     *  @param rightValue The string to find in the leftValue
     *
     *  @return True if right is contained in the left, false otherwise
     */

    boolean stringContains (String leftValue, String rightValue) {
	return (leftValue.indexOf(rightValue) != -1);
    }
    
    /**
     *  The numeric compare function is used to attempt to compare two strings
     *  as numeric double values
     *
     *  @param leftVal The left string for the comparison
     *  @param rightVal The right string for the comparison
     *  @param opCode The comparison operator to use
     *
     *  @return boolean value, true if comparison succeeds
     */

    private boolean numericCompare (String leftVal, String rightVal) 
	throws java.lang.NumberFormatException {
	
	double rightDouble = -1;
	double leftDouble  = -1;

	// Convert to doubles if possible, if this fails, an
	// exception will be thrown
	rightDouble = new Double(rightVal).doubleValue();
	leftDouble  = new Double(leftVal).doubleValue();
	
	switch (operator) { 
	    
	case opType.EQ:   return ( leftDouble == rightDouble );
	case opType.NEQ:  return ( leftDouble != rightDouble );
	case opType.GT:   return ( leftDouble > rightDouble );
	case opType.LT:   return ( leftDouble < rightDouble );
	case opType.LEQ: return ( leftDouble <= rightDouble );
	case opType.GEQ: return ( leftDouble >= rightDouble );
	    //Ugly hack for containment
        case opType.CONTAIN: throw new java.lang.NumberFormatException();
	    
	default:   return false;
	}
    }

    /**
     * This function compares two atomic values. KT - atomic values
     * are always strings.
     *
     * @param leftValue The left value in the comparison
     * @param rightValue The right value in the comparison
     * @param oper The comparison operator
     *
     * @return True if the comparison succeeds and false otherwise
     */

    private boolean compareAtomicValues (String leftValue, 
                                         String rightValue) {
	// Check to see whether values exist
	if (leftValue == null || rightValue == null)
	    throw new PEException("A null value passed for Comparison");

	// first try a numeric comparison, if the conversion to
	// numbers fail, we try a string comparison
	try {
	    return numericCompare(leftValue, rightValue);
	} catch (java.lang.NumberFormatException e) {
	    switch (operator) {
		
	    case opType.NEQ: return !stringEquals(leftValue, rightValue);
		
	    case opType.EQ: return stringEquals(leftValue, rightValue);
			
	    case opType.GT: return stringGreaterThan(leftValue, rightValue);
		
	    case opType.LT: return stringLessThan(leftValue, rightValue); 
		
	    case opType.LEQ: 
		return stringLessThanEquals(leftValue,rightValue); 
		
	    case opType.GEQ: 
		return stringGreaterThanEquals(leftValue, rightValue); 

	    case opType.CONTAIN:
		return stringContains(leftValue, rightValue);
		
	    default: 
		throw new PEException("ERROR: invalid opType for arithOpNode");
	    }
	}
    }
}

