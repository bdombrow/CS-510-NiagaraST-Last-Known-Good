
/**********************************************************************
  $Id: PredicateEvaluator.java,v 1.5 2002/03/26 23:52:32 tufte Exp $


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
import java.lang.*;
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


    ///////////////////////////////////////////////////////////////////
    // These are the methods of the class                            //
    ///////////////////////////////////////////////////////////////////

    /**
     *  Evaluate a predicate over stream tuple element.
     *
     *  @param tuple the tuple to evaluate the predicate on
     *  @param pred the predicate to use in the evaluation
     *
     *  @return true if predicate is satisfied and false otherwise
     */

    public boolean eval(StreamTupleElement tuple,
			predicate pred) {

	// Add the tuple a newly created array
	//
	oneTuple[0] = tuple;

	// Invoke the evaluation function
	//
	return eval(oneTuple, pred);
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
			StreamTupleElement rightTuple,
			predicate pred) {
	
	// Add the tuples to a newly created array
	//
	twoTuples[0] = leftTuple;
	twoTuples[1] = rightTuple;

	// Invoke the evaluation function
	//
	return eval(twoTuples, pred);
    }


    /**
     *  Evaluate a predicate over stream tuple elements.
     *
     *  @param tuples the tuples to evaluate the predicate on
     *  @param pred the predicate to use in the evaluation
     *
     *  @return true if predicate is satisfied and false otherwise
     */

    private boolean eval(StreamTupleElement[] tuples,
			 predicate pred) {

	// Act based on the type of predicate
	//
	if (pred instanceof predArithOpNode) {

	    // Handle an arithmetic operator node
	    //
	    return evaluateArithmeticPredicate(tuples,
					       (predArithOpNode) pred);
					  
	}
	else if ( pred instanceof predLogOpNode){ 

	    // Handle a logical operator node
	    //
	    return evaluateLogicalPredicate(tuples,
					    (predLogOpNode) pred);
	}
	else if (pred == null) {
	    // Null predicate means cartesian -- always true
	    return true;
	}
	else{
	    System.err.println("ERROR: invalid predicate type");
	    return false;
	}
    }


    /**
     * This function returns the atomic values given a tuple and a
     * schema attribute
     *
     * @param tuple The tuple from which values are to be extracted
     * @param attribute The attribute from which values are to be
     *                  extracted
     *
     * @return A vector of atomic values corresponding to the schema attribute
     */

    public Vector getAtomicValues (StreamTupleElement tuple,
				   schemaAttribute attribute) {

	// Return the atomic values
	//
	oneTuple[0] = tuple;

	return getAtomicValues(oneTuple, attribute);
    }


    /**
     * This function evaluate an arithmetic predicate on a tuple
     *
     * @param tuples The tuples on which the predicate is to be evaluated
     * @param arithmeticPred The arithmetic predicate to be evaluated
     *
     * @return True if the predicate is satisfied and false otherwise
     */

    private boolean evaluateArithmeticPredicate (
						 StreamTupleElement[] tuples,
						 predArithOpNode arithmeticPred) {

	// Extract the values to be compared
	//
	Object leftValue = arithmeticPred.getLeftExp().getValue();
	Object rightValue = arithmeticPred.getRightExp().getValue();
	    
	// Get the vector of atomic values to be compared
	//
	Vector leftAtomicValues = getAtomicValues(tuples, leftValue);
	Vector rightAtomicValues = getAtomicValues(tuples, rightValue);

	// Get the operator from the predicate
	//
	int oper = arithmeticPred.getOperator();

	// Loop over every combination of values and check whether
	// predicate holds
	//
	int numLeft = leftAtomicValues.size();
	int numRight = rightAtomicValues.size();

	for (int left = 0; left < numLeft; ++left) {
	    
               for (int right = 0; right < numRight; ++right) {
                   /*    if(leftAtomicValues.elementAt(left) instanceof TXElement) {
                    System.err.println("L Ele: " +
                            ((TXElement)leftAtomicValues.elementAt(left)).getText());
                }
                else {
                    System.err.println("L Ele: " +
                            leftAtomicValues.elementAt(left));
                }
                
	        if(rightAtomicValues.elementAt(right) instanceof TXElement) {
                    System.err.println("R Ele: " +
                            ((TXElement)rightAtomicValues.elementAt(right)).getText());
                }

                else {
                    System.err.println("R Ele: " +
                           rightAtomicValues.elementAt(right));
                }
  */              
		if (compareAtomicValues(leftAtomicValues.elementAt(left),
					rightAtomicValues.elementAt(right),
					oper) == true) {
		    
		    // The comparison succeeds - return true
		    //
		    return true;
		}
	    }
	}

	// The comparison failed - return false
	//
	return false;
    }


    /**
     * This function evaluate a logical predicate on a tuple
     *
     * @param tuples The tuples on which the predicate is to be evaluated
     * @param logicalPred The logical predicate to be evaluated
     *
     * @return True if the predicate is satisfied and false otherwise
     */

    private boolean evaluateLogicalPredicate (StreamTupleElement[] tuples,
					      predLogOpNode logicalPred) {

	// Extract the sub-predicates
	//
	predicate leftPredicate = logicalPred.getLeftChild();
	predicate rightPredicate = logicalPred.getRightChild();
	
	// Get the operator from the predicate
	//
	int oper = logicalPred.getOperator();
	
	// Make appropriate recursive call 
	//
	switch (oper) {
	    
	case opType.AND: return (eval(tuples, leftPredicate) && 
				 eval(tuples, rightPredicate)); 
	
	case opType.OR: return (eval(tuples, leftPredicate) ||
				eval(tuples, rightPredicate));
	
	case opType.NOT: return !eval(tuples, leftPredicate);
	    
	default: System.err.println("Invalid pridicate: " + logicalPred);
	    return false;
	}
    }
    

    /**
     * This function returns the atomic values associated with a given value
     * in a predicate.
     *
     * @param tuples The tuples over which the value is defined
     * @param value The value in the predicate
     *
     * @return A vector of atomic values associated with the value and tuple
     * KT - looks to me like atomic values are always strings
     */

    private Vector getAtomicValues (StreamTupleElement[] tuples, 
				    Object value) {

	// First create the result vector
	//
	Vector result = new Vector();

	// If the value is of type string, then just add it to result
	//
	if (value instanceof String) {

	    result.addElement((String)value);
	}
	else if (value instanceof schemaAttribute) {

	    // This is a schema attribute - so evaluate path expression
	    // over schema attribute. First typecast
	    //
	    schemaAttribute schema = (schemaAttribute) value;
		
	    // First get the stream id of the tuple
	    //
	    int streamId = schema.getStreamId();

	    // First get the attribute id in the tuple
	    //
	    int attribute = schema.getAttrId();

	    // Now get the path expression
	    //
	    regExp pathExpr = schema.getPath();

	    // Invoke the path expression evaluator to get the nodes reachable
	    // using the path expression
	    //
	    Vector reachableNodes =
		PathExprEvaluator.getReachableNodes(
		         (Node) tuples[streamId].getAttribute(attribute),
			 pathExpr);

	    // For each reachable node, get the atomic value associated with
	    // it
	    //
	    int numReachableNodes = reachableNodes.size();

	    for (int nodeid = 0; nodeid < numReachableNodes; ++nodeid) {

		// First get the node
		//
		Node node = (Node) reachableNodes.elementAt(nodeid);

		// Now get its first child
		//
		Node firstChild = node.getFirstChild();

		// If such a child exists, then add its value to the result
		//
		if (firstChild != null) {
		    
		    result.addElement(firstChild.getNodeValue());
		}
	    }
	}
	else {

	    System.err.println("Unknown value type: Returning no atomic values");
	}

	// Return the constructed result
	//
	return result;
    }


    /**
     * This function compares two atomic values
     *
     * @param leftValue The left value in the comparison
     * @param rightValue The right value in the comparison
     * @param oper The comparison operator
     *
     * @return True if the comparison succeeds and false otherwise
     */

    private boolean compareAtomicValues (Object leftValue,
						Object rightValue,
						int oper) {

	// Check to see whether values exist
	//
	if (leftValue == null || rightValue == null) {
	    System.err.println("A null value passed for Comparison");
	    return false;
	}

	// Do the comparison  based on the operator type
	//
	switch (oper) {
	    
	case opType.NEQ: return notEquals(leftValue, rightValue);
 
	case opType.EQ: return equals(leftValue, rightValue);
 
	case opType.GT: return greaterThan(leftValue, rightValue);
 
	case opType.LT: return lessThan(leftValue, rightValue); 

	case opType.LEQ: return lessThanEquals(leftValue, rightValue); 

	case opType.GEQ: return greaterThanEquals(leftValue, rightValue); 
	    
	default: System.err.println("ERROR: invalid opType for arithOpNode");
	         return false;
	}
    }
    

    /**
     * This function generates a hash key for a tuple given the list of
     * relevant attributes. This is NOT a hash code - it is a key to be
     * input into a hash table. equality of keys => equality of elements
     * this is not true for hash codes. KT - this hashing stuff is hard
     * to understand. This key created isn't really a key, but I don't
     * think that matters because of the way the hash tables are
     * structured - java's native hash table isn't used directly.
     *
     * @param tupleElement The tuple to be hashed on
     * @param attributeList The list of attributes in the tuple to hash on
     *
     * @return The hash key. If any of the attributes in the attribute list
     *         do not have a hashable value, then returns null.
     */
    public String hashKey (StreamTupleElement tupleElement,
			 Vector attributeList) {

	// Create storage for the result
	//
	StringBuffer hashResult = new StringBuffer();

	// For each attribute, get the atomic value and add that to the
	// hash code
	//
	int numAttributes = attributeList.size();

	for (int att = 0; att < numAttributes; ++att) {

	    // Get the atomic value of attribute
	    //
	    oneTuple[0] = tupleElement;

	    Vector atomicValues = 
		getAtomicValues(oneTuple, attributeList.elementAt(att));

	    // If there is not exactly one atomic value, then it is an error
	    //
	    if (atomicValues.size() != 1) {
		throw new PEException("More than one atomic value in hashCode eval");
	    }

	    // Add the atomic value (a string) to the current result
	    //
	    hashResult.append('<');
	    hashResult.append((String) atomicValues.elementAt(0));
	    hashResult.append('<');
	}

	// Return the hash result
	//
	return hashResult.toString();
    }

    /**
     *  The equality operator.  Compares different types of objects for equality.
     *
     *  @param leftValue The left value for the equality comparison
     *  @param rightValue The right value for the equality comparison
     *
     *  @return True if the two objects are equal, false otherwise
     */

    public boolean equals (Object leftValue, Object rightValue) {

	// Only string comparisons supported currently
	//
	if (!(leftValue instanceof String && rightValue instanceof String)) {

	    System.err.println("Non-string comparison attempted");
	    return false;
	}
	  
	// Typecast values to strings
	//
	String leftString = (String) leftValue;
	String rightString = (String) rightValue;

	// First try numeric comparison
	//
	if (numericCompare(leftString, rightString, opType.EQ)) {
	    return true;
	}

	// If that fails, check if string are equal using string comparison
	//
	return leftString.equals(rightString);
    }
    

    /**
     *  The deep equality operator.  Compares different types of 
     *  objects for deep equality.  For instance, test if book a = 
     *  book b where book is a complex element.
     *
     *  @param leftValue The left value for the equality comparison
     *  @param rightValue The right value for the equality comparison
     *
     *  @return True if the two objects are equal, false otherwise
     */

    public boolean deepEquals (Object leftValue, Object rightValue) {

	// Currently not implemented - just returns false
	//
	return false;
    }


    /**
     *  The greater than operator. ">"
     *
     *  @param leftValue The left value for the equality comparison
     *  @param rightValue The right value for the equality comparison
     *
     *  @return True if left object > right object, false otherwise
     */

    public boolean greaterThan (Object leftValue, Object rightValue) {

	// Only string comparisons supported currently
	//
	if (!(leftValue instanceof String && rightValue instanceof String)) {

	    System.err.println("Non-string comparison attempted");
	    return false;
	}
	  
	// Typecast values to strings
	//
	String leftString = (String) leftValue;
	String rightString = (String) rightValue;

	// First try numeric comparison
	//
	if (numericCompare(leftString, rightString, opType.GT)) {
	    return true;
	}

	// Now check greater than using string comparison
	//
	if (leftString.compareTo(rightString) > 0) {
	    return true;
	}
	else {
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

    public boolean lessThan (Object leftValue, Object rightValue) {

	// Only string comparisons supported currently
	//
	if (!(leftValue instanceof String && rightValue instanceof String)) {

	    System.err.println("Non-string comparison attempted");
	    return false;
	}
	  
	// Typecast values to strings
	//
	String leftString = (String) leftValue;
	String rightString = (String) rightValue;

	// First try numeric comparison
	//
	if (numericCompare(leftString, rightString, opType.LT)) {
	    return true;
	}

	// Now check greater than using string comparison
	//
	if (leftString.compareTo(rightString) < 0) {
	    return true;
	}
	else {
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

    public boolean greaterThanEquals (Object leftValue, Object rightValue) {

	// Either greater than or equal
	//
	return (equals(leftValue, rightValue) ||
		greaterThan(leftValue, rightValue));
    }


    /**
     *  The less than or equal operator. "<="
     *
     *  @param leftValue The left value for the equality comparison
     *  @param rightValue The right value for the equality comparison
     *
     *  @return True if left object <= right object, false otherwise
     */

    public boolean lessThanEquals (Object leftValue, Object rightValue) {

	// Either less than or equal
	//
	return (equals(leftValue, rightValue) ||
		lessThan(leftValue, rightValue));
    }

    
    /**
     *  The not equal operator. "!="
     *
     *  @param leftValue The left value for the equality comparison
     *  @param rightValue The right value for the equality comparison
     *
     *  @return True if left object != right object, false otherwise
     */

    public boolean notEquals (Object leftValue, Object rightValue) {

	// The negation of equal
	//
	return (!equals(leftValue, rightValue));
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

    private boolean numericCompare (String leftVal, 
					   String rightVal, 
					   int opCode) {	
	double rightDouble = -1;
	double leftDouble  = -1;

	// Convert to doubles if possible, if this fails, return false
	//
	try{
	    rightDouble = new Double(rightVal).doubleValue();
	    leftDouble  = new Double(leftVal).doubleValue();
	}
	catch(java.lang.NumberFormatException e){
	    return false;
	}
	
	// Do the comparision based on numeric values
	//
	switch (opCode){
	    
	case opType.EQ:   return ( leftDouble == rightDouble );
	case opType.NEQ:  return ( leftDouble != rightDouble );
	case opType.GT:   return ( leftDouble > rightDouble );
	case opType.LT:   return ( leftDouble < rightDouble );
	case opType.LEQ: return ( leftDouble <= rightDouble );
	case opType.GEQ: return ( leftDouble >= rightDouble );
	    
	default:   return false;
	}
    }

}
