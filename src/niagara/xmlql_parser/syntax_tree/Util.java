
/**********************************************************************
  $Id: Util.java,v 1.2 2002/10/26 21:57:11 vpapad Exp $


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
 * This class has some static methods which are used in the generation
 * of the logical plan
 *
 */
package niagara.xmlql_parser.syntax_tree;

import java.util.*;

import niagara.logical.*;

public class Util {

	/**
	 * used to generate regular expression from a set of identifiers
	 * e.g. the set {author, editor} will give rise to the following
	 * regular expression :
	 *                 |
	 *                / \
	 *               /   \
	 *           author editor
	 *
	 * @param a vector of data objects, each representing a variable or
         *	  an identifier
         * @return an equivalent regular expression
	 */

	public static regExp getEquivRegExp(Vector v) {
		regExp currRegExp = new regExpDataNode((data)v.elementAt(0));
		regExpDataNode reDnode;

		for(int i=1; i<v.size(); i++) {
			reDnode = new regExpDataNode((data)v.elementAt(i));
			currRegExp = new regExpOpNode(opType.BAR,currRegExp,reDnode);
		}

		return currRegExp;
	}

	/**
	 * this function does a breadth first traversal of pattern (that
	 * is element - subelement structure) and finds the position of
	 * the parent of an element for building a Schema for the pattern
	 *
	 * @param pattern to traverse
	 * @return a vector with the elements and parent index
	 */

	public static Vector bft(pattern _pat) {
		pnode currpnode, newpnode;
		pattern currpat;
		Vector patternList;
		Vector attrList;
		patternLeafNode newpattern;
		regExp newregexp;
		attr curattr;

		Vector listOfpnodes = new Vector();
		pnode firstnode = new pnode(_pat,-1);
		listOfpnodes.addElement(firstnode);
		
		for(int i=0;i<listOfpnodes.size();i++) {
			currpnode = (pnode)listOfpnodes.elementAt(i);
			currpat = currpnode.getPattern();
			if(currpat instanceof patternInternalNode) {
				patternList = ((patternInternalNode)currpat).getPatternList();
				for(int j=0;j<patternList.size();j++) {
					newpnode = new pnode((pattern)patternList.elementAt(j),i);
					listOfpnodes.addElement(newpnode);
				}
			}
			attrList = currpat.getAttrList();
			for(int k=0;k<attrList.size();k++) {
				curattr = (attr)attrList.elementAt(k);
				newregexp = new regExpDataNode(new data(dataType.IDEN,curattr.getName()));
				newpattern = new patternLeafNode(newregexp, new Vector(), curattr.getValue(),null);
				newpnode = new pnode(newpattern,i);
				listOfpnodes.addElement(newpnode);
			}
				
		}
		return listOfpnodes;
	}

	/**
	 * generating one predicate by ANDing multiple predicates
	 *
	 * @param vector of predicates to AND
	 * @return predicate resulting from ANDing multiple predicates
	 */

	public static Predicate andPredicates(Vector preds) {
		Predicate curpred = (Predicate) preds.elementAt(0);	
		for(int i=1;i<preds.size();i++) 
			curpred = new And(curpred,(Predicate) preds.elementAt(i));
		return curpred;
	}


	/**
	 * function to merge two schema. used when two schema are to be merged
	 * for the join node. Backpointers in the second schema (except for the
	 * first schema unit that represents the root of the document) are 
         * increased by the size of the first schema
	 *
	 * @param first schema
	 * @param second schema to join with the first one
	 * @return new schema produced by joining the two schemas
	 */

	public static Schema mergeSchemas(Schema leftSchema, Schema rightSchema) {
		
		int leftRelSize = leftSchema.numAttr();
		Schema newSchema = new Schema();
		SchemaUnit new_su, old_su;

		for(int i=0;i<leftRelSize;i++)
			newSchema.addSchemaUnit(new SchemaUnit(leftSchema.getSchemaUnit(i)));
		old_su = rightSchema.getSchemaUnit(0);
		new_su = new SchemaUnit(old_su);
		newSchema.addSchemaUnit(new_su);
		
		for(int i=1;i<rightSchema.numAttr();i++) {
			old_su = rightSchema.getSchemaUnit(i);
			new_su = new SchemaUnit(old_su.getRegExp(),leftRelSize+old_su.getBackPtr());
			newSchema.addSchemaUnit(new_su);
		}

		return newSchema;
	}

	/**
	 * this method is used to merge to variable tables for a join.
	 *
	 * @param first variable table
	 * @param second variable table to join
	 * @param the length by which the attributes in the second table
	 *        needs to be shifted. this is the size of schema for the
	 *        left input stream whose list of variables is given by the
	 *        first table
	 * @return var table produced by joining the two input var table
	 */

	public static varTbl mergeVarTbl(varTbl leftVarTbl, varTbl rightVarTbl, int leftRelSize) {

		String var;
		varToAttr varElem;
		Vector attrList;
		schemaAttribute sa;

		varTbl newVarTbl = new varTbl(leftVarTbl);
		for(int i=0;i<rightVarTbl.size();i++) {
			varElem = rightVarTbl.getVarToAttr(i);
			attrList = varElem.getAttributeList();
			var = varElem.getVar();
			for(int j=0; j<attrList.size();j++) {
				sa = new schemaAttribute((schemaAttribute)attrList.elementAt(j));
				sa.shift(leftRelSize);
				newVarTbl.addVar(var,sa);
			}
		}
		return newVarTbl;
	}

	/**
	 * this method is used to make a single predicate from the equi-join
	 * attributes of the two input stream
	 *
	 * @param attributes of the left relation
	 * @param attributes of the right relation that should be equal to
	 *        the corresponding attribute of the left relation
	 * @return predicate that represent this join condition
	 */

	public static Predicate makePredicate(Vector leftJoinAttr, Vector rightJoinAttr) {
		
		Predicate newPred, rightChild;
		schemaAttribute leftSA, rightSA;
		Atom leftData, rightData;

		if((leftJoinAttr == null) || (rightJoinAttr == null))
		   return null;
		int numAttr = leftJoinAttr.size();

		if(numAttr == 0)
			return null;

		leftSA = new schemaAttribute((schemaAttribute)leftJoinAttr.elementAt(0));
		rightSA = new schemaAttribute((schemaAttribute)rightJoinAttr.elementAt(0));
		rightSA.setStreamId(1);

		leftData = new OldVariable(leftSA);
		rightData = new OldVariable(rightSA);

		newPred = Comparison.newComparison(opType.EQ,leftData,rightData);

		for(int i=1;i<numAttr;i++) {
			leftSA = new schemaAttribute((schemaAttribute)leftJoinAttr.elementAt(i));
			rightSA = new schemaAttribute((schemaAttribute)rightJoinAttr.elementAt(i));
			rightSA.setStreamId(1);

			leftData = new OldVariable(leftSA);
			rightData = new OldVariable(rightSA);
			
			rightChild = Comparison.newComparison(opType.EQ,leftData,rightData);

			newPred = new And(newPred,rightChild);
		}

		return newPred;
	}

	/**
	 * this function checks if a list of variables is contained in the union
	 * of two variable tables. This is used to add join nodes when 
	 * generating a logical plan
	 *
	 * @param first variable table
	 * @param second variable table
	 * @param list of variables to check if it is contained in either of the
	 *        above two tables
	 * @return true if variables are contained in the union, false otherwise
	 */

	public static boolean unionContains(varTbl leftTbl, varTbl rightTbl, Vector variables) {
	   String var;
	   for(int i=0;i<variables.size();i++) {
	      var = (String)variables.elementAt(i);
	      if((leftTbl.lookUp(var) == null) && (rightTbl.lookUp(var) == null))
		 return false;
	   }
	   return true;
	}
}
