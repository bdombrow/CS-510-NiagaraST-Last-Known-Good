/**********************************************************************
  $Id: SEQueryExtractor.java,v 1.2 2002/10/27 02:23:04 vpapad Exp $


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

/**
 * Notes:
 *
 * Issues for pushing sargable predicates:
 *   - regular path expression
 *   - numerical predicates
 *   - negation
 *
 * Issues for pusing containment relationships:
 *   - tag variable
 *
 * Note: currently no attention is paid to generate the "DIRECT_CONTAIN"
 * search engine query operations, only the general "CONTAIN" is
 * generated.
 *
 */

import java.util.*;
import niagara.xmlql_parser.syntax_tree.*;
import niagara.logical.And;
import niagara.logical.Atom;
import niagara.logical.BinaryPredicate;
import niagara.logical.Comparison;
import niagara.logical.Constant;
import niagara.logical.Not;
import niagara.logical.OldVariable;
import niagara.logical.Or;
import niagara.logical.Predicate;
import niagara.logical.Variable;
import niagara.utils.PEException;
import niagara.xmlql_parser.op_tree.*;

/**
 * A SEQueryExtractor extracts containment relationships and sargable
 * predicates from the parsed XML Query logical plan
 */
public class SEQueryExtractor {

    private static final boolean DEBUG = false;

	/**
	 * This function should be called directly by the Query optimizer
	 *
	 * @param selectNodes a collection of select logNode's.  The
	 *        predicates in these select nodes are "AND"ed together.
	 * @param schema  the schema that the selectNodes use, so that we
	 *        can bind the variables to their tag names
	 * @return a search engine query string
	 */
	public static String makeQuery (Vector selectNodes, Schema schema,
		boolean pushSargPredicatesOnly) {

		ContainTreeNode resultTree = null;

		// copy the schema into our own data structure which has more info

		Vector containUnits = extractSchema (schema);
		if (containUnits == null)
			return null;

		if (selectNodes != null && selectNodes.size()>0) {

			// make a tree out of the first select node

			resultTree = makeContainmentTree (
				(logNode)selectNodes.elementAt(0), containUnits);

			// make subtrees for each of the rest of the select nodes,
			// and connect them up into the main tree

			for (int i = 1; i < selectNodes.size(); i++) {

				ContainTreeNode tmptree = makeContainmentTree (
					(logNode)selectNodes.elementAt(i), containUnits);
				if (resultTree == null) {
					resultTree = tmptree;
				}
				else if (tmptree != null) {
					resultTree = makeContainTreeNode (
						opType.AND, resultTree, tmptree);
				}
			}
		}

		if (DEBUG) {
			System.out.println ("tree after select nodes are handled ------------------");
			if (resultTree==null) System.out.println ("tree is null.");
			else resultTree.dump(0);
		}

		if (!pushSargPredicatesOnly) {

			ContainTreeNode containTree = null;

			// now we check all contain units and see if there is any leaf
			// tag unused in constructing tree, if so, we make a tree for
			// it as well and connect it with the main containment tree

			for (int i = 0; i < containUnits.size(); i++) {

				ContainUnit contain_unit = (ContainUnit)
					containUnits.elementAt(i);

				if (contain_unit.numChildren() == 0
				&& !contain_unit.isUsedInConstructTree()) {

					regExp tagexp = contain_unit.getTagExpression();
					ContainTreeNode tmptree = makeContainmentTree (
						tagexp, null, -1, contain_unit.getParent(), 
						new Boolean[1]);

					if (containTree == null) {
						containTree = tmptree;
					}
					else {
						if (tmptree != null);
						containTree = makeContainTreeNode (
							opType.AND, containTree, tmptree);
					}

					contain_unit.setUsedInConstructTree();
				}
			}

			if (containTree != null) {
				resultTree = makeContainTreeNode (
					opType.AND, resultTree, containTree);
			}

			if (DEBUG) {
				if (resultTree==null) System.out.println ("tree is null.");
				else resultTree.dump(0);
			}

			// while the above tree is being built, each leaf node will
			// have recorded in it the corresponding parent in the schema.
			// now we are ready to extract the containment relationships
			// using that information.

			Vector ancestors = extractContainRelations (
								resultTree, containUnits);
			if (DEBUG) {
				if (resultTree==null) System.out.println ("tree is null.");
				else resultTree.dump(0);
			}

			resultTree = addContainNodes (resultTree, ancestors, containUnits);
		}

		if (DEBUG) {
			System.out.println ("the final tree ------------------");
			if (resultTree==null) System.out.println ("tree is null.");
			else resultTree.dump(0);
		}

		// convert the containment tree into a query string and return
		if (resultTree == null) return null;
		else return resultTree.toString();
	}

	/**
	 * Convert the schema units into contain units
	 */
	private static Vector extractSchema (Schema schema) {

		if (schema == null) return null;

		Vector containmentVec = new Vector(); // of ContainUnit

		// walk through the schema units and create ContainUnit's
		for (int i = 0; i < schema.numAttr(); i++) {

			SchemaUnit schema_unit = schema.getSchemaUnit (i);
			regExp tag_exp = schema_unit.getRegExp();
			int parent_index = schema_unit.getIndex();

			// if the tag expression is a tag variable, we ignore it
			if (tag_exp instanceof regExpOpNode
			&& ((regExpOpNode)tag_exp).getOperator() == opType.DOLLAR) {
				tag_exp = null;
			}

			containmentVec.addElement (new ContainUnit(tag_exp, parent_index));

			if (parent_index >= 0) {
				ContainUnit parentUnit = (ContainUnit)
					containmentVec.elementAt(parent_index);
				parentUnit.addChild (i);
			}
		}

		if (DEBUG) {
			for (int i = 0; i < containmentVec.size(); i++) {
				System.out.println ("containment unit "+i+"-----");
				ContainUnit cu = (ContainUnit)containmentVec.elementAt(i);
				cu.dump();
			}
		}

		return containmentVec;
	}

	/**
	 * Make a containment tree out of a SELECT logNode
	 */
	private static ContainTreeNode makeContainmentTree (
		logNode selectNode, Vector containUnits) {

		op operator = selectNode.getOperator();
		Predicate pred = ((selectOp)operator).getPredicate();

		Boolean[] hasNegation = new Boolean[1];
		ContainTreeNode tree = makeContainmentTree (
			pred, containUnits, hasNegation);

		hasNegation = null; // help garbage collector

		return tree;
	}

	/**
	 * Make a containment tree out of a predicate 
	 *
	 * @param pred the predicate to work on
	 * @param containUnits the schema that the predicate uses.
	 *        we need this to bind the variables in the predicate to
	 *        the tag names.
	 * @hasNegation tells the upper level node 
	 *        whether there is a negation imbedded. Predicates with
	 *	      Negations are ignored.
	 *
	 * @return a containment tree
	 */
	private static ContainTreeNode makeContainmentTree (
		Predicate pred, Vector containUnits, Boolean[] hasNegation) {

		if (pred == null) {
			hasNegation[0] = new Boolean(false);
			return null;
		}

		if (pred instanceof Comparison) {
			int predOp = ((Comparison) pred).getOperator();

			// If this predicate has a negation, ignore it
			if (predOp == opType.NEQ) {
				hasNegation[0] = new Boolean(true);
				return null;
			}

			Atom lexp = ((Comparison)pred).getLeft();
			Atom rexp = ((Comparison)pred).getRight();

			// 'lexp' contains info that we can use to bind the 
			// predicate to a contain unit. we find out that contain 
			// unit and get its tag.

			if (! (lexp instanceof Variable)) {
				System.err.print ("BUG in logical query plan tree: ");
				System.err.println ("lexp.value() not schemaAttribute!!");
				hasNegation[0] = new Boolean(false);
				return null;
			}

			int contain_unit_idx = ((schemaAttribute)((OldVariable) lexp).getSA()).getAttrId();
			ContainUnit contain_unit = (ContainUnit)containUnits.elementAt (
								contain_unit_idx);
			if (contain_unit == null) {
				System.err.print ("BUG in logical query plan tree: ");
				System.err.println ("variable bound to null schema unit");
				hasNegation[0] = new Boolean(false);
				return null;
			}

			// make a tree node out of the rexp
			// do it differently for numerical and non-numerical predicates:
			// for numerical predicates, don't push unless it is an 
			// equality predicate; for non-numerical predicates,
			// need to add quotes around the string value

			ContainTreeNode tree_of_rvalue = null;
			boolean rvalueIsNumber = true;

			// check to see if the rexp is another variable, if so,
			// we ignore this predicate

			if (! (rexp instanceof Constant)) {
				hasNegation[0] = new Boolean(false);
				return null;
			}

			// numerical predicate
			try {
				Double dummy = new Double((String)((Constant)rexp).getValue());
				tree_of_rvalue = new ContainTreeNode (
					(String)((Constant) rexp).getValue(),
					contain_unit.getParent());
			}

			// non-numerical predicate
			catch (NumberFormatException e) {
				rvalueIsNumber = false; // it is a string
				if (predOp == opType.EQ) {
					tree_of_rvalue = new ContainTreeNode (
						"\""+ ((Constant) rexp).getValue() +"\"", // value
						contain_unit.getParent());			// ancestor
				}
				else {
					tree_of_rvalue = new ContainTreeNode (
						((Constant) rexp).getValue(),
						contain_unit.getParent());
				}
			}

			// now make a tree out of this predicate:
			// connecting the tag expression and the rvalue with
			// predicate operator

			if (rvalueIsNumber) {
				// ignore non-equality non-muerical predicate
				if (predOp != opType.EQ) {
					hasNegation[0] = new Boolean(false);
					return null;
				}
			}
			else if (predOp == opType.EQ) {
				predOp = opType.IS;
			}

			regExp tagExp = contain_unit.getTagExpression();
			Boolean[] dummy = new Boolean[1];
			ContainTreeNode tree = makeContainmentTree (tagExp, 
				tree_of_rvalue, predOp, contain_unit.getParent(), dummy);
			dummy = null;

			contain_unit.setUsedInConstructTree();

			hasNegation[0] = new Boolean(false);
			return tree;
		}

		else if (!(pred instanceof Comparison)) {

			// if this is a "NOT" predicate, we ignore it
			if (pred instanceof Not) {
				hasNegation[0] = new Boolean(true);
				return null;
			}

			Predicate lchild = ((BinaryPredicate)pred).getLeft();
			Predicate rchild = ((BinaryPredicate)pred).getRight();

			// make a tree from the left child
			Boolean[] leftHasNegation = new Boolean[1];
			ContainTreeNode ltree = makeContainmentTree (
				lchild, containUnits, leftHasNegation);

			// make a tree from the right child
			Boolean[] rightHasNegation = new Boolean[1];
			ContainTreeNode rtree = makeContainmentTree (
				rchild, containUnits, rightHasNegation);

			// if both children have negation, we get nothing
			if (leftHasNegation[0].booleanValue()
			&& rightHasNegation[0].booleanValue()) {

				hasNegation[0] = new Boolean(true);
				return null;
			}

			// if only one child has negation, process depending on the
			// predicate operator:
			// if the op is AND, we get the non-negated child
			// if the op is OR, we get nothing
			else if (leftHasNegation[0].booleanValue()
			|| rightHasNegation[0].booleanValue()) {
	
				ContainTreeNode neg_child = 
					leftHasNegation[0].booleanValue() ? ltree : rtree;
				ContainTreeNode non_neg_child = 
					leftHasNegation[0].booleanValue() ? rtree : ltree;

				if (pred instanceof And) {
					hasNegation[0] = new Boolean(false);
					return non_neg_child;
                                } else if (pred instanceof Or) {
					hasNegation[0] = new Boolean(true);
					return null;
				}
			}

			// neither child has negation, connect the two children
			else {
				hasNegation[0] = new Boolean(false);
                                // XXX vpapad: really ugly
                                int type;
                                if (pred instanceof And) type = opType.AND;
                                else if (pred instanceof Or) type = opType.OR;
                                else throw new PEException("Cannot convert this predicate to an optype");
				return makeContainTreeNode (type, ltree, rtree);
			}
		}

		return null;
	}

	/**
	 * Make a containment tree out of a regular expression
	 *
	 * This function returns two things: the tree that's made, and a
	 * flag indicating to the UPPER LEVEL node whether it should connect
	 * to the tree returned here by direct/indirect containment.
	 * An indirect containment would result if the regular expression 
	 * contains things such as '*', '+', '?'. 
	 * Note: no attention is paid to direct/indirect containment actually
	 * since the search engine does not support direct containment.
	 *
	 * @param regexp the input regular expression (it could be a tree)
	 * @param isDirectContain the flag indicating to the upper level
	 *		node whether a direct/indirect relationship should be
	 *		established.  This is made a Boolean[] to get around the
	 *		problem that Java pass args by value.
	 *		The array contains only one Boolean object. The Boolean
	 *		object is true for direct containment, false for
	 * 		indirect containment.
	 *		Note: this is currently ignored.
	 *
	 * @return root of containment tree
	 */
	private static ContainTreeNode makeContainmentTree (regExp regexp,
								ContainTreeNode subTreeToConnect, 
								int opToConnect,
								int parentContainUnitIdx,
								Boolean[] isDirectContain) {

		isDirectContain[0] = new Boolean(true);

		if (regexp == null) return null;

		ContainTreeNode ret_tree = null; // tree to return

		if (regexp instanceof regExpDataNode) {

			data dx = ((regExpDataNode)regexp).getData();
			Object dx_value = dx.getValue();

			if (dx_value instanceof String) {
				ret_tree = new ContainTreeNode (
					(String)dx_value,
					parentContainUnitIdx);
			}

			if (subTreeToConnect != null) {
				ret_tree = new ContainTreeNode (
					opToConnect, ret_tree, subTreeToConnect);
			}
			return ret_tree;
		}

		else if (regexp instanceof regExpOpNode) {
			// need to traverse the regular expression tree and
			// convert some regular expression operations into
			// containment operations

			int operator = ((regExpOpNode)regexp).getOperator();
			regExp lchild = ((regExpOpNode)regexp).getLeftChild();
			regExp rchild = ((regExpOpNode)regexp).getRightChild();

			if (operator == opType.DOLLAR) {
				return null;
			}

			ContainTreeNode ltree = null;
			ContainTreeNode rtree = null;

			switch (operator) {
			case opType.STAR: // fall thru
			case opType.QMARK: 
				if (rchild != null) {
					System.err.print ("notify chun of assertion failure: ");
					System.err.print (" STAR/QMARK predicate nodes ");
					System.err.println ("could have right child!!");
					System.exit(1);
				}

				// ignore the whole sub tree 
				isDirectContain[0] = new Boolean(false);
				return null;

			case opType.PLUS:
				if (rchild != null) {
					System.err.print ("notify chun of assertion failure: ");
					System.err.print (" STAR predicate nodes could have ");
					System.err.println ("right child!!");
					System.exit(1);
				}
				ret_tree = makeContainmentTree (lchild, 
					subTreeToConnect, opToConnect,
					parentContainUnitIdx, isDirectContain);
				isDirectContain[0] = null; // ignore what passed from below
				isDirectContain[0] = new Boolean(false);

				return ret_tree;

			case opType.DOT:
				rtree = makeContainmentTree (rchild, 
					subTreeToConnect, opToConnect, parentContainUnitIdx,
					isDirectContain);

				if (isDirectContain[0].booleanValue() == true) {
					isDirectContain[0] = null;
					ret_tree = makeContainmentTree (lchild, 
						rtree, opType.DIRECT_CONTAIN, 
						parentContainUnitIdx, isDirectContain);
				}
				else {
					isDirectContain[0] = null;
					ret_tree = makeContainmentTree (lchild, 
						rtree, opType.CONTAIN, 
						parentContainUnitIdx, isDirectContain);
				}

				return ret_tree;

			case opType.BAR:
				ltree = makeContainmentTree (lchild,
				subTreeToConnect, opToConnect, 
				parentContainUnitIdx, new Boolean[1]);

				rtree = makeContainmentTree (rchild,
					subTreeToConnect, opToConnect, 
					parentContainUnitIdx, new Boolean[1]);

				return makeContainTreeNode (opType.OR, ltree, rtree);

			default:
				return null;
			}
		}

		return null;
	}

	/**
	 * @return a list of ancestors of this node
	 */
	private static Vector extractContainRelations (ContainTreeNode tree,
												Vector containUnits) {

		ContainTreeNode lchild = tree.getLeftChild();
		ContainTreeNode rchild = tree.getRightChild();

		if (lchild==null && rchild==null) { // leaf node
			return tree.getAncestors (containUnits);
		}
		else {
			// note that every node in a containment tree either has
			// no child, or has two children

			Vector lancestors = extractContainRelations (lchild, containUnits);
			Vector rancestors = extractContainRelations (rchild, containUnits);

			Vector inter = extractIntersect (lancestors, rancestors);
				// extractIntersect has the side effect of
				// modifying the two vectors

			tree.setAncestors (inter);

			tree.setLeftChild (addContainNodes (
				lchild, lancestors, containUnits));
			tree.setRightChild (addContainNodes (
				rchild, rancestors, containUnits));

			return inter;
		}
	}

    /**
     * Make a ContainTreeNode given an operator type and two children
     */
    private static ContainTreeNode makeContainTreeNode (int optype,
	ContainTreeNode left, ContainTreeNode right) {

	if (left==null && right==null) return null;

	else if (left==null) return right;

	else if (right==null) return left;

	else return new ContainTreeNode(optype, left, right);
    }

    /**
     * Here we require that the two vectors are sorted
     * The common elements are extracted out of the two vectors,
     * and the two vectors are left with the differences.
     */
    private static Vector extractIntersect (Vector vec1, Vector vec2) {
	int i=0, j=0;

	Vector ret_vec = new Vector();

	while (i < vec1.size() && j < vec2.size()) {
	    int num1 = ((Integer)vec1.elementAt(i)).intValue();
	    int num2 = ((Integer)vec2.elementAt(j)).intValue();

	    if (num1==num2) {
		ret_vec.addElement (new Integer(num1));
		vec1.remove (i);
		vec2.remove (j);
	    }
	    else if (num1 > num2) {
		j++;
	    }
	    else { // num1 < num2
		i++;
	    }
	}

	return ret_vec;
    }

	private static ContainTreeNode addContainNodes (ContainTreeNode tree,
													Vector ancestors,
													Vector containUnits) {

		if (ancestors.size()==0) return tree;

		// remove the first ancestor, and build a tree with the rest

		int anc = ((Integer)ancestors.remove(0)).intValue();
		ContainTreeNode rest = addContainNodes(tree, ancestors, containUnits);

		// now add the first ancestor in

		ContainUnit anc_unit = (ContainUnit)containUnits.elementAt(anc);
		regExp tagexp = anc_unit.getTagExpression();
		if (tagexp == null) {
			return rest;
		}
		else {
			ContainTreeNode ret_tree = makeContainmentTree (tagexp, rest, 
				opType.CONTAIN, -1, new Boolean[1]);
			return ret_tree;
		}
	}

}

