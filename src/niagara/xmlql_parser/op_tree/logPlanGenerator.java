
/**********************************************************************
  $Id: logPlanGenerator.java,v 1.2 2002/05/23 06:32:03 vpapad Exp $


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
*
* This class generates logical plan from a syntax tree for XML-QL. There is
* no optimization. It uses simple hurestic to first generate a sequence of
* Scan nodes for each element to scan and then add a Select node if the 
* select predicates are over the elements in one chain, merges two chains by
* a Join node if there are some predicates with variables in both the chains.
* Finally there are Construct nodes at the top.
* 
*
*/
package niagara.xmlql_parser.op_tree;

import java.util.*;
import java.io.*;
import niagara.xmlql_parser.syntax_tree.*;

public class logPlanGenerator {
	private Vector listOfInClause;   // list of in clauses ( IN *.xml)
	private Vector listOfTagVar;     // list of tag variables like $A in
					 //   {author, editor}
	private Vector listOfPredicates; // list of predicates like
					 //   $A = "Ritchie"
	private Vector listOfConstruct;  // list of constructs created from the
					 //   pattern in the construct part
	private constructBaseNode constructPart; // the construct clause
	private	Vector partialTree;	 // list of partial trees, at the end
					 //   only one tree is left as its first
					 //   element

	private static int tmpvarnum = 0; // for the generation of 
					  //   temp variables

        /**
	 * Constructor
	 *
	 * @param the syntax tree for the XML-QL query
	 */
	public logPlanGenerator(query q) {
		listOfInClause = new Vector();
		listOfTagVar = new Vector();
		listOfPredicates = new Vector();
		partialTree = new Vector();
		listOfConstruct = new Vector();

		condition cond;

		// there are three type of conditions: set like "$v In {..}",
		// In Clause like "<book>..</> IN book.xml", and predicates
		// like "$v = "Ritchi"". These are conditions are separated
		// into three groups depending on their types
		Vector listOfCond = q.getConditions();
		for(int i=0; i<listOfCond.size(); i++) {
			cond = (condition)listOfCond.elementAt(i);
			if (cond instanceof inClause)
				listOfInClause.addElement((inClause)cond);
			else if(cond instanceof set)
				listOfTagVar.addElement((set)cond);
			else if(cond instanceof predicate)
				listOfPredicates.addElement((predicate)cond);
		}

		// besides these conditions there is a construct part
		constructPart = q.getConstructPart();
	}

	/**
	 * gives an equivalent regular expression for the tag variables.
	 * $a IN {author, editor} will give the following regular expression
	 * for the tag variable $a :         |
	 *                                  / \
	 *                             author editor
	 *
	 * @param the variable
	 * @return the equivalent regular expression
	 */
	private regExp valueOfTagVar(String var) {
		set set_var;
	
		// go through the list of tag variables, if the variable
		// name matches, then return the equivalent regular expression
		for(int i=0; i<listOfTagVar.size(); i++) {
			set_var = (set)listOfTagVar.elementAt(i);
			if(var.equals(set_var.getVar()))
				return set_var.getRegExp();
		}
		
		// if the tag variable is not bound to a list of identifiers
		// by the set IN, then just return a regular expression with the
		// dollar operator which implies that it stands for any tag name
		return new regExpOpNode(opType.DOLLAR);
	}

	/**
	 * used for generating temporary variables
	 *
	 * @return temporary variable
	 */
	private String getTmpVar() {
		String var = new String("#"+tmpvarnum);
		tmpvarnum++;
		return var;
	}

        /**
	 * generates the partial tree which is a chain of Scan nodes with the
	 * dtdScan as the leaf node for an IN clause.
	 * The IN clause <book> <author> </> </> IN book.xml will give the
	 * following chain:
	 *   Scan book
	 *       ^
	 *   Scan author
	 *       ^
	 *   dtdScan book.xml
	 *
         * @param the IN clause
	 * @return the partial tree
	 */
	private logNode genLogPlan(inClause inc) {
		
		logNode childLogNode, curLogNode;
		Vector listOfpnodes;
		Schema tupleDes = new Schema();
		varTbl varList = new varTbl();

		SchemaUnit su;
		pnode currpnode;
		int backPtr, type;
		pattern curpat;
		regExp curregExp;
		data expData, leafData, curbd, varData;
		String var;
		schemaAttribute schattr;
		scanOp scan;
	
		// get the list of XML data sources
		Vector vec=inc.getSources();

		// create the leaf dtdScan operator for reading and parsing
		// the XML data sources
		dtdScanOp dtdOp = new dtdScanOp();
		dtdOp.setDtdScan(vec, inc.getDtdType());
		curLogNode = childLogNode = new logNode(dtdOp);

		// As we build the table we construct the Schema that describes
		// the tuple to store pointers to different elements of the
		// DOM tree. Initially this tuple will just store the pointer
		// to the DOM tree returned by the dtdScan operator
		tupleDes.addSchemaUnit(new SchemaUnit(null,-1)); 

		// get the n-arry tree that represents the pattern of the
		// IN clause and do a breadth first traversal to generate a
		// flat structre i.e. a list of elements with the back pointers
		// for the parents.
		pattern pat = inc.getPattern();
		listOfpnodes = Util.bft(pat);

		// Schema is generated by adding the regular
		// expression at the end with
		// a back pinter pointing to the parent
		for(int i=0; i<listOfpnodes.size();i++) {
			currpnode = (pnode)listOfpnodes.elementAt(i);
			// we add one, because root of the document is at
			// the 0th position
			backPtr = currpnode.getParent() + 1;
			curpat = currpnode.getPattern();
			
			curregExp = curpat.getRegExp();
			if(curregExp instanceof regExpDataNode) {
				expData = ((regExpDataNode)curregExp).getData();
				type = expData.getType();

				// if the tag name is a variable, then replace
				// it with a equivalent regular expression and
				// add it with its schemaAttribute (position
				// in the schema) to the variable table
				if(type == dataType.VAR) {
					var = (String)expData.getValue();
					curregExp = valueOfTagVar(var);
					schattr = new schemaAttribute(i+1,varType.TAG_VAR);
					varList.addVar(var,schattr);
				}
			}
		
			// if the element scanned has a binding to a variable
			// in element_as or content_as, then add that variable
			// to the var table with its corresponding 
			// schema attribute
			curbd = curpat.getBindingData();
			if(curbd != null) {
				type = curbd.getType();
				var = (String)curbd.getValue();
				if(type == dataType.ELEMENT_AS)
					schattr = new schemaAttribute(i+1,varType.ELEMENT_VAR);
				else
					schattr = new schemaAttribute(i+1);
				varList.addVar(var,schattr);
			}
			
		        // if the pattern is a leaf node (identifier or 
			// variable)	
			if(curpat instanceof patternLeafNode) { 
				leafData = ((patternLeafNode)curpat).getExpData();
				type = leafData.getType();

				// if the leaf data is a variable
				if(type == dataType.VAR) {
					var = (String)leafData.getValue();

					// if the variable has been used
					// before, then replace it with a new
					// temp variable, and add a predicate
					// to the list of predicates that
					// capture this equality (sort of an
					// internal join)
					if(varList.lookUp(var) != null) {
					   var = getTmpVar();
					   varData = new data(dataType.VAR,var);
					   listOfPredicates.addElement(new predArithOpNode(opType.EQ,varData,leafData));
					}
					schattr = new schemaAttribute(i+1);
					varList.addVar(var,schattr);
				}
				else {
					// leaf data is an identifier. replace
					// it with a new tmp variable and add
					// a predicate which says that this var
					// should be equal to the given
					// identifier
					var = getTmpVar();
					varData = new data(dataType.VAR,var);
					listOfPredicates.addElement(new predArithOpNode(opType.EQ,varData,leafData));
					schattr = new schemaAttribute(i+1);
					varList.addVar(var,schattr);
				}
			}
			
			tupleDes.addSchemaUnit(new SchemaUnit(curregExp,backPtr));
		}

		// for each schema unit in the schema, generate a Scan node
		// and add it to the top of the chain
		for(int i=1;i<tupleDes.numAttr();i++) {
			scan = new scanOp();
			su = tupleDes.getSchemaUnit(i);
			if(i==1)
				scan.setScan(new schemaAttribute(su.getBackPtr(),varType.ELEMENT_VAR),su.getRegExp());
			else
				scan.setScan(new schemaAttribute(su.getBackPtr()),su.getRegExp());
			curLogNode = new logNode(scan,childLogNode);
			curLogNode.setSchema(tupleDes);
			curLogNode.setVarTbl(varList);
			childLogNode = curLogNode;
		}
		
		return curLogNode;
	
	}

	/**
	 * Generate a chain of Scan nodes for eact IN clause and add it
	 * to the list of partial trees
	 */
	private void initialLogTrees() {
		int numOfInClause = listOfInClause.size();
		for(int i=0;i<numOfInClause;i++) 
			partialTree.addElement(genLogPlan((inClause)listOfInClause.elementAt(i)));
	}


        /**
	 * Scan through the list of predicates and check if the variables
	 * present in them are contained in the variables encountered in any
	 * of the partial trees. If yes, then add a Select node with these
	 * predicates as the Selection condition.
	 */
	private void addSelect() {
		logNode curlogNode, childlogNode;
		predicate curpredicate;
		Vector variables;
		selectOp select;
		varTbl tableofvar;
                
		// for each partial tree, collect the predicates whose variables
		// have been encountered in the tree
		for(int i=0;i<partialTree.size();i++) {
			childlogNode = (logNode)partialTree.elementAt(i);
			tableofvar = childlogNode.getVarTbl();
			int j=0;
			Vector preds = new Vector();

			// predicates that can be added are collected and
			// removed from the list of predicates
			while(j<listOfPredicates.size()) {
				curpredicate = (predicate)listOfPredicates.elementAt(j);
				variables = curpredicate.getVarList();	
				if(tableofvar.contains(variables)) {
					listOfPredicates.removeElementAt(j);
					curpredicate.replaceVar(tableofvar);
					preds.addElement(curpredicate);
				}
				else
					j++;
			}

			// if the number of such predicates is not zero
			// then add a Select operator to the top of the
			// partial tree
			if(preds.size()!=0) {
				select = new selectOp();
				select.setSelect(preds);
				curlogNode = new logNode(select,childlogNode);
				curlogNode.setVarTbl(childlogNode.getVarTbl());
				curlogNode.setSchema(childlogNode.getSchema());
				partialTree.setElementAt(curlogNode,i);
			}
		}
	}

	/**
	 * Used to join two partial trees which have some variables in common
	 *
	 * @param the left tree
	 * @param the right tree
	 * @param list of common variables
	 */
	
	private logNode joinTree(logNode leftTree, logNode rightTree, Vector comVar)  {
		Vector leftJoinAttr, rightJoinAttr;
		varTbl leftVarTbl, rightVarTbl, newVarTbl;
		Schema leftSchema, rightSchema, newSchema;
		logNode newTree;
		String var;

		joinOp join;
		leftJoinAttr = new Vector();
		rightJoinAttr = new Vector();

		predicate curpredicate;
		Vector variables;
		Vector preds = new Vector();
		predicate joinPredicate = null;

		// Schema of the left tree
		leftSchema = leftTree.getSchema();

		// Schema of the right tree
		rightSchema = rightTree.getSchema();

		// length of the Schema of the left tree
		// on merging the two Schema, the pointers in the right Schema
		// have to be shifted by this length, as the right Schema is
		// appended to the end of the left Schema
		int leftRelSize = leftSchema.numAttr();	
	
		// variable table of the left tree
		leftVarTbl = leftTree.getVarTbl();

		// variable table of the right tree
		rightVarTbl = rightTree.getVarTbl();

		schemaAttribute leftAttr, rightAttr;

		// find the position or the corresponding schema attribute of
		// the common variables in both the left and the right schema.
		// these are the equi-join attributes
		for(int i=0;i<comVar.size();i++) {
			var = (String)comVar.elementAt(i);
			leftAttr = leftVarTbl.lookUp(var);		
			leftJoinAttr.addElement(leftAttr);
			rightAttr = rightVarTbl.lookUp(var);
			rightJoinAttr.addElement(rightAttr);
		}

		// merge the two schema to get a new Schema 
		// right schema is appended to the end of the left schema, 
		// updating the pointers
		newSchema = Util.mergeSchemas(leftSchema, rightSchema);

		// merge the two tables. the variables of the rigth tree have
		// a stream id of 1 in their schema attributes.
		newVarTbl = Util.mergeVarTbl(leftVarTbl,rightVarTbl,leftRelSize);
	
		// go throught the list of predicates to check if any of
		// the predicates have their variables contained in the variable
		// table formed by the union of the two variable table. If yes, 
		// then these predicates beomes the join predicate, in addition
		// to the equi-join.
		int j=0;
		while(j<listOfPredicates.size()) {
			curpredicate = (predicate)listOfPredicates.elementAt(j);
			variables = curpredicate.getVarList();	
			if(newVarTbl.contains(variables)) {
				listOfPredicates.removeElementAt(j);
				curpredicate.replaceVar(leftVarTbl,rightVarTbl);
				preds.addElement(curpredicate);
			}
			else
				j++;
		}

		// if number of such predicate is not zero then AND all of them
		// to get a single join predicate, besides the equi-join on
		// the common variables
		if(preds.size()!=0) 
			joinPredicate = Util.andPredicates(preds);
	
		// add the join operator to the top of the tree with the
		// new Schema and variable table
		join = new joinOp();
		join.setJoin(joinPredicate,leftJoinAttr,rightJoinAttr);
		newTree = new logNode(join,leftTree,rightTree);
		newTree.setVarTbl(newVarTbl);
		newTree.setSchema(newSchema);

		return newTree;
	}


	/**
	 * From the list of partial trees, find the pairs that have some
	 * variables in common and then replace it with their join. This
	 * process is repeated until no more trees can be joined.
	 */
	private void addEquiJoin()  {
		
		logNode leftTree, rightTree, resultTree;
		varTbl leftVar, rightVar;
		Vector joinVar;

		int j;
		int i=0;

		// for every tree, scan the trees after it in the list of
		// partial trees. If the variables of two intersect then
		// delete these entries, while adding the new partial tree
		// created by the join to the end of the list.
		while(i < (partialTree.size()-1)) {
			leftTree = (logNode)partialTree.elementAt(i);
			leftVar = leftTree.getVarTbl();
			j = i+1;
			while(j < partialTree.size()) {
				rightTree = (logNode)partialTree.elementAt(j);
				rightVar = rightTree.getVarTbl();
				joinVar = leftVar.intersect(rightVar);

				// if the variables intersect
				if(joinVar.size() != 0){ 
					resultTree = joinTree(leftTree,rightTree,joinVar);
					// delete the two entries
					partialTree.removeElementAt(j);
					partialTree.removeElementAt(i);

					// as the first entry is deleted, we
					// move back the scanning pointer
					i--;

					// add the new tree to the end of 
					// the list
					partialTree.addElement(resultTree);
					break;
				}
				j++;
			}
			i++;
		}
	}


	/**
	 * Try to join the partial trees whose union of variables contain
	 * all the variables of some predicates.
	 */
	private void addJoin()  {
		logNode leftTree, rightTree, newTree;
		varTbl leftVarTbl, rightVarTbl, newVarTbl;
		Schema leftSchema, rightSchema, newSchema;
		predicate curpredicate, joinPredicate = null;
		Vector variables;
		joinOp join;

		// for each possible pair of partial trees, calculate
		// the union of their variables, and then collect all the
		// predicates whose variables are contained in the union. Add
		// such pair of trees by a join operator, putting it at the end
		// of the list of partial trees, while deleting the entries for
		// the two previous trees.
		int j;
		int i=0;
		while(i< (partialTree.size()-1)) {
		   leftTree = (logNode)partialTree.elementAt(i);
		   leftVarTbl = leftTree.getVarTbl();
		   j = i+1;
		   while(j < partialTree.size()) {
		      rightTree = (logNode)partialTree.elementAt(j);
		      rightVarTbl = rightTree.getVarTbl();
		      Vector preds = new Vector();
		      int k=0;

		      // scan through the list of predicates
		      while(k<listOfPredicates.size()) {
			curpredicate = (predicate)listOfPredicates.elementAt(k);
			variables = curpredicate.getVarList();

			// check to see if the union of two var table for this
			// pair of partial tree contains the all the variables
			// of this predicate. If yes then add it to the list
			// of join predicates.
			if(Util.unionContains(leftVarTbl, rightVarTbl, variables)) {
				listOfPredicates.removeElementAt(k);
				curpredicate.replaceVar(leftVarTbl,rightVarTbl);
				preds.addElement(curpredicate);
			}
			else
				k++;
		      }

		     // if the list of join predicates is not empty, then
		     // AND them to get a single join predicate. Join the
		     // two tree and add them to the end of the list of 
		     // partial trees. Delete the entries for the joining
		     // tree.
		     if(preds.size()!=0) {
			joinPredicate = Util.andPredicates(preds);
		        leftSchema = leftTree.getSchema();
		        rightSchema = rightTree.getSchema();
	
		        int leftRelSize = leftSchema.numAttr();	
		        newSchema = Util.mergeSchemas(leftSchema, rightSchema);
		        newVarTbl = Util.mergeVarTbl(leftVarTbl,rightVarTbl,leftRelSize);
		        join = new joinOp();
		        join.setJoin(joinPredicate,new Vector(),new Vector());
		        newTree = new logNode(join,leftTree,rightTree);
		        newTree.setVarTbl(newVarTbl);
		        newTree.setSchema(newSchema);
			
			// delete the entries of the joining trees
			partialTree.removeElementAt(j);
			partialTree.removeElementAt(i);

			// As the first entry is deleted, new partial tree shift
			// into its position and we want to continue scanning
			// the partial trees from here in the outer loop
			i--;

			// add the joined tree to the end of the list of 
			// partial trees
			partialTree.addElement(newTree);
			break;
		      }
		      j++;
		    }
		    i++;
		}
		
	}


	/**
	 * If the number of partial trees is still more than one, after 
	 * consuming all the predicates and none of them have variables in
	 * common with other, then they are joined without any join predicate
	 * (i.e. cartesian product) to get only one tree.
	 */
	private void addCartesian()  {
		logNode leftTree, rightTree, newTree;
		varTbl leftVarTbl, rightVarTbl, newVarTbl;
		Schema leftSchema, rightSchema, newSchema;
		
		joinOp join;

		// while there are more than two partial trees, join the second
		// one with the first and delete the second entry. At the end
		// only one tree will be left in the first position.
		while(partialTree.size() != 1) {
		   leftTree = (logNode)partialTree.elementAt(0);
		   rightTree = (logNode)partialTree.elementAt(1);

		   leftSchema = leftTree.getSchema();
		   rightSchema = rightTree.getSchema();

		   leftVarTbl = leftTree.getVarTbl();
		   rightVarTbl = rightTree.getVarTbl();

		   int leftRelSize = leftSchema.numAttr();
		   newSchema = Util.mergeSchemas(leftSchema, rightSchema);
		   newVarTbl = Util.mergeVarTbl(leftVarTbl, rightVarTbl, leftRelSize);

		   // join without any predicate
		   join = new joinOp();
		   join.setJoin(null,new Vector(), new Vector());
		   newTree = new logNode(join,leftTree,rightTree);
		   newTree.setVarTbl(newVarTbl);
		   newTree.setSchema(newSchema);

		   // put the joined tree in the first position
		   partialTree.setElementAt(newTree,0);

		   // delete the entry for the second tree
		   partialTree.removeElementAt(1);
                }
	}


        /**
	 * This is a recursive function that is used to flatten the n-ary
	 * tree representation of the pattern of the construct part of the
	 * query. Ecah root of the subtree of this n-ary tree has a 
	 * corresponding schema attribute. An internal node is has the tag name
	 * and these schema attributes as its children. This function is passed
	 * the length of the current Schema, so that the position of the next
	 * schema attribute is one more than this.
	 *
	 * @param the construct node
	 * @param the position or value of the first schema attribute
	 */
	private int addToList(constructBaseNode _cn,int initial_slot) {

		int numChild;
		constructBaseNode baseNode;
		constructInternalNode cinternal;
		constructLeafNode newLeafNode;
		Vector children, new_children;
		int[] childPtrs;

		int first_slot = initial_slot;

		// if the construct node is the leaf node then return the 
		// the initial slot, as this would already have a schema 
		// attribute
		if(_cn instanceof constructLeafNode) {
			return first_slot;
		}

		// if it is an internal node then for all its non-leaf children
		// generate a schema attribute that depend on the first 
		// available slot 
		cinternal = (constructInternalNode)_cn;
		children = cinternal.getChildren();
		numChild = children.size();
		childPtrs = new int[numChild];

		for(int i=0;i<numChild;i++) {
			childPtrs[i] = addToList((constructBaseNode)children.elementAt(i),first_slot);
			if((constructBaseNode)children.elementAt(i) instanceof constructInternalNode)
				first_slot = childPtrs[i]+1;
		}

		new_children = new Vector();

		// recreate this internal node with its non-leaf children
		// replace by a leaf node which has a schema attribute
		// corresponding to the root of that child
		for(int i=0;i<numChild;i++) {
			baseNode = (constructBaseNode)children.elementAt(i);
			if(baseNode instanceof constructLeafNode)
				new_children.addElement(baseNode);
			else {
				newLeafNode = new constructLeafNode(new data(dataType.ATTR,new schemaAttribute(childPtrs[i],varType.ELEMENT_VAR)));
				new_children.addElement(newLeafNode);
			}
		}
                
		// add this internal node to the list of construct node
		cinternal.setChildren(new_children);
		listOfConstruct.addElement(cinternal);
		return first_slot;
	}

	
	/**
	 * Use the addToList method to flatten the n-ary tree for the 
	 * pattern and then for each construct node in the list generate a
	 * construct operator and add to the top of the tree
	 */
	private void addConstruct()  {

		logNode curLogNode;
		varTbl var_tbl;
		Schema _schema;
		
		constructOp construct;
		nestOp nest;

		constructInternalNode constructTagPart;
		
		int tupleSize, numchild, pos, numOfConstruct;
		schemaAttribute sa;
		
		skolem sk;
	
		int numGroup = 0;

		if(partialTree.size() != 1) {
			System.out.println("can't have more than one tree before adding construct operator");
			return;
		}
	
		curLogNode = (logNode)partialTree.elementAt(0);
		var_tbl = curLogNode.getVarTbl();
		_schema = curLogNode.getSchema();
		tupleSize = _schema.numAttr();

		// if the construct part is already a leaf, then no need
		// to call addToList. Just add a single construct operator
		// to the top of the tree
		if(constructPart instanceof constructLeafNode) {
			constructPart.replaceVar(var_tbl);			
			construct = new constructOp();
			construct.setConstruct(constructPart);
			curLogNode = new logNode(construct,curLogNode);
			curLogNode.setVarTbl(var_tbl);
			curLogNode.setSchema(_schema);
			partialTree.setElementAt(curLogNode,0);
			return;
		}

		// construct part is not a simple node, but a tree
		addToList(constructPart,tupleSize);

		// add a construct operator for every entry in the list of 
		// construct nodes. The top two level can have a skolem
		// function, in which case we add a nest operator instead of
		// construct operator. This is a hack which needs 
		// to be re-worked!!!!!
		numOfConstruct = listOfConstruct.size();
		for(int i=0;i<numOfConstruct;i++) {
			constructTagPart = (constructInternalNode)listOfConstruct.elementAt(i);
			constructTagPart.replaceVar(var_tbl);

			sk = constructTagPart.getSkolem();
			
			// no skolem function. add a construct operator.
			if(sk == null) {
				construct = new constructOp();
				construct.setConstruct(constructTagPart);
				curLogNode = new logNode(construct,curLogNode);
				curLogNode.setVarTbl(var_tbl);
				curLogNode.setSchema(_schema);
				continue;
			}

			// skolem function is present. Add a nest operator
			nest = new nestOp();

			// to keep track of how many skolem function was
			// encountered
			numGroup++;

			/* UGLY will have to go */
			// if its the toplevel skolem, bring all the elements
			// in one root
			if(numGroup ==2) {
				constructTagPart.truncate();
//				constructTagPart.dump(0);
			}
			/*-----------------------*/

			nest.setResTemp(constructTagPart);
			curLogNode = new logNode(nest,curLogNode);
			curLogNode.setVarTbl(var_tbl);
			curLogNode.setSchema(_schema);
			
		}

		// replace the old tree, with the new tree having a construct
		// operator
		partialTree.setElementAt(curLogNode,0);
	}



	/**
	 * dump the plan to the standard output
	 */
	public void dumpPlan() {
		initialLogTrees();
		addSelect();
		addEquiJoin();
		addJoin();
		addCartesian();
		for (int i=0; i<partialTree.size(); i++)
			((logNode)partialTree.elementAt(i)).dump();
	}

	/**
	 * the main method that returns the logical plan
	 *
	 * @return the logical plan for the query
	 */
	public logNode getLogPlan() {
		// generate the chain of Scan nodes for all the IN clause
		initialLogTrees();

		// Add a Select operator to these chains, if possible
		addSelect();

		// Add equi-join if the variables of a pair of partial trees
		// intersect
		addEquiJoin();

		// Add a join if the union of variables of a pair of partial 
		// tree contains the variables of a predicate
		addJoin();

		// Merge the partial trees with a join without predicates to
		// get a single tree
		addCartesian();

		// Add a chain of construct operator to the logical tree
		// for construction of results in XML
		addConstruct();

		// return the logical tree
		return (logNode)partialTree.elementAt(0);
	}

	//return the files for dtdScan. used for trigger system
	//to monitor the sources
	public Vector getListOfInClause() {
		return listOfInClause;
	}


	/**
	 * test program for testing the generation of logical plan
	 */
	public static void main (String argv[]) {
      
     	 try {

	   // initialise a scanner by passing the test query
       	   Scanner s = new Scanner(new EscapedUnicodeReader(new FileReader(argv[0])));

	   // initialise the parser with the above scanner
       	   QueryParser p = new QueryParser(s);
       	   java_cup.runtime.Symbol parse_tree = p.parse();

	   // get the syntax tree for the query
	   xqlExt q = (xqlExt)(parse_tree.value);
	   q.dump();
           System.out.println("No errors. Parsing took ");
	   
	   System.out.println("Plan");
	   query xml_query = q.getQuery();

	   // initialise the logPlanGenerator with the syntax tree 
	   // for the query
	   logPlanGenerator planGen = new logPlanGenerator(xml_query);

	   // generate the logical plan for the query
	   logNode logicalPlan = planGen.getLogPlan();

	   // dump the logical plan on the screen
	   logicalPlan.dump();

         }
         catch (Exception e) {
           e.printStackTrace();
         }
       }
}
