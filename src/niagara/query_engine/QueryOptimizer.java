
/**********************************************************************
  $Id: QueryOptimizer.java,v 1.3 2002/10/27 02:37:57 vpapad Exp $


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

import java.util.Vector;
import java.util.Hashtable;
import niagara.data_manager.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;

/**
 * This class is used to optimize a logical plan and return an optimized logical
 * plan
 *
 * @version 1.0
 *
 */

public class QueryOptimizer {    
    
    //////////////////////////////////////////////////////////////////////////
    // These are the private data members of the class                      //
    //////////////////////////////////////////////////////////////////////////

    // This is the data manager to be contacted for optimization statistics,
    // URLs etc
    //
    private DataManager dataManager;


    //////////////////////////////////////////////////////////////////////////
    // These are the methods of the class                                   //
    //////////////////////////////////////////////////////////////////////////

    /**
     * This is the constructor that initializes a query optimizer
     *
     * @param dataManager The data manager that the query optimizer can contact
     *                    if necessary
     */

    public QueryOptimizer (DataManager dataManager) {

	// Initialize the data manager
	//
	this.dataManager = dataManager;
    }


    /**
     * This function optimizes an input logical plan and returns an optimized
     * logical plan
     *
     * @param unoptimizedPlan The unoptimized logical plan that is to be
     *                        optimized
     *
     * @return The optimized logical plan. If there is an error during optimization
     *         return null.
     */

    public logNode optimize (logNode unoptimizedPlan) 
	throws NoDataSourceException {

	// Walk the unoptimized plan and get statistics and list of URLs to
	// be queries
	//
	boolean proceed = true;
	try {
	    proceed = getStatisticsAndUrls(unoptimizedPlan);
	}
	catch (PushSEQueryException e) {
	    System.err.println (e);
	    proceed = false;
	}

	 System.out.println(proceed);

	// If there is an error, then return null
	// Well, Trigger do use weired file names.
	if (!proceed) {
            // Well, hack here.  
            // really should check it is the trigger thing. 
            selectAlgorithms(unoptimizedPlan);
	    return unoptimizedPlan;
	}

	// Select algorithms for each operator
	//
	proceed = selectAlgorithms(unoptimizedPlan);

	// For now, just return the unoptimized plan with algorithms selected
	//
	return unoptimizedPlan;
    }


    /**
     * This function walks a logical plan and contacts the data manager to get
     * statistics and the list of URLs for the relevant DTDs. The logical plan
     * is updated with this information.
     *
     * @param logicalPlan The logical plan describing the query
     *
     * @return True if no error occurs; false otherwise.
     */

    private boolean getStatisticsAndUrls (logNode logicalPlan) 
	throws PushSEQueryException, NoDataSourceException {

	op operator = logicalPlan.getOperator();

	// if it is constructOp, we keep descending
	if (operator instanceof constructOp) {
	    int numInputs = logicalPlan.getArity ();

	    if (numInputs != 1) {
		System.err.print ("notify chun of assertion failure:");
		System.err.print (" constructOp nodes do not have ");
		System.err.println (" one child");
	    }
	    return getStatisticsAndUrls (logicalPlan.input(0));
	}

	// same goes for joinOp. note that statistics are not gathered now
	else if (operator instanceof joinOp) {
	    int numInputs = logicalPlan.getArity();

	    boolean proceed = true;
	    for (int i = 0; i < numInputs; i++) {
		proceed &= getStatisticsAndUrls (logicalPlan.input(i));
	    }

	    return proceed;
	}

	// the node is neither constructOp nor joinOp, need to gather
	// the select nodes so that we can process the predicates in
	// them and dtdScanOp nodes so that we can find urls for them
	else {
	    Vector dtdVector = new Vector();
	    Vector selectNodes = new Vector();
	    Vector dtdPredicateVector = new Vector();

	    boolean proceed = collectDTDsAndSelectNodes (
		logicalPlan, dtdVector, selectNodes);

	    if (!proceed) return false;

	    String sequery = SEQueryExtractor.makeQuery (
		selectNodes, logicalPlan.getSchema(), false);
	    if (sequery == null) sequery = "@doc";

	    if (dtdVector.size() > 0) {
		if (dtdVector.size() != 1) {
		    String estr = "bug: more than one dtd in one ";
		    estr += " branch of the logical plan tree";
		    throw new PushSEQueryException(estr);
		}
		String dtd = (String)dtdVector.elementAt(0);

		if (dtd != null) {
		    sequery += " conformsto \"" + dtd + "\"";
		}

		if (sequery != null)
		    dtdPredicateVector.addElement (sequery);

		System.out.println ("\ndtdVector: ");
		System.out.println (dtdVector);

		System.out.println ("dtdPredicateVector: ");
		System.out.println (dtdPredicateVector);
	    }

	    // Now with this list, contact the data manager to get the URLs
	    // conforming to the DTDs and satisfy the predicates.

	    if (dtdPredicateVector.size() > 0) {

		// Contact the data manager 
		System.out.println ("Contacting the data manager...\n");

		DTDInfo dtdInfo = null;
        	dtdInfo = dataManager.getDTDInfo(dtdPredicateVector);

		// Now with the information about the URLs for each DTD, 
		// update dtd information in the logical plan

		if (dtdInfo != null) {
		    proceed = updateDTDInfoWithURLs (logicalPlan, dtdInfo);
		}
	    }
	
	    // No errors encountered

	    return proceed;
	}
    }

    /**
     * This function returns the list of DTDs reachable from the given 
     * node of the logical plan that do not have specific URLs to query over.
     *
     * @param logPlanRoot The root of the logical plan of interest
     * @param dtdVector The vector to add DTD that do not have specific 
     *		URLs to query over
     * @param dtdPredicateVector search engine predicates
     *
     * @return True if no error occurred, false otherwise
     */

    private boolean collectDTDsAndSelectNodes (logNode logPlanRoot, 
	Vector dtdVector, Vector selectNodes) {

	op operator = logPlanRoot.getOperator();

	if (operator instanceof dtdScanOp) {
	    return getDTDWithInexplicitURLs((dtdScanOp)operator, dtdVector);
	}
	else {
	    boolean proceed = true;

	    // collect select node
	    if (operator instanceof selectOp) {
		selectNodes.addElement (logPlanRoot);
	    }

	    int numInputs = logPlanRoot.getArity();

	    // recurse
	    for (int i = 0; i < numInputs; i++) {
		proceed = collectDTDsAndSelectNodes(
		    logPlanRoot.input(i), dtdVector, selectNodes);
	    }

	    return proceed;
	}
    }


    /**
     * This function adds the dtd scan operator to the list if it has
     * URLs that are not listed explicitly
     *
     * @param dtdScanOperator The dtd scan operator to be checked
     * @param dtdVector The vector to which the dtd scan operator is to
     *                  be added if it has inexplictly specified URLs
     *
     * @return True if there is no error and false otherwise
     */

    private boolean getDTDWithInexplicitURLs (dtdScanOp dtdScanOperator,
					      Vector dtdVector) 
    {

	// Get the representation of the list of urls to fetch
	//
	Vector urlRep = dtdScanOperator.getDocs();

	// Create storage for explict URL list
	//
	Vector urlVector = new Vector();

	// If there is a element in the representation that is "*"
	// then the list is inexplicit
	//
	int inexplicitIndex = -1;

	int numRep = urlRep.size();

	for (int rep = 0; rep < urlRep.size(); ++rep) {

	    // Get the data representation
	    //
	    data dataRep = null;
            try {
                dataRep = (data) urlRep.elementAt(rep);
            } catch (ClassCastException e) {
                dataRep = new data(dataType.STRING, urlRep.elementAt(rep));
                // System.err.println("Cast Error 1");
            }
	    // Extract the URL out of it
	    //
	    if (dataRep.getType() == dataType.STRING) {

		// Get the string
		//
                
		String url = null;
                try {
                    url = (String) dataRep.getValue();
                } catch (ClassCastException e) {
                    // System.err.println("Class Cast Error 2.");
                }

		// If the string is "*", then set inexplicit index
		//
		if (url.equals("*")) {
		    inexplicitIndex = rep;
		}
		else {
		    // Add the string to the urlList
		    //
		    //System.out.println("Optimizer adding url to url vector: "+url);
		    urlVector.addElement(url);
		}
	    }
	    else {

		System.err.println("Unknown type for url");
		return false;
	    }
	}

	// Set the documents of the DTD scan operator to the url vector
	//
	dtdScanOperator.setDocs(urlVector);

	// If the inexplicit index is non-negative, then add current dtd to
	// list
	//
	if (inexplicitIndex >= 0) {

	    dtdVector.add(dtdScanOperator.getDtdType());
	}

	// No errors encountered
	//
	return true;
    }


    /**
     * This function updates the list of URLs associated with each DTD, based on
     * information received from the data manager
     *
     * @param logPlanRoot The root of the logical plan of interest
     * @param dtdInfoHashTable Hashtable containing information about each DTD,
     *                         with the DTD id as the key
     *
     * @return True if no error occurred, false otherwise
     */

    private boolean updateDTDInfoWithURLs (logNode logPlanRoot, 
						DTDInfo dtdInfo)
			throws PushSEQueryException, NoDataSourceException  {

	// Get the operator corresponding to the logical node
	//
	op operator = logPlanRoot.getOperator();

	// If this is a DTD Scan operator, then process accordingly
	//
	if (operator instanceof dtdScanOp) {

	    return updateInfoWithURLs((dtdScanOp) operator, dtdInfo);
	}
	else {

	    // This is a regular operator, so recurse over all children
	    //
	    boolean proceed = true;

	    int numInputs = logPlanRoot.getArity();

	    // since this function is only called for one branch of the
	    // logical plan tree, which should contain at most one dtd
	    // scan operator and no join nodes, an assumption 
	    // 'numInputs==1' is made. If this assumption fails, the 
	    // processing will not work

	    if (numInputs > 1) {
		String estr = "failure for assertion: (numInputs==1) ";
		estr += "in updateDTDInfoWithURLs";
		throw new PushSEQueryException(estr);
	    }

	    for (int child = 0; child < numInputs && proceed; ++child) {

		// Recurse on child
		//
		proceed = updateDTDInfoWithURLs(logPlanRoot.input(child),
						dtdInfo);
                if(!proceed)
		   return false;
	    }
            
	    return proceed;
	}
    }


    /**
     * This function updates the list of URLs associated with the DTD, based on
     * information received from the data manager
     *
     * @param dtdScanOperator The dtd scan operator to be updated
     * @param dtdInfoHashTable Hashtable containing information about each DTD,
     *                         with the DTD id as the key
     *
     * @return True if no error occurred, false otherwise
     */

    private boolean updateInfoWithURLs (dtdScanOp dtdScanOperator,
					DTDInfo dtdInfo) throws NoDataSourceException {

	// Add the URLs if found
	//
	if (dtdInfo != null) {

	    // Get the list of URLs obtained from data manager
	    //
	    Vector urlList = dtdInfo.getURLs();

	    // Add this list to existing list
	    //
	    if (urlList != null) {
		(dtdScanOperator.getDocs()).addAll(urlList);
	    }

	    System.out.println("Number of URLs for " 
		+ dtdScanOperator.getDtdType() + " = " 
		+ dtdScanOperator.getDocs().size());
	    System.out.println (urlList);

	    if(dtdScanOperator.getDocs().size() == 0)
	       throw new NoDataSourceException();
	}

	// No errors encountered
	//
	return true;
    }


    /**
     * This function selects algorithms for various operators. This represents primitive
     * optimization
     *
     * @param logicalPlan The logical plan for which algorithms have to be selected for
     *                    each operator
     *
     * @return True if no error occured; false otherwise
     */

    private boolean selectAlgorithms (logNode logicalPlan) {

	// Get the operator corresponding to the logical node
	//
	op operator = logicalPlan.getOperator();

	// If this is a DTD Scan operator, then nothing to do
	//
	if (operator instanceof dtdScanOp) {

	    return true;
	}
	else {

	    // This is a regular operator, so have to select algorithm
	    //
	    if (operator instanceof joinOp) {
		
		selectJoinAlgorithm((joinOp) operator);
	    }
	    else {

		// All other operators just select first algorithm for now
		//
		operator.setSelectedAlgoIndex(0);
	    }

	    // Recurse on all children
	    //
	    int numInputs = logicalPlan.getArity();

	    boolean proceed = true;

	    for (int child = 0; child < numInputs && proceed; ++child) {

		// Recurse on child
		//
		proceed = selectAlgorithms(logicalPlan.input(child));
	    }

	    return proceed;
	}	    
    }


    /**
     * This function selects a join algorithm for a join operator
     *
     * @param joinOperator The join operator for which an algorithm is to be
     *                     selected
     */

    private void selectJoinAlgorithm (joinOp joinOperator) {

	// If there is an equi-join component, then use hash join (join algorithm
	// index of 1), else use nested loop join (join algorithm index of 0). Note
	// that in the future, one will not make explicit references to index-algo
	// associations. Each join algorithm will have a cost and that will be used
	// to select the appropriate join. Thus, in the case of non-equi join, the
	// hash join operator will have infinite cost and will not be selected. No
	// reference should be made to the ordering of join algorithms as is done
	// now.
	//
	if (joinOperator.isCartesian())
            // Choose Nested loop join
            joinOperator.setSelectedAlgoIndex(0);
        else    
	    // Choose hash join
	    joinOperator.setSelectedAlgoIndex(1);
    }
}





