
/**********************************************************************
  $Id: TriggerManager.java,v 1.3 2002/05/07 03:11:01 tufte Exp $


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


package niagara.trigger_engine;

/**
 * The class <code>TriggerManager</code> is the main class for managing 
 * continuous queries. It consists of two main functionalities.
 * First, it handles users request for creating and deleting continuous
 * queries. After getting a new installed constinous query, it invokes
 * GroupQueryOptimizer to do the group optimization. Then it will inform
 * EventDector to monitor this new query and related documents. 
 * Second, it invokes Niagra query engine to run the queries 
 * after getting the "firing" notification from EventDector.  
 * It contains an reference to QueryEngine for executing trigger plans. 
 * @version 1.0
 *
 *
 * @see GroupQueryOptimizer
 * @see EventDector
 */

import niagara.query_engine.*;
import niagara.data_manager.*;
import org.w3c.dom.*;
import java.io.*;
import java.util.*;
import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import niagara.utils.*;
import niagara.xmlql_parser.op_tree.*;
import niagara.xmlql_parser.syntax_tree.*;


/**
 * The class <code>TrigExecutor</code> executes each firing query by 
 * allocating one thread. 
 */
class TrigExecutor implements Runnable {

    private TriggerManager TM;
    private Integer trigId;
    private Vector changedFileNames;

    /**
     * constructor  --create the TrigExecutor for executing specified trigger
     * and changed file names
     *
     * @param TM the instance of TriggerManager
     * @param trigId the id of the firing trigger
     * @param changeFileNames Vector which contains all changed file names
     **/
    TrigExecutor(TriggerManager TM, Integer trigId, Vector changedFileNames) {
	this.TM=TM;
	this.trigId=trigId;
	this.changedFileNames=changedFileNames;
    } 

    /**
     * method to execute the firing trigger
     **/
    public void run() { 
	try {
	    System.err.println("TM:: Will fire " + trigId + " on file " + changedFileNames);
	    TM.fire(trigId, changedFileNames);
	    // Timing.recordTrig(trigId);
	} catch (ShutdownException se) {
	    System.err.println("TriggerManager.fire got shutdown exception " +
			       se.getMessage());
	    // just return and quit.
	    return;
	}
    }
}

public class TriggerManager implements Runnable {

    //Niagra object instances referred by trigger manager
    //
    private QueryEngine qe;  //reference of query engine, 1 per Niagra server
    private QueryOptimizer qOpt; //instance of query optimizer
    private EventDetector eventDetector; //instance of event detector
    private GroupQueryOptimizer gqOpt; //instance of group query optimizer
    private DataManager dataManager; //instance of data manager

    //Tables to hold useful informations
    //
    Hashtable trigTbl; //store mapping from the trigger id to 
                       //the root of query plan
    Hashtable trigResultTbl; //store mapping from the trigger id to 
                       //result queue 
    //Hashtable topTrigIdTbl; //store mapping from the trigger name to 
                       //the top level trigger Id. 

    private boolean active=true;

    private int nextId; //next availabe trigger id


    //Default values used by the Trigger Engine, all in milliseconds
    //
    long defaultEDCheckInterval=10000; // default time interval between event 
                                       // detector checks events
    long defaultStartUnit=40000; //start time will be rounded by this unit

    
    boolean doGroupOptimization = true; //flag of using group optimizer or not
     

    /**
     * constructor
     * @param QueryEngine, QueryOptimizer 
     **/
    public TriggerManager(QueryEngine q) {

	qe=q; //Trigger manager uses the same query engine instance
                     
	nextId = 0; // Persistence is needed in the future!!!

        //get the data manager in the query engine. 
        dataManager = q.getDataManager();	

	//set the data manager
	PhysicalOperator.setDataManager(dataManager);

        //the basic optimizer to choose the algorithms for
        //each logical operator
        qOpt = new QueryOptimizer(dataManager);

	//storing the mapping between
	//the trigger id and the trigger root currently
        trigTbl = new Hashtable(); 

	//storing the mapping between
	//the trigger id and the trigger result queue
        trigResultTbl = new Hashtable(); 
        
	//storing the mapping between
	//the trigger name and the top trigger id
	//topTrigIdTbl = new Hashtable(); 

	//create an event detector using default check frequency
	//in the future, would take this parameter from a config
	//file
	eventDetector=new EventDetector(dataManager, this);
	eventDetector.setDefaultCheckInterval(defaultEDCheckInterval);

	//set event detector in data manager
	dataManager.setEventDetector(eventDetector);

	//create a group query optimizer.
	gqOpt= new GroupQueryOptimizer(qOpt, this); 
    }

    /**
     * Set the flag to use group optimization or not
     * @param boolean flag
     **/    
    public void setDoGroupOptimization(boolean GOflag) {
	doGroupOptimization=GOflag;
    }

    /**
     * Simple id generator for trigger system
     * @return a unique id 
     **/    
    public int getNextId() {
	return(++nextId);
    }

    /**
     * Interface for users to delete a trigger
     * @param xqlExt
     **/    
    public void deleteTrigger(xqlExt q) {
	String trigName=q.getTriggerName();
	//delete the info stored in ED
	eventDetector.removeTrigger(trigName);
	//delete the info stored in TM
        removeExpiredTrigger(trigName);
    }

    /**
     * Another interface for users to delete a trigger
     * @param String trigName
     **/    
    public void deleteTrigger(String trigName) {
	System.out.println("delete trigger of "+trigName+ " is called");
	//delete the info stored in ED
	//Integer topTrigId = (Integer)topTrigIdTbl.get(trigName);
	//System.out.println("tirg id is "+topTrigId);
	//eventDetector.removeTrigger(topTrigId.intValue());
	eventDetector.removeTrigger(trigName);
	//delete the info stored in TM
        removeExpiredTrigger(trigName);
    }
    
    // This function takes prevTime and rounds it by the unit(millisec)
    private Date roundTimeBy(long prevTime, long unit) {
	long roundTime=prevTime/unit*unit;
	return (new Date(roundTime));
    }

    // This function returns the Date from the String format
    private Date getDate(String time) {
	    try {
		debug.mesg(time);
		SimpleDateFormat df = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy");
		Date tmpDate=df.parse(time,new ParsePosition(0));
		return (tmpDate);
		//startTime=new Date(tmpStartTime);
	    } catch (Exception e) {
		System.err.println(" time is not in valid format");
		return null;
	    }
    }

    /**
     * Function for server to create a trigger. 
     * @param trigQuery  the trigger query string 
     * @param qrq a SynchronizedQueue from which server to get result in the future.
     * @return trigName if success, null otherwise.
     **/   
    public String createTrigger(String trigQuery, SynchronizedQueue qrq) {
	try {
	    Scanner s = new Scanner(new EscapedUnicodeReader(new StringReader(trigQuery)));
	    QueryParser p = new QueryParser(s);
	    java_cup.runtime.Symbol parse_tree = p.parse();
	    xqlExt q = (xqlExt)(parse_tree.value);

	    String trigName=null;
	    trigName=internalCreateTrigger(q,qrq);

	    //if createTrigger is successful, store the trigger into
	    //disk. In case there is a system crash, the stored triggers
	    //will be reinstalled automaticlly.
	    //
	    if (trigName!=null) {
		FileOutputStream fout = null;		

		//get name of the trigger to use as the stored file name
		try{
		    fout = new FileOutputStream("TRIGGERS/"+trigName);
		    fout.write(trigQuery.getBytes());	
		    fout.close();
		}
		catch(IOException qioe){
		    System.out.println("can not write trigger to disk");
		}		
	    }
		
	    return trigName;

	} catch (Exception e) {
	    e.printStackTrace();
	    return null;
	}
	
    }

    //Generate start time for a new trigger 
    private Date procStartDate(xqlExt q) {
	String tmpStartTime=q.getStart();
	Date startTime;
	if (tmpStartTime==null || tmpStartTime.equals("")) {
	    //if user does not specify a start time, use current time plus
	    //a small delay
	    startTime = new Date(System.currentTimeMillis()+defaultStartUnit); 
	}	
	else {
	    startTime=getDate(tmpStartTime);
	}
	return startTime;
    }

    //Generate time interval for a new trigger
    private long procTimeInterval(xqlExt q) {
	long fireInterval;
	try {
	    //get the time interval
	    fireInterval = Long.parseLong(q.getInterval()); 
	}
	catch (NumberFormatException e) {
	    fireInterval = defaultEDCheckInterval;
	}
	
	//debug.mesg("prev fireInterval is "+fireInterval);
	//round the time interval by the boundary of default value
	if (fireInterval!=0) {
	    if (fireInterval<defaultEDCheckInterval) 
		fireInterval=defaultEDCheckInterval;
	    else 
		fireInterval=fireInterval/defaultEDCheckInterval*defaultEDCheckInterval;
	}
	//debug.mesg("after fireInterval is "+fireInterval);
	return fireInterval;
    }

    //Generate expire date for a new trigger
    private Date procExpireDate(xqlExt q) {
	//get the expire date
	String tmpExpire=q.getExpiry();
	Date expireDate;

	//if user does not specify expire date, we use null which means
	//never expire
	if (tmpExpire==null)
	    expireDate=null;
	else
	    expireDate=getDate(tmpExpire);

	return expireDate;
    }

    //Add a potion of fireInterval to the start time
    private long addDelayToStartTime(Date startTime, long fireInterval) {
	long updatedStartTime;
	long upperbound=30*60*1000; //no delay more than 30min
	long tmp=fireInterval/100;
	if (fireInterval > upperbound) {
	    updatedStartTime=startTime.getTime()+upperbound/100;
	}
	else {
	    updatedStartTime=startTime.getTime()+tmp;
	}
	return updatedStartTime;
    }

    /**
     * Another interface for user to create a trigger
     * @param q constains parsed format of trigger query
     * @param qrq the query result queue to put the query result object
     * @return String trigName if success, otherwise null
     **/   
    public String internalCreateTrigger(xqlExt q, SynchronizedQueue qrq) {
        
        //q contains trigger name, times, expire date, action, blah blah.
        // Now Just handle query part.
        
        query xml_query;
        logPlanGenerator planGen;
        logNode logicPlan;

        try {
            xml_query = q.getQuery();
            planGen = new logPlanGenerator(xml_query);
            logicPlan = planGen.getLogPlan();
        } catch (Exception e) {
            System.err.println("OOPS! Parser got problem");
            System.err.println("createTrigger Failed");
	    return null;
        }
            
        //System.err.println("Debugging ... Print out the original Plan");
        //logicPlan.dump();
        //System.err.println("Debugging ... Plan DONE!");
        
	//get name of the trigger
	String trigName=q.getTriggerName();

	//get start time
	Date startTime = procStartDate(q);

	//get time interval
	long fireInterval = procTimeInterval(q);
	
	//get the expire date
	Date expireDate = procExpireDate(q);
		
        // and next thing to do, put a trigActionOp on top of
        // the query plan. 
        trigActionOp top = null;
        try { 
	    top = (trigActionOp)operators.TrigAct.clone();
        } catch (CloneNotSupportedException e) {
            System.err.println("Error, cannot create trigger");
            e.printStackTrace();
	    return null;
        }
        top.setAction(q.getActionList());        
        logNode trigPlan = new logNode(top, logicPlan); 

	boolean tmpDoGroupOpt = doGroupOptimization;

	//Note, currently we only can handle some kinds of query for
	//group optimization. If a query can not be group optimized,
	//it will still be installed without group optimization.
	//
	if (tmpDoGroupOpt) {
	    Vector gOptResults=null;
	    try {
		//Call group optimizer to perform group optimization
		gOptResults = gqOpt.groupOptimize(trigPlan);
		int size = gOptResults.size();
		
		for (int i=0; i<size; i++) {
		    GroupOptResult gOptRes=(GroupOptResult)gOptResults.elementAt(i);
		    Integer trigId = gOptRes.getTrigId();
		    debug.mesg("trigId= "+trigId);
		    logNode trigRoot = gOptRes.getTrigRoot();
		    //trigRoot.dump();
		    Vector srcFileNames = gOptRes.getSrcFileNames();
		    debug.var(srcFileNames);
		    
		    // put the mapping of trigger name with the top tirgger id
		    //in the table. Currrently we only delete the top trigger.
		    if (i==(size-1)) {
			//topTrigIdTbl.put(trigName, trigId);
			if (qrq!=null)
			    trigResultTbl.put(trigId, qrq);
		    }

		    //put the mapping of trigger root (action node) with 
		    //trigger id into trigTbl;
		    trigTbl.put(trigId, trigRoot);
		    
		    ////////////////////////////////////////////////////////
		    //Since the upper level trigger has the same fire interval
		    //with the lower level trigger after group. For 
		    //timer-based continuous queries, it could cause
		    //the upper level trigger to miss one fire. We try to avoid
		    //the missing for long interval trigger, for short interval
		    //trigger, it is not very significant. We add a small delay
		    //for the start time of the upper level triggers. In the
		    //future, the relationship among split queries will be 
		    //recored, and the upper level trigger will be checked
		    //after the lower trigger is fired. This needs to add
		    //some work in Event Detector.
		    //////////////////////////////////////////////////////////
		    long updatedStartTime;
		    String srcFileName = (String)srcFileNames.elementAt(0);
		    if (CacheUtil.isTrigTmp(srcFileName)) {
			//upper level trigger
			updatedStartTime = addDelayToStartTime(startTime, fireInterval);
		    }
		    else {
			updatedStartTime=startTime.getTime();
		    }	
		    Date updatedStartDate=roundTimeBy(updatedStartTime, defaultStartUnit);
		    		    
		    //create an entry in the event monitor
		    debug.mesg("^^^^Calling ED with parameters:");
		    System.out.println(trigName+trigId.intValue()+srcFileNames.toString());
		    eventDetector.addTrigger (trigName,
					      trigId.intValue(), // Trigger id.
					      srcFileNames,// file dir's or URLs.
					      //trigType,
					      updatedStartDate, //Start fire time.					      
					      fireInterval, // Interval (millisec)
					      expireDate        // expire date
					      );	    
		    
		    //debug.mesg("trigger "+trigName+" has been added");
		}
	    }
	    catch (Exception ex) {
		System.err.println("Error in group optimization");
		//try to install the trigger without group optimization
		tmpDoGroupOpt = false; 		
	    }
	};

	//Not use group optimization.
	//Any complex query can be installed in this case. This query
	//will not be split and only be installed as one query.
	//
	if (!tmpDoGroupOpt) {
	    Vector fileNames = new Vector();
	    Integer trigId = new Integer(getNextId()); 

	    try {
		findAllSourceFiles(trigId, trigPlan, fileNames);    
	    }
	    catch (Exception e) {
		System.err.println("Data source files error");
		return null;
	    }

	    //put the mapping of trigger root (action node) with 
	    //trigger id into trigTbl;
	    trigTbl.put(trigId, trigPlan);
	    
	    if (qrq!=null)
		trigResultTbl.put(trigId, qrq);
 
	    //topTrigIdTbl.put(trigName, trigId);
		
	    long updatedStartTime;		
	    updatedStartTime=startTime.getTime();		
	    Date updatedStartDate=roundTimeBy(updatedStartTime, defaultStartUnit);
	    
	    //create an entry in the event monitor
	    //note the null values will be replaced in the future 
	    debug.mesg("^^^^Calling ED with parameters:");
	    System.out.println(trigName+trigId.intValue()+fileNames.toString());
	    eventDetector.addTrigger (trigName,
				      trigId.intValue(),   // Trigger id.
				      fileNames,   // file dir's or URLs.
				      //trigType,
				      updatedStartDate, // Next fire time.
				      
				      fireInterval, // Interval (millisec)
				      
				      expireDate        // expire date
				      );	    
	    
	    //debug.mesg("trigger "+trigName+" has been added");
	    
	}

	//flush out the tmp files for debug purpose
	/*
	for (int i=0; i<size; i++) {
	    GroupOptResult gOptRes=(GroupOptResult)gOptResults.elementAt(i);
	    String srcFileName = gOptRes.getSrcFileName();
	    dataManager.flushDoc(srcFileName);
	}
	*/

        return trigName;
    }

    void fire(Integer trigId, Vector changedFileNames) 
	throws ShutdownException {

	//debug.mesg("trigger start firing...");

        String t_mesg = "Will fire ";
	
	t_mesg+=trigId.intValue() + ":";	    
	
        //debug.mesg(t_mesg);
	logNode optimizedPlan=null;
	
	//Execute the stored trigger plan.
	//Niagra query optimizer is invoked before execution. 	
	//
	logNode unOptimizedPlan = (logNode)trigTbl.get(trigId);

	//System.err.println("TM::Fire: unoplan " + unOptimizedPlan);
	Vector dtdScanOps = new Vector();
	findAllDtdScanOps(unOptimizedPlan, dtdScanOps);
	int dtdScanNo = dtdScanOps.size();
	
	//we need a loop here to execute this trigger maybe for
	//several times for the source file changes
	for (int d=0; d<dtdScanNo; d++) {
	    dtdScanOp currDtdScanOp = (dtdScanOp)dtdScanOps.elementAt(d);
	    if (!replaceFileNames(currDtdScanOp, changedFileNames, trigId)) {
		continue;
	    }
	
	  //debug.mesg("***TM.fire.unoptimizedplan.dump() start");
	  //unOptimizedPlan.dump();
	  //debug.mesg("***TM.fire.unoptimizedplan.dump() ends");
	
	//Call the optimizer to choose an algorithm
	try {
	    //System.err.println("+++++++ Using qOpt " + qOpt);
	    optimizedPlan = qOpt.optimize(unOptimizedPlan);
	    /*
	    debug.mesg("***TM.fire.optimizedplan.dump() start");
	    optimizedPlan.dump();
	    debug.mesg("***TM.fire.optimizedplan.dump() ends");
	    */
	} catch (Exception e) {
	    System.err.println("Exception!.  Just be silent");
	    e.printStackTrace();
	}	 
	
	// debug.var(optimizedPlan);
	//execute the query
	System.err.println("TM:fire: opplan " + optimizedPlan);

	//get the result queue name for this trigger
	SynchronizedQueue queryResultQueue = (SynchronizedQueue)trigResultTbl.get(trigId);

	QueryResult tmpres=null;	
	tmpres=qe.executeOptimizedQuery(optimizedPlan);
	if (queryResultQueue!=null) {
	    debug.mesg("TM::query result is put into the queue");
	    queryResultQueue.put(tmpres,true);
	}
	/*	
	//Old code for displaying the execution results
	//Now, the results are returned to client GUI.
	debug.mesg("Query Done! Result count ");
	
	TXDocument tx = new TXDocument();
	Element root= tx.createElement("result");
	tx.appendChild(root);
	QueryResult.ResultObject tmp;
	
	try {
	    tmp=tmpres.getNext();
	    while(tmp!=null) {
		if (tmp.result==null) {
		    System.out.println("null......nulll");
		    break;
		} else {
		    root.appendChild(
				     tmp.result.getDocumentElement().cloneNode(true)
				     ); 
		}
		tmp=tmpres.getNext();    
		// debug.mesg(tmp.result.getText());
	    }
	} catch (Exception e) {
	    debug.mesg("NULL result");
	}

	debug.mesg("Firing results for trigger "+trigId.intValue()+" !!!");
        CUtil.printTree(tx.getDocumentElement(),"");
	tx=null;
	*/

	currDtdScanOp.restoreDocs();
    }
	
	debug.mesg("trigger"+trigId+" finish firing...");
}

    //interface to terminate the trigger manager
    public void killTriggerManager () {
	active =  false;
    }

    /**
     * A thread continously pulls the event monitor for firing triggers 
     **/   
    public void run() {
 
 	System.err.println("TM::Start listening on ED");
	while (active) {
	    FiredTriggers firedTriggers = eventDetector.getFiredTriggers();

	    while (firedTriggers.hasMoreTriggers()) {
		Integer trigId = firedTriggers.nextTrigger();
		Vector changedFileNames=firedTriggers.getFiles(trigId);
		System.err.println("TM::Trigger "+trigId+" is going to be fired"+changedFileNames.toString());
		TrigExecutor trigExecutor = new TrigExecutor(this,trigId,changedFileNames);
		new Thread(trigExecutor).start();
	    }

	}
    }

    /**
     * Interface for eventDector to deletes expired trigger.
     * This function is called by event detector when trigger expires.
     * Future Work!
     **/   
    public void removeExpiredTrigger ( String triggerName ) {};

    /**
     * Interface for delete a trigger plan in the trigger table
     * @param triggerId id of the trigger to be deleted
     * 
     **/   
    public void removeTriggerPlan ( int triggerId ) {
	trigTbl.remove(new Integer(triggerId));
    };

    /**
     * return the data manager instance used in trigger manager
     * @return DataManager 
     **/
    public DataManager getDataMgr() {
	return dataManager;
    }

    /*Traverse the plan and find all the source files
    * @param trigId tirgger id
    * @param node trigger plan root
    * @param sourceNames Vector containing all source files used in the trigger
    **/
    private void findAllSourceFiles(Integer trigId, logNode node, Vector sourceNames) 
    throws Exception {
        op currOp = node.getOperator();
	
        if(!(currOp instanceof dtdScanOp)) {
            for(int i=0; i<node.numInputs(); i++) {
                findAllSourceFiles(trigId, node.input(i), sourceNames); 	      
	    }
	}
	else {
            Vector docs=((dtdScanOp)currOp).getDocs(); 
	    int size = docs.size();
      
	    for (int i=0; i<size; i++) {
		data d=(data)docs.elementAt(i);
		String srcFileName=(String)d.getValue();
		if (srcFileName.equals("*")) {
		    String dtdName = ((dtdScanOp)currOp).getDtdType();
		    if (i!=0) 
			throw new Exception("Can not have file name with * together!");
		    else 
			sourceNames.addElement(dtdName);
		    return;
		}
		else 
		    sourceNames.addElement(srcFileName);
		    
		}
	    }
	}

    /**
     * Traverse the plan and find all dtd scan nodes, we will execute the
     * trigger for n times where file changes affect n dtdscan nodes
     * We do this because of the incremental evaluation of triggers.
     * For example, when a trigger is a join query, when two source
     * files (for example, R.xml and S.xml)
     * are changed, we need to run twice to get correct incremental
     * results. New results = Delta_R join old_S + new_R join Delta_S
     * where Delta_ represents the changes on the file,
     * old_ represents the old version of the file
     * new_ represents the new version of the file (incorporating the changes)
     * @param node trigger plan root
     * @param dtdScanOps Vector containing all dtdScan nodes
     **/
    private void findAllDtdScanOps(logNode node, Vector dtdScanOps) {
        op currOp = node.getOperator();
	
        if(!(currOp instanceof dtdScanOp)) {
            for(int i=0; i<node.numInputs(); i++) {
                findAllDtdScanOps(node.input(i), dtdScanOps);        
	    }
	}
	else {
	    dtdScanOps.addElement(currOp);
        }
    }

    /**
     * Replace the files in dtdScanNode that are changed to delta files format
     * @param dtdScanOp current dtdScan operator
     * @param changedFileNames Vector changed File Names
     * @param Integer trigId
     * @return boolean whether files in this node have been replaced
     **/
    boolean replaceFileNames(dtdScanOp currOp, Vector changedFileNames, Integer trigId) {
	boolean b=false;

	Vector docs=currOp.getDocsCpy();
	if (docs==null) {
	    currOp.makeDocsCpy(); //this function just needs to be called once
	    docs=currOp.getDocsCpy();
	}

	int size = changedFileNames.size();
	Vector newDocs = new Vector();
	
	for (int i=size-1; i>=0; i--) {
		int index = docs.indexOf(changedFileNames.elementAt(i));
		if (index !=-1) {
		    newDocs.addElement((String)changedFileNames.elementAt(i)+"&"+trigId);
		    changedFileNames.removeElementAt(i);
		    b=true;
		}
	}

	if (b) {
	    currOp.setDocs(newDocs);
	}

	return b;
}


    /**
     * Asks search engine to get a list of source files which conforms a specified DTD file.
     * @param dtdName  dtd file name
     * @return a list of source urls
     */
    public Vector getSourceForDTD ( String dtdName ) {

	DTDInfo dtdInfo = null;
	String sequery = null;
	Vector dtdVector = new Vector();
	Vector sourceList = null;

	sequery = " conformsto \"" + dtdName + "\"";
	dtdVector.addElement( sequery );

	dtdInfo = dataManager.getDTDInfo( dtdVector );

	if ( dtdInfo != null )
	    sourceList = dtdInfo.getURLs();

	return sourceList;
    }

}



