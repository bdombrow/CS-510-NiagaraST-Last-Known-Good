
/**********************************************************************
  $Id: xqlExt.java,v 1.1 2000/05/30 21:03:30 tufte Exp $


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
 * An object of this calss will be the value of the Symbol object after the
 * parsing of the XML-QL query. The type variable indicates weather it is
 * a "Create Trigger", "Delete Trigger", or a simple query.
 *
 */
package niagara.xmlql_parser.syntax_tree;

import java.util.*;
import java.io.*;

public class xqlExt {
	
	private int type;        // CREATE_TRIG, DELETE_TRIG or XMLQL
	private String trigName; // name of the trigger
	private int times;       // number of times the trigger should be 
				 // executed (once or multiple)
	private String interval; // interval at which the trigger should be
				 // executed
	private String start;    // start time of the trigger
	private String expire;   // expiration of the trigger
	private query xmlq;      // XML-QL query
	private Vector actionList; // list of actions associated with 
				   // the trigger

	/**
	 * Constructor for the extended XML-QL (creating trigger)
	 *
	 * @param name of the trigger
	 * @param the base query
	 * @param if the trigger is executed once or multiple times
	 * @param first time when the trigger should be executed
	 * @param interval at which the trigger should be executed
	 * @param the time at which the trigger will expire
	 * @param list of actions to be take when the trigger is fired
	 */

	public xqlExt(String name, query q, int optTimes, String optStart, String optInterval, String optExpire, Vector al) {
		type = opType.CREATE_TRIG;
		trigName = name;
		times = optTimes;
		start = optStart;
		interval = optInterval;
		expire = optExpire;
		xmlq = q;
		actionList = al;
	}

	/**
	 * Constructor for delete trigger
	 *
	 * @param name of the trigger to be deleted
	 */

	public xqlExt(String name) {
		type = opType.DELETE_TRIG;
		trigName = name;
	}

	/**
	 * Constructor for base query
	 *
	 * @param the main XML-QL query
	 */

	public xqlExt(query q) {
		type = opType.XMLQL;
		xmlq = q;
	}

        /**
	 * @return the main XML-QL query
	 */

	public query getQuery() {
		return xmlq;
	}

	/**
	 * @return the name of the trigger
	 */

	public String getTriggerName() {
		return trigName;
	}

	/**
	 * @return the list of action to take when the trigger is fired
	 */

	public Vector getActionList() {
		return actionList;
	}

	/**
	 * @return the first time the trigger will be executed
	 */

	public String getStart() {
		return start;
	}

	/**
	 * @return the interval at which the trigger should be executed
	 */

	public String getInterval() {
		return interval;
	}

	/**
	 * @return the expiration time of the trigger
	 */

	public String getExpiry() {
		return expire;
	}

	/**
	 * @return true if the trigger will be executed only once, false
	 *         otherwise
	 */

	public boolean isOnce() {
		return (times == opType.ONCE);
	}

	/**
	 * @return true if the trigger will be executed multiple times, false
	 *         otherwise
	 */

	public boolean isMultiple() {
		return (times == opType.MULTIPLE);
	}

	/**
	 * @return if the trigger should be executed once or more
	 */

	public int getTimes() {
		return times;
	}

        /**
	 * @return the type of this extended query
	 */

	public int getType() {
		return type;
	}

	/**
	 * prints to the standard output
	 */

	public void dump() {
		if(type == opType.DELETE_TRIG) {
			System.out.println("delete trigger:");
			System.out.println(trigName);
			return;
		}
		if(type == opType.CREATE_TRIG) {
			System.out.println("create trigger:");
			System.out.println(trigName + " " + times + " " + expire);
			for(int i=0; i<actionList.size(); i++)
				System.out.println("\t" + (String)actionList.elementAt(i));

		}
		xmlq.dump();
	}
			
}
