
/**********************************************************************
  $Id: FiredTriggers.java,v 1.1 2000/05/30 21:03:29 tufte Exp $


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

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;



/**
 * <code>FiredTriggers</code> contains triggers to be processed by trigger manager.
 * <code>EventDetector</code> finds files of these triggers are modified.
 * @see <code>EventDetector</code>
 */
public class FiredTriggers {

    /**
     * file -- triggers.
     */
    private Hashtable fireList;

    /**
     * file names.
     */
    private Enumeration enum;


    /**
     * Constructor.
     */
    FiredTriggers () {

	this.fireList = null;
	this.enum = null;
    }


    /**
     * Adds fired triggers.
     */
    public void addTriggers ( Hashtable firedTriggers ) {

	if ( this.fireList == null )
	    this.fireList = firedTriggers;
	else {

	    Enumeration trigs = firedTriggers.keys();

	    while ( trigs.hasMoreElements() ) {

		Integer tid = ( Integer )trigs.nextElement();
		Vector files = ( Vector )firedTriggers.get( tid );

		if ( this.fireList.containsKey( tid ) ) {

		    Vector fileList = ( Vector )this.fireList.get( tid );

		    for ( int i = 0; i < files.size(); i++ ) {

			String file = ( String )files.elementAt( i );
			if ( !fileList.contains( file ) )
			    fileList.add( file );
		    }
		}
		else
		    this.fireList.put( tid, files );
	    }
	}
    }


    /**
     * Gets the modified file list of a trigger.
     * @param triggerId  trigger id
     * @return a vector of file names or null if trigger id not found
     */
    public Vector getFiles ( Integer triggerId ) {

	return ( Vector )this.fireList.get( triggerId );
    }


    /**
     * Checks if there're more triggers to be fired.
     */
    public boolean hasMoreTriggers () {

	// Initializes the trigger id enumeration.
	if ( this.enum == null )
	    this.enum = this.fireList.keys();

	return this.enum.hasMoreElements();
    }


    /**
     * Gets the next trigger id be fired.
     * @return trigger id; null if no more triggers to be fired
     */
    public Integer nextTrigger () {

	return ( Integer )this.enum.nextElement();
    }
}
