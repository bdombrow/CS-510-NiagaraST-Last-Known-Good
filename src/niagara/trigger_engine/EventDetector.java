
/**********************************************************************
  $Id: EventDetector.java,v 1.2 2002/03/26 23:53:14 tufte Exp $


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

import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;
import niagara.data_manager.*;



/**
 * <code>TriggerEntry</code> contains information for a user defined
 * trigger, including split trigger id list, start process time, interval,
 * and expire time. Instances of this class are used as values in trigger 
 * hashtable in <code>TriggerTable</code>
 */
class TriggerEntry {

    /**
     * Estimated number of split triggers.
     */
    private static final int ID_COUNT = 2;

    /**
     * Trigger Id list.
     */
    private Vector idList;

    /**
     * Start process time ( maybe multiple start time for different triggers ).
     */
    private Vector startTimeList;

    /**
     * Checking interval.
     */
    private long interval;

    /**
     * Expire time.
     */
    private Date expireTime;

    /**
     * For debugging purpose.
     */
    private static boolean DEBUG = EventDetector.DEBUG;


    /**
     * Constructor.
     * @param triggerId    split trigger id
     * @param startTime    start process time
     * @param interval     checking interval
     * @param expireTime   expire time
     */
    TriggerEntry ( int triggerId, Date startTime, long interval, Date expireTime ) {

	this.idList = new Vector( ID_COUNT );
	this.idList.add( new Integer( triggerId ) );

	this.startTimeList = new Vector();
	this.startTimeList.add( startTime );
	this.interval = interval;
	this.expireTime = expireTime;
    }


    /**
     * Adds another trigger id to this user defined trigger.
     * @param triggerId  trigger id
     * @return true if trigger id is added successfully; otherwise false
     */
    boolean addTrigger ( int triggerId ) {

	Integer id = new Integer( triggerId );

	if ( this.idList.contains( id ) ) {

	    if ( DEBUG )
		System.err.println( "EventDetector: Trigger " + triggerId + " has been already added!" );
	    return false;
	}
	else {

	    this.idList.add( id );
	    return true;
	}
    }


    /**
     * Adds another trigger id with its start time.
     * @param triggerId    trigger id
     * @param startTime    start time
     * @return true if trigger is added successfully; otherwise false
     */
    boolean addTrigger ( int triggerId, Date startTime ) {

	Integer id = new Integer( triggerId );

	if ( this.idList.contains( id ) ) {

	    if ( DEBUG )
		System.err.println( "EventDetector: Trigger " + triggerId + " has been already added!" );
	    return false;
	}
	else {

	    this.idList.add( id );
	    this.startTimeList.add( startTime );
	    return true;
	}
    }


    /**
     * Gets id list.
     * @return a vector of id's
     */
    Vector getIdList () {

	if ( ( this.idList == null ) || ( this.idList.size() == 0 ) ) {

	    if ( DEBUG )
		System.err.println( "EventDetector: Trigger Empty!" );
	    return null;
	}
	else {

	    Vector list = ( Vector )this.idList.clone();
	    return list;
	}
    }


    /**
     * Gets trigger start time.
     * @return start fire time
     */
    Vector getStartTime () {

	return this.startTimeList;
    }


    /**
     * Gets checking interval.
     * @return check interval
     */
    long getInterval () {

	return this.interval;
    }


    /**
     * Gets expire time.
     * @return expire time
     */
    Date getExpireTime () {

	return this.expireTime;
    }
}



/**
 * <code>TriggerTable</code> is used to store all user defined triggers in a hashtable. 
 * Keys in hashtable are user-defined trigger names, its values are <code>TriggerEntry</code>.
 * @see <code>TriggerEntry</code>
 */
class TriggerTable {

    /**
     * Trigger table.
     */
    private Hashtable table;

    /**
     * For debugging purpose.
     */
    private static boolean DEBUG = EventDetector.DEBUG;


    /**
     * Constructor.
     */
    TriggerTable () {

	this.table = new Hashtable();
    }


    /**
     * Adds a trigger to the table.
     * @param triggerName  trigger name
     * @param triggerId    trigger id
     * @param startTime    start time
     * @param interval     checking interval
     * @param expireTime   expire time
     * @return true if trigger is added successfully, otherwise false
     */
    synchronized boolean addTrigger ( String triggerName, int triggerId, Date startTime, long interval, Date expireTime ) {

	if ( this.table.containsKey( triggerName ) ) {

	    TriggerEntry entry = ( TriggerEntry )this.table.get( triggerName );
	    if ( interval == 0 )
		return entry.addTrigger( triggerId );
	    else
		return entry.addTrigger( triggerId, startTime );
	}
	else {

	    TriggerEntry entry = new TriggerEntry( triggerId, startTime, interval, expireTime );
	    this.table.put( triggerName, entry );

	    return true;
	}
    }


    /**
     * Deletes a user defined trigger from this table.
     * @param triggerName trigger name
     * @return the entry of trigger; or null if not found
     */
    synchronized TriggerEntry removeTrigger ( String triggerName ) {

	return ( TriggerEntry )this.table.remove( triggerName );
    }
}



/**
 * <code>TriggerInfo</code> contains a trigger ( not user-defined trigger but split trigger )
 * and all its information, which includes its id, file, install time/last process time and this process time.
 * Instances of this classes will be used as entries in <code>TrigerInfoList</code>.
 * Note: if a trigger has to check multiple files, there will be multiple instances for this 
 * trigger.
 */
class TriggerInfo {

    /**
     * Trigger id.
     */
    int id;

    /**
     * File name.
     */
    String file;

    /**
     * Trigger Count -- how many triggers share this information.
     */
    int triggerCount;

    /**
     * Last process time / install time at first.
     */
    Date lastProcessTime;

    /**
     * current process time.
     */
    Date thisProcessTime;


    /**
     * Constructor.
     * @param triggerId   trigger id
     * @param file        file name
     * @param installTime install time
     */
    TriggerInfo ( int triggerId, String file, Date installTime ) {

	this.id = triggerId;
	this.file = file;
	this.triggerCount = 1;
	this.lastProcessTime = installTime;
	this.thisProcessTime = null;
    }


    /**
     * Constructor. -- reconstructing object, for future usage...
     * @param triggerId        trigger id
     * @param file             file name
     * @param triggerCount     trigger count
     * @param lastProcessTime  last process time
     * @param thisProcessTime  current process time
     */
    TriggerInfo ( int triggerId, String file, int triggerCount, Date lastProcessTime, Date thisProcessTime ) {

	this.id = triggerId;
	this.file = file;
	this.triggerCount = triggerCount;
	this.lastProcessTime = lastProcessTime;
	this.thisProcessTime = thisProcessTime;
    }


    /**
     * Adds a trigger.
     */
    void addTrigger () {

	this.triggerCount++;
    }


    /**
     * Compares two trigger by their ids.
     * @param tid  another trigger id
     * @return 0 if equal; 1 if greater than; -1 if less than
     */
    int compareTo ( int tid ) {

	if ( this.id < tid )
	    return -1;

	if ( this.id > tid )
	    return 1;

	return 0;
    }


    /**
     * Compares the sequence between two timer triggers on the list.
     * @param tid       another trigger id
     * @param fileName  file name of another trigger
     * @return 0 if equal; 1 if greater than; -1 if less than
     */
    int compareTo ( int tid, String fileName ) {

	if ( this.id < tid )
	    return -1;

	if ( this.id > tid )
	    return 1;

	return this.file.compareTo( fileName );
    }


    /**
     * Removes trigger -- decreases trigger count.
     * @return current trigger count
     */
    int removeTrigger () {

	this.triggerCount--;
	return this.triggerCount;
    }
}



/**
 * <code>TriggerInfoList</code> contains all triggers ( not user-defined triggers but split triggers )
 * and their file and timer information.
 * @see <code>TriggerInfo</code>
 */
class TriggerInfoList {

    /**
     * Trigger list.
     */
    private Vector list;

    /**
     * Searching purpose.
     */
    private int position;

    /**
     * For debugging purpose.
     */
    private static boolean DEBUG = EventDetector.DEBUG;


    /**
     * Constructor.
     */
    TriggerInfoList () {

	this.list = new Vector();
    }


    /**
     * Adds a trigger to this list.
     * @param triggerId   trigger id
     * @param file        file name
     * @param installTime install time
     */
    synchronized void addTrigger ( int triggerId, String file, Date installTime ) {

	TriggerInfo trig = new TriggerInfo( triggerId, file, installTime );
	insertTrigger( trig );
    }


    /**
     * Adds a trigger to this list. -- reconstructing list for future usage...
     * @param triggerId        trigger id
     * @param file             file name
     * @param triggerCount     trigger count
     * @param lastProcessTime  last process time
     * @param thisProcessTime  current process time
     */
    synchronized void addTrigger ( int triggerId, String file, int triggerCount, Date lastProcessTime, Date thisProcessTime ) {

	TriggerInfo trig = new TriggerInfo( triggerId, file, triggerCount, lastProcessTime, thisProcessTime );
	insertTrigger( trig );
    }


    /**
     * Finds the entry of a trigger on the list by its id. Set the position as the first entry of this trigger.
     * @param triggerId trigger id
     * @return true if trigger is found; otherwise false
     */
    synchronized private boolean findTrigger ( int triggerId ) {

	boolean found = false;
	int left = 0, right = this.list.size() - 1;

	if ( left > right ) {

	    this.position = 0;
	    return false;
	}

	// Binary search.
	int pos;
	while ( !found ) {

	    pos = ( left + right ) / 2;

	    TriggerInfo trig = ( TriggerInfo )this.list.elementAt( pos );

	    int seq = trig.compareTo( triggerId );

	    if ( seq  == 0 ) {

		this.position = pos;
		return true;
	    }
	    else { 

		if ( seq < 0 )
		    left = pos + 1;
		else
		    right = pos - 1;

		if ( left > right ) {

		    this.position = left;
		    break;
		}
	    }
	}

	return false;
    }


    /**
     * Finds the entry of a trigger on the list by its id and file. Set the position.
     * @param triggerId trigger id
     * @param file      file name
     * @return true if trigger is found; otherwise false
     */
    synchronized private boolean findTrigger ( int triggerId, String file ) {

	boolean found = false;
	int left = 0, right = this.list.size() - 1;

	if ( left > right ) {

	    this.position = 0;
	    return false;
	}

	// Binary search.
	int pos;
	while ( !found ) {

	    pos = ( left + right ) / 2;

	    TriggerInfo trig = ( TriggerInfo )this.list.elementAt( pos );

	    int seq = trig.compareTo( triggerId, file );

	    if ( seq  == 0 ) {

		this.position = pos;
		return true;
	    }
	    else { 

		if ( seq < 0 )
		    left = pos + 1;
		else
		    right = pos - 1;

		if ( left > right ) {

		    this.position = left;
		    break;
		}
	    }
	}

	return false;
    }


    /**
     * Inserts a trigger into this list.
     * @param trig  an entry of trigger info
     */
    synchronized void insertTrigger ( TriggerInfo trig ) {

	// Searches the right position to insert this trigger.
	if ( findTrigger( trig.id, trig.file ) ) {

	    TriggerInfo trigger = ( TriggerInfo )this.list.elementAt( this.position );
	    trigger.addTrigger();
	}
	else {

	    // Inserts the trigger.
	    this.list.add( this.position, trig );
	}
    }


    /**
     * Gets last process time of a specified trigger.
     * @param triggerId  trigger id
     * @param file       file name
     * @return last process time
     */
    synchronized Date getLastProcessTime ( int triggerId, String file ) {

	// Searches the trigger.
	if ( !findTrigger( triggerId, file ) ) {

	    if ( DEBUG )
		System.err.println( "EventDetector: Trigger " + triggerId + " on file "
				    + file + " not found on trigger information list!" );

	    return null;
	}
	else {

	    TriggerInfo trig = ( TriggerInfo )this.list.elementAt( this.position );
	    return trig.lastProcessTime;
   	}
    }


    /**
     * Gets (last process time, current process time] of a trigger on a specified file,
     * and updates last process time.
     * @param triggerId  trigger id
     * @param file       file name
     * @return a vector contains both time in milliseconds; or null if not found
     */
    Vector getProcessTimeRange ( int triggerId, String file ) {

	// Searches the trigger.
	if ( !findTrigger( triggerId, file ) ) {

	    if ( DEBUG )
		System.err.println( "EventDetector: Trigger " + triggerId + " on file "
				    + file + " not found on trigger infomation list!" );

	    return null;
	}
	else {

	    TriggerInfo trig = ( TriggerInfo )this.list.elementAt( this.position );

	    Vector time = new Vector( 2 );
	    time.add( 0, new Long ( trig.lastProcessTime.getTime() ) );
	    time.add( 1, new Long ( trig.thisProcessTime.getTime() ) );

	    // Updates last fire time.
	    trig.lastProcessTime = trig.thisProcessTime;

	    return time;
	}
    }


    /**
     * Removes a trigger from the list.
     * @param triggerId trigger id
     * @return all files of this trigger; or null if not found
     */
    synchronized Vector removeTrigger ( int triggerId ) {

	if ( !findTrigger( triggerId ) )
	    return null;
	else {

	    Vector files = new Vector();
	    TriggerInfo trigger = ( TriggerInfo )this.list.elementAt( this.position );

	    do {

		files.add( trigger.file );

		// Removes empty trigger after decreasing trigger count.
		if ( trigger.removeTrigger() == 0 )
		    this.list.removeElementAt( this.position );

		if ( this.list.size() > this.position )
		    trigger = ( TriggerInfo )this.list.elementAt( this.position );
		else
		    break;

	    } while ( trigger.id == triggerId );

	    return files;
	}
    }


    /**
     * Removes a ( trigger, file ) from the list.
     * @param triggerId trigger id
     * @param file      file name
     * @return true if trigger is found; otherwise false
     */
    synchronized boolean removeTrigger ( int triggerId, String file ) {

	if ( !findTrigger( triggerId, file ) )
	    return false;
	else {

	    TriggerInfo trigger = ( TriggerInfo )this.list.elementAt( this.position );

	    // Removes empty trigger after decreasing trigger count.
	    if ( trigger.removeTrigger() == 0 )
		this.list.removeElementAt( this.position );

	    return true;
	}
    }


    /**
     * Updates last process time.
     * @param triggerId trigger id
     * @param file      file name
     */
    synchronized void updateLastProcessTime ( int triggerId, String file ) {

	// Searches the trigger.
	if ( !findTrigger( triggerId, file ) ) {

	    if ( DEBUG )
		System.err.println( "EventDetector: Trigger " + triggerId + " on file "
				    + file + " not found on trigger information list!" );
	}
	else {

	    TriggerInfo trig = ( TriggerInfo )this.list.elementAt( this.position );
	    trig.lastProcessTime = trig.thisProcessTime;
	}
    }


    /**
     * Updates current process time.
     * @param triggerId trigger id
     * @param file      file name
     * @param fireTime  this fire time
     */
    synchronized void updateThisProcessTime ( int triggerId, String file, Date processTime ) {

	// Searches the trigger.
	if ( !findTrigger( triggerId, file ) ) {

	    if ( DEBUG )
		System.err.println( "EventDetector: Trigger " + triggerId + " on file "
				    + file + " not found on trigger information list!" );
	}
	else {

	    TriggerInfo trig = ( TriggerInfo )this.list.elementAt( this.position );
	    trig.thisProcessTime = processTime;
	}
    }
}



/**
 * <code>PushTriggerGroup</code> contains a group of push triggers which are
 * related to the same file. Instances of this class are used as hash values
 * in <code>PushTriggerList</code>
 */
class PushTriggerGroup {


    /**
     * File change can be pushed or not.
     */
    private boolean canPush;

    /**
     * Trigger id list which all can be fired at file change.
     */
    private Vector readyList;

    /**
     * Trigger id list which are not ready yet because of late start fire time.
     */
    private Vector waitList;

    /**
     * Trigger id count.
     */
    private Vector triggerCount;

    /**
     * Start time of triggers in waiting list.
     */
    private Vector startTimeList;

    /**
     * For searching purpose.
     */
    private int position;

    /**
     * For debugging purpose.
     */
    private static boolean DEBUG = EventDetector.DEBUG;


    /**
     * Constructor.
     * @param canPush     file change can be pushed or not
     * @param triggerId   trigger id
     * @param startTime   start time
     * @param installTime trigger install time
     */
    PushTriggerGroup ( boolean canPush, int triggerId, Date startTime, Date installTime ) {

	// Initializes two id list.
	this.canPush = canPush;
	this.readyList = new Vector();	
	this.waitList = new Vector( 0 );
	this.triggerCount = new Vector();
	this.startTimeList = new Vector( 0 );

	if ( ( startTime == null ) || ( startTime.compareTo( installTime ) <= 0 ) ) {

	    this.readyList.add( new Integer( triggerId ) );
	    this.triggerCount.add( new Integer( 1 ) );
	}
	else {

	    this.waitList.add( new Integer( triggerId ) );
	    this.startTimeList.add( startTime );
	}
    }


    /**
     * Adds a trigger to the group.
     * @param triggerId  trigger id
     * @param startTime  start fire time
     */
    synchronized void addTrigger ( int triggerId, Date startTime ) {

	Date now = new Date( System.currentTimeMillis() );
	Integer id = new Integer( triggerId );

	if ( ( startTime == null ) || ( startTime.compareTo( now ) <= 0 ) ) {

	    if ( findTrigger( triggerId ) ) {

		int count = ( ( Integer )this.triggerCount.remove( this.position ) ).intValue() + 1;
		this.triggerCount.add( this.position, new Integer( count ) );
	    }
	    else {

		this.readyList.add( this.position, id );
		this.triggerCount.add( this.position, new Integer( 1 ) );
	    }
	}
	else {

	    findTrigger( triggerId, startTime );
	    this.waitList.add( this.position, id );
	    this.startTimeList.add( this.position, startTime );
	}
    }


    /**
     * Gets the file flag.
     * @return true if file change can be pushed; otherwise false
     */
    boolean canPush () {

	return this.canPush;
    }


    /**
     * Finds the position of a trigger id on the ready list or where it should be. Sets position.
     * @param triggerId trigger id
     * @return true if trigger is found; otherwise false
     */
    synchronized private boolean findTrigger ( int triggerId ) {

	boolean found = false;
	int left = 0, right = this.readyList.size() - 1;

	if ( left > right ) {

	    this.position = 0;
	    return false;
	}

	// Binary search.
	int pos;
	while ( !found ) {

	    pos = ( left + right ) / 2;

	    int trigId = ( ( Integer )this.readyList.elementAt( pos ) ).intValue();

	    int seq = trigId - triggerId;

	    if ( seq  == 0 ) {

		this.position = pos;
		return true;
	    }
	    else { 

		if ( seq < 0 )
		    left = pos + 1;
		else
		    right = pos - 1;

		if ( left > right ) {

		    this.position = left;
		    break;
		}
	    }
	}

	return false;
    }


    /**
     * Finds the position of a trigger id on the ready list or where it should be. Sets position.
     * @param triggerId trigger id
     * @param startTime start time
     * @return true if trigger is found; otherwise false
     */
    synchronized private boolean findTrigger ( int triggerId, Date startTime ) {

	if ( this.waitList.size() == 0 ) {

	    this.position = 0;
	    return false;
	}
	else {

	    for ( int i = 0; i < this.waitList.size(); i++ ) {

		Date time = ( Date )this.startTimeList.elementAt( i );
		if ( time.compareTo( startTime ) > 0 ) {

		    this.position = i;
		    return false;
		}
		else if ( time.compareTo( startTime ) == 0 ) {

		    int id = ( ( Integer )this.waitList.elementAt( i ) ).intValue();
		    if ( id > triggerId ) {

			this.position = i;
			return false;
		    }
		    else if ( id == triggerId ) {

			this.position = i;
			return true;
		    }
		}
	    }
	}

	this.position = this.waitList.size();
	return false;
    }


    /**
     * Gets the number of push triggers on this file.
     * @return a number
     */
    int getTriggerCount () {

	return this.readyList.size() + this.waitList.size();
    }


    /**
     * Processess all ready-triggers and updates this process time.
     * @param triggerInfoList    trigger information list
     * @param file               file name
     * @param changeTime         updated last modified time of this file
     * @return a vector which contains all ready-trigger id's
     */
    synchronized Vector processTrigger ( TriggerInfoList triggerInfoList, String file, Date changeTime ) {

	Vector triggers = new Vector();

	// Searchs ready triggers on waiting list.
	for ( int i = 0; i < this.waitList.size(); ) {

	    Date time = ( Date )this.startTimeList.elementAt( i );
	    if ( time.compareTo( changeTime ) <= 0 ) {

		this.startTimeList.remove( i );
		int id = ( ( Integer )this.waitList.remove( i ) ).intValue();
		addTrigger( id, null );
	    }
	    else
		break;
	}

	// Gets every trigger on ready list.
	for ( int i = 0; i < this.readyList.size(); i++ ) {

	    int tid = ( ( Integer )this.readyList.elementAt( i ) ).intValue();

	    // Updates this process time of this trigger.
	    triggerInfoList.updateThisProcessTime( tid, file, changeTime );
	    triggers.add( new Integer( tid ) );
	}

	return triggers;
    }


    /**
     * Deletes a trigger from the group.
     * @param triggerId    trigger id
     * @param startTime    start time
     * @return true if the trigger is found and deleted successfully; otherwise false
     */
    synchronized boolean removeTrigger ( int triggerId, Date startTime ) {

	if ( this.waitList.size() > 0 ) {

	    Date time = ( Date )this.waitList.elementAt( 0 );

	    // The trigger is in wait list.
	    if ( time.compareTo( startTime ) <= 0 ) {

		if ( !findTrigger( triggerId, startTime ) )
		    return false;

		this.waitList.removeElementAt( this.position );
		this.startTimeList.removeElementAt( this.position );
		return true;
	    }
	}

	// The trigger has to be in ready list.
	if ( !findTrigger( triggerId ) )
	    return false;

	int count = ( ( Integer )this.triggerCount.remove( this.position ) ).intValue() - 1;
	if ( count < 0 ) {

	    if ( DEBUG )
		System.err.println( "EventDetector: Wrong trigger count of trigger "
				    + triggerId + " on push trigger list!" );

	    return false;
	}
	else if ( count == 0 )
	    this.readyList.removeElementAt( this.position );
	else
	    this.triggerCount.add( new Integer( count ) );

	return true;
    }
}



/**
 * <code>PushTriggerList</code> contains all push-triggers and their files.
 * All triggers are stored in a hashtable in which file names are keys and 
 * instances of <code>PushTriggerGroup</code> are values.
 * @see <code>PushTriggerGroup</code>
 */
class PushTriggerList {

    /**
     * Hashtable which uses file names as keys and push-trigger group as values.
     */
    private Hashtable list;

    /**
     * Trigger information list.
     */
    private TriggerInfoList triggerInfoList;

    /**
     * For debugging purpose.
     */
    private static boolean DEBUG = EventDetector.DEBUG;


    /**
     * Constructor.
     * @param triggerInfoList trigger info. list
     */
    PushTriggerList ( TriggerInfoList triggerInfoList ) {

	this.triggerInfoList = triggerInfoList;
	this.list = new Hashtable();
    }


    /**
     * Adds a trigger to the list.
     * @param triggerId      trigger id
     * @param fileName       file name
     * @param canPush        file change can be pushed or not
     * @param startTime      start fire time
     * @param installTime    globle install time for this trigger
     * @return true if the file is already on list; otherwise false
     */
    boolean addTrigger ( int triggerId, String fileName, boolean canPush, Date startTime, Date installTime ) {

	PushTriggerGroup trig;

	// Adds this trigger to the trigger infomation list.
	if ( startTime == null )
	    this.triggerInfoList.addTrigger( triggerId, fileName, installTime );
	else
	    this.triggerInfoList.addTrigger( triggerId, fileName, startTime );

	if ( this.list.containsKey( fileName ) ) {

	    trig = ( PushTriggerGroup )this.list.get( fileName );
	    trig.addTrigger( triggerId, startTime );
	    return true;
	}
	else {

	    trig = new PushTriggerGroup( canPush, triggerId, installTime, startTime );
	    this.list.put( fileName, trig );
	    return false;
	}
    }


    /**
     * Gets file feature.
     * @param file   file name
     * @return true if file change can be pushed; otherwise false
     */
    boolean canPush ( String file ) {

	PushTriggerGroup trig = ( PushTriggerGroup )this.list.get( file );

	if ( trig != null )
	    return trig.canPush();
	else
	    return false;
    }


    /**
     * Checks if a specific file has been already monitored.
     * @param file  file name
     * @return true if it has; otherwise false
     */
    boolean containsFile ( String file ) {

	return this.list.containsKey( file );
    }


    /**
     * Gets the number of triggers on a specified file.
     * @param fileName  file name
     * @return a number or -1 if file not found
     */
    int getTriggerCount ( String fileName ) {

	PushTriggerGroup trig = ( PushTriggerGroup )this.list.get( fileName );

	if ( trig == null ) {

	    if ( DEBUG )
		System.err.println( "EventDetector: File " + fileName + " not found in Push-Trigger-List!" );
	    return -1;
	}

	return trig.getTriggerCount();
    }


    /**
     * Deletes a trigger on specified file.
     * @param triggerId trigger id
     * @param fileName  related file name
     * @param startTime start time
     * @return true if deleting trigger successfully; otherwise false
     */
    boolean removeTrigger ( int triggerId, String fileName, Date startTime ) {

	PushTriggerGroup trig;

	if ( this.list.containsKey( fileName ) ) {

	    trig = ( PushTriggerGroup )this.list.get( fileName );
	    boolean ret = trig.removeTrigger( triggerId, startTime );

	    // Deletes empty file.
	    if ( ret && ( trig.getTriggerCount() == 0 ) )
		this.list.remove( fileName );

	    return ret;
	}
	else
	    return false;
    }


    /**
     * Sets new last modified time for a file, and fires all ready triggers on it.
     * @param fileName      file name
     * @param changeTime    file change time
     * @return a vector of triggers to be notified to trigger manager; or null if file not found
     */
    Vector setFileLastModified ( String fileName, Date changeTime ) {

	// Gets the push trigger group.
	PushTriggerGroup triggerGroup = ( PushTriggerGroup )this.list.get( fileName );

	// There're no push triggers on this file.
	if ( triggerGroup == null )
	    return null;

	return triggerGroup.processTrigger( this.triggerInfoList, fileName, changeTime );
    }
}



/**
 * <code>TimerTrigger</code> contains a trigger which will be process every a checking interval.
 * A time trigger may have several checking intervals because this may be a trigger in a group plan.
 */
class TimerTrigger {

    /**
     * trigger id
     */
    private int id;

    /**
     * interval list
     */
    private Hashtable intervalList;


    /**
     * Constructor. -- for a new timer trigger
     * @param triggerId     trigger id
     * @param interval      checking interval
     * @param count         count of specified "interval"
     */
    TimerTrigger ( int triggerId, long interval, int count ) {

	this.id = triggerId;

	this.intervalList = new Hashtable();
	this.intervalList.put( new Long( interval ), new Integer( count ) );
    }


    /**
     * Adds an interval to this timer trigger.
     * @param interval   checking interval
     * @param count      count of this "interval"
     */
    void addInterval ( long interval, int count ) {

	Long inter = new Long( interval );

	if ( this.intervalList.containsKey( inter ) ) {

	    int newcount = ( ( Integer )this.intervalList.get( inter ) ).intValue()
		           + count;
	    this.intervalList.put( inter, new Integer( newcount ) );
	}
	else
	    this.intervalList.put( inter, new Integer( count ) );
    }


    /**
     * equal method!
     * @param trig  another trigger
     * @return true if two triggers have same id; otherwise false
     */
    public boolean equals ( Object trig ) {

	try {

	    TimerTrigger t = ( TimerTrigger )trig;

	    int tid = t.getId();

	    if ( this.id == tid )
		return true;
	    else
		return false;

	} catch ( Exception e ) {

	    return false;
	}
    }


    /**
     * Gets trigger id.
     * @return trigger id
     */
    int getId () {

	return this.id;
    }


    /**
     * Gets interval list.
     * @return an enumeration of intervals
     */
    Enumeration getIntervalList () {

	return this.intervalList.keys();
    }


    /**
     * Gets interval count.
     * @param interval  interval
     * @return the count of this interval
     */
    int getIntervalCount ( long interval ) {

	Integer count = ( Integer )this.intervalList.get( new Long( interval ) );

	if ( count == null )
	    return -1;
	else
	    return count.intValue();
    }


    /**
     * Gets trigger count.
     * @return trigger count
     */
    int getTriggerCount () {

	return this.intervalList.size();
    }


    /**
     * Deletes a trigger.
     * @param interval  interval
     * @return true if interval found; otherwise false
     */
    boolean removeTrigger ( long interval ) {

	Long val = new Long( interval );
	Integer count = ( Integer )this.intervalList.get( val );

	if ( count == null )
	    return false;

	int c = count.intValue() - 1;
	if ( c == 0 )
	    this.intervalList.remove( val );
	else
	    this.intervalList.put( val, new Integer( c ) );

	return true;
    }
}



/**
 * <code>Event</code> contains all monitored file names and timer triggers
 * which will be processed at the same time.
 * @see <code>TimerTrigger</code>
 */
class Event {

    /**
     * Process time for this event.
     */
    private Date processTime;

    /**
     * File list ( key: file name; value: timer trigger list ).
     */
    private Hashtable fileList;

    /**
     * For searching purpose.
     */
    private int position;

    /**
     * For debugging purpose.
     */
    private static boolean DEBUG = EventDetector.DEBUG;


    /**
     * Constructor.
     * @param processTime   process time
     * @param fileName      file name
     * @param triggerId     trigger id
     * @param interval      checking interval
     * @param intervalNum   number of this "interval"s
     */
    Event ( Date processTime, String fileName, int triggerId, long interval, int intervalNum ) {

	this.processTime = processTime;

	this.fileList = new Hashtable();

	Vector triggerList = new Vector();
	TimerTrigger trig = new TimerTrigger( triggerId, interval, intervalNum );
	triggerList.add( trig );

	this.fileList.put( fileName, triggerList );
    }


    /**
     * Adds a timer trigger into this event.
     * @param fileName      file name
     * @param triggerId     trigger id
     * @param interval      checkint interval
     * @param intervalNum   number of this "interval"s
     */
    void addTrigger ( String fileName, int triggerId, long interval, int intervalNum ) {

	if ( this.fileList.containsKey( fileName ) ) {

	    Vector triggerList = ( Vector )this.fileList.get( fileName );

	    if ( findTrigger( triggerList, triggerId ) ) {

		TimerTrigger trigger = ( TimerTrigger )triggerList.elementAt( this.position );
		trigger.addInterval( interval, intervalNum );
	    }
	    else {

		TimerTrigger trigger = new TimerTrigger( triggerId, interval, intervalNum );
		triggerList.add( this.position, trigger );
	    }
	}
	else {

	    TimerTrigger trigger = new TimerTrigger( triggerId, interval, intervalNum );

	    Vector triggerList = new Vector();
	    triggerList.add( trigger );
	    this.fileList.put( fileName, triggerList );
	}
    }


    /**
     * Compares two events by their process time.
     * @param time   process time of the other event
     * @return 0 if equal; 1 if greater than; -1 if less than
     */
    int compareTo ( Date time ) {

	return this.processTime.compareTo( time );
    }


    /**
     * Finds the position of a trigger id on timer trigger list or where it should be.
     * Sets position.
     * @param triggerList  trigger list
     * @param triggerId    trigger id
     * @return true if trigger is found; otherwise false
     */
    synchronized private boolean findTrigger ( Vector triggerList, int triggerId ) {

	boolean found = false;
	int left = 0, right = triggerList.size() - 1;

	if ( left > right ) {

	    this.position = 0;
	    return false;
	}

	// Binary search.
	int pos;
	while ( !found ) {

	    pos = ( left + right ) / 2;

	    TimerTrigger trigger = ( TimerTrigger )triggerList.elementAt( pos );
	    int trigId = trigger.getId();

	    int seq = trigId - triggerId;

	    if ( seq  == 0 ) {

		this.position = pos;
		return true;
	    }
	    else { 

		if ( seq < 0 )
		    left = pos + 1;
		else
		    right = pos - 1;

		if ( left > right ) {

		    this.position = left;
		    break;
		}
	    }
	}

	return false;
    }


    /**
     * Gets all file names on this event.
     * @return an enumeration of file names
     */
    Enumeration getFileList () {

	return this.fileList.keys();
    }


    /**
     * Gets the process time of this event.
     * @return process time
     */
    Date getProcessTime () {

	return this.processTime;
    }


    /**
     * Gets the trigger list of a specified file on this event.
     * @param fileName file name
     * @return a vector of triggers
     */
    Vector getTriggerList ( String fileName ) {

	Vector triglist = ( Vector )this.fileList.get( fileName );

	if ( triglist == null ) {

	    if ( DEBUG )
		System.err.println( "EventDetector: File " + fileName + " not found on event!" );

	    return null;
	}

	return triglist;
    }


    /**
     * Deletes a trigger and its files from this event.
     * @param triggerId  trigger id
     * @param fileList   file list
     * @param interval   checking interval
     * @return true if trigger is found; otherwise false
     */
    synchronized boolean removeTrigger ( int triggerId, Vector fileList, long interval ) {

	boolean ret = true;

	for ( int i = 0; i < fileList.size(); i++ ) {

	    String file = ( String )fileList.elementAt( i );
	    Vector triggerList = ( Vector )this.fileList.get( file );

	    if ( triggerList == null )
		ret = false;
	    else {

		if ( !findTrigger( triggerList, triggerId ) )
		    ret = false;
		else {

		    TimerTrigger trigger = ( TimerTrigger )triggerList.elementAt( this.position );
		    if ( !trigger.removeTrigger( interval ) )
			ret = false;

		    if ( trigger.getTriggerCount() == 0 ) {

			triggerList.removeElementAt( this.position );

			if ( triggerList.size() == 0 )
			    this.fileList.remove( file );
		    }
		}
	    }
	}

	return ret;
    }
}



/**
 * <code>EventList</code> contains a list of events in time order.
 * @see <code>Event</code>
 */
class EventList {

    /**
     * List of events.
     */
    private Vector list;

    /**
     * For searching purpose.
     */
    private int position;

    /**
     * For debugging purpose.
     */
    private static boolean DEBUG = EventDetector.DEBUG;


    /**
     * Constructor.
     */
    EventList () {

	this.list = new Vector();
    }


    /**
     * Adds a trigger to this list.
     * @param triggerId     trigger id
     * @param fileName      file name
     * @param processTime   process time
     * @param interval      checking interval
     * @param intervalNum   number of "interval"s
     * @return true if timer needs to be adjusted
     */
    boolean addTrigger ( int triggerId, String fileName, Date processTime, long interval, int intervalNum ) {

	// Searches the right event to insert this trigger.
	if ( findEvent( processTime ) ) {

	    Event event = ( Event )this.list.elementAt( this.position );
	    event.addTrigger( fileName, triggerId, interval, intervalNum );
	    return false;
	}
	else {

	    Event event = new Event( processTime, fileName, triggerId, interval, intervalNum );
	    this.list.add( this.position, event );

	    if ( this.position == 0 )
		return true;
	    else
		return false;
	}
    }


    /**
     * Finds the entry of an event on the list by its process time, and sets proper position for this event.
     * @param processTime  processTime
     * @return true if event is found; otherwise false
     */
    synchronized private boolean findEvent ( Date processTime ) {

	boolean found = false;
	int left = 0, right = this.list.size() - 1;

	if ( left > right ) {

	    this.position = 0;
	    return false;
	}

	// Binary search.
	int pos;
	while ( !found ) {

	    pos = ( left + right ) / 2;

	    Event event = ( Event )this.list.elementAt( pos );

	    int seq = event.compareTo( processTime );

	    if ( seq  == 0 ) {

		this.position = pos;
		return true;
	    }
	    else { 

		if ( seq < 0 )
		    left = pos + 1;
		else
		    right = pos - 1;

		if ( left > right ) {

		    this.position = left;
		    break;
		}
	    }
	}

	return false;
    }


    /**
     * Finds the proper event for deleting triggers
     * @param startTime    start time
     * @param interval     interval
     * @return true if event is found; otherwise false
     */
    synchronized private boolean findEvent ( Date startTime, long interval ) {

	if ( this.list.size() == 0 )
	    return false;

	Date beginTime = getNextEventTime();

	if ( startTime.compareTo( beginTime ) >= 0 )
	    return findEvent( startTime );
	else {

	    long distance = beginTime.getTime() - startTime.getTime();
	    distance = ( distance + interval - 1 ) / interval * interval;
	    Date eventTime = new Date( startTime.getTime() + distance );

	    return findEvent( eventTime );
	}
    }


    /**
     * Gets the process time of next event on the list.
     * @return process time or null if no event found on list
     */
    Date getNextEventTime () {

	if ( ( this.list == null ) || ( this.list.size() == 0 ) )
	    return null;
	else {

	    Event event = ( Event )this.list.elementAt( 0 );
	    return event.getProcessTime();
	}
    }


    /**
     * Processes the first event on the list.
     * @param eventDetector    event detector
     * @param dataManager      data manager 
     * @param triggerInfoList  trigger timer information list
     * @return a hashtable which contains a list of files and to-be-fired triggers
     */
    Hashtable processEvent ( EventDetector eventDetector, DataManager dataManager, TriggerInfoList triggerInfoList ) {

	Hashtable fireTriggerList = new Hashtable();

	// Removes the first event from the event list.
	Event event = ( Event )this.list.remove( 0 );

	// Gets the process time.
	Date processTime = event.getProcessTime();

	// Checks each file on the event.
	Enumeration enum = event.getFileList();
	while ( enum.hasMoreElements() ) {

	    String file = ( String )enum.nextElement();

	    // Initializes checking time for optimizing checking. -- Left points for file change or unchange
	    Date lastChangeTime = new Date( 0 );
	    Date unChangedSince = processTime;

	    // Gets triggers on this file.
	    Vector triggerList = event.getTriggerList( file );
	    if ( ( triggerList == null ) || ( triggerList.size() == 0 ) )
		continue;

	    for( int i = 0; i < triggerList.size(); i++ ) {

		TimerTrigger trigger = ( TimerTrigger )triggerList.elementAt( i );
		int id = trigger.getId();

		// Gets the last processing time of this trigger on this file.
		Date lastProcessTime = triggerInfoList.getLastProcessTime( id, file );
		if ( DEBUG )
		    System.out.println( "EventDetector: Trigger " + id + " on file " + file
					+ " was processed on " + lastProcessTime );

		if ( lastProcessTime == null )
		    continue;

		boolean isModified;

		// Before the known last change time of file.
		if ( lastProcessTime.compareTo( lastChangeTime ) <= 0 )
		    isModified = true;

		// After the known earliest unchange time of file.
		else if ( lastProcessTime.compareTo( unChangedSince ) >= 0 )
		    isModified = false;

		// Checks with data manager.
		else
		    isModified = dataManager.getLastModified( file, lastProcessTime, processTime );

		if ( isModified )
		    lastChangeTime = lastProcessTime;
		else
		    unChangedSince = lastProcessTime;

		if ( DEBUG ) {

		    if ( isModified )
			System.out.println( "EventDetector: " + file + " was modified between "
					    + lastProcessTime + " and " + processTime );
		    else
			System.out.println( "EventDetector: " + file + " was NOT modified between "
					    + lastProcessTime + " and " + processTime );
		}

		// Adds this trigger and file to fired trigger list in order to notify trigger manager.
		if ( isModified ) {

		    // Updates this process time in trigger infomation list.
		    triggerInfoList.updateThisProcessTime( id, file, processTime );

		    // Sepcial action for the special timer-based trigger.
		    if ( eventDetector.isSpecialTrigger( id ) )
			eventDetector.setFileLastModified( file, processTime );

		    // For regular timer_based trigger.
		    else {

			Integer tid = new Integer( id );

			if ( fireTriggerList.containsKey( tid ) ) {

			    // Adds file name.
			    Vector files = ( Vector )fireTriggerList.get( tid );
			    files.add( file );
			}
			else {

			    // Adds a trigger.
			    Vector files = new Vector();
			    files.add( file );
			    fireTriggerList.put( tid, files );
			}
		    }

		    // Updates last process Time for the special timer-based trigger.
		    // Performs this only when file has been modified since last process.
		    if ( eventDetector.isSpecialTrigger( id ) )
			triggerInfoList.updateLastProcessTime( id, file );
		}

		// Move this trigger to another event on the list.
		Enumeration intervalList = trigger.getIntervalList();
		while ( intervalList.hasMoreElements() ) {

		    long interval = ( ( Long )intervalList.nextElement() ).longValue();
		    int interCount = trigger.getIntervalCount( interval );

		    if ( interCount <= 0 )
			continue;

		    Date nextProcessTime = new Date( processTime.getTime() + interval );

		    // Inserts this trigger to another event.
		    addTrigger( id, file, nextProcessTime, interval, interCount );
		}
	    }
	}

	return fireTriggerList;
    }


    /**
     * Deletes a trigger from this event list.
     * @param triggerId  trigger id
     * @param fileList   file list
     * @param startTime  start fire time
     * @param interval   checking interval
     * @return true if timer need to be adjusted; otherwise false
     */
    boolean removeTrigger ( int triggerId, Vector fileList, Date startTime, long interval ) {

	if ( !findEvent( startTime, interval ) ) {

	    if ( DEBUG )
		System.err.println( "EventDetector: Event not found!" );

	    return false;
	}

	Event event = ( Event )this.list.elementAt( this.position );
	return event.removeTrigger( triggerId, fileList, interval );
    }
}



/**
 * <code>FileIntervalList</code> contains all files and their checking interval information.
 * This class is used to notify the data manager that how long a file should be monitored.
 * To simplify the whole thing, this, currently, keeps only the longest interval.
 */
class FileIntervalList {

    /**
     * File list. ( Key: file name; Value: interval list )
     */
    private Hashtable list;

    /**
     * For debugging purpose.
     */
    private static boolean DEBUG = EventDetector.DEBUG;


    /**
     * Constructor.
     */
    FileIntervalList () {

	this.list = new Hashtable();
    }


    /**
     * Checks/Updates checking interval on a specified file
     * @param fileName  file name
     * @param interval  new interval
     * @return true if need to notify data manager; otherwise false
     */
    boolean addInterval ( String fileName, long interval ) {

	Long oldInterval = ( Long )this.list.get( fileName );

	if ( oldInterval == null ) {

	    this.list.put( fileName, new Long( interval ) );
	    return true;
	}
	else {

	    if ( interval > oldInterval.longValue() ) {

		this.list.put( fileName, new Long( interval ) );
		return true;
	    }
	    else
		return false;
	}
    }
}



/**
 * <code>EventDetector</code> is used to detect file changes and fire triggers
 * An <code>EventDetector</code> object is created by a trigger manager.  
 * Events of all installed triggers will be added to <code>EventDetector</code> 
 * which administrates timers and checks query source files periodically.  
 * Whenever an event occurs, <code>EventDetector</code> will fire proper triggers 
 * and send relative imformation, such as file changes, to trigger manager.
 * @see <code>TriggerTable</code>, <code>PushTriggerList</code>, <code>TimerTriggerList</code>, 
 * and <code>EventList</code>
 */
public class EventDetector implements Runnable {

    /**
     * For debugging purpose.
     */
    public static boolean DEBUG = false;

    /**
     * Special DTD file extension name for '*'.
     */
    public static final String WILD_CARD = ".dtd" ;

    /**
     * The "special" timer trigger which to check files which has change-based trigger.
     */
    private static final int SPECIAL_TRIGGER = -1;

    /**
     * Trigger information.
     */
    private TriggerTable triggerTable;
    private PushTriggerList pushTriggerList;
    private TriggerInfoList triggerInfoList;
    private EventList eventList;
    private FileIntervalList fileIntervalList;

    /**
     * Triggers to be fired ( In fact, to be notified to trigger manager ).
     */
    private FiredTriggers firedTriggers;

    /**
     * The data manager.
     */
    private DataManager dataManager;

    /**
     * The trigger manager.
     */
    private TriggerManager triggerManager;

    /**
     * Time interval between two consecutive file checkings. ( default = 1 min. )
     */
    private static final long MINIMAL_CHECK_INTERVAL = 10000;
    private long defaultCheckInterval = 60000;

    /**
     * Time interval for timer thread
     */
    private long sleepInterval;

    /**
     * True if there're triggers in "firedTriggers".
     */
    volatile private boolean firedTriggersAvailable;

    /**
     * True if current "firedTriggers" has been sent to the trigger manager.
     */
    volatile private boolean firedTriggersNotified;

    /**
     * Concurrency control flag for modifying data structure.
     */
    volatile private boolean canModify;
    volatile private boolean inModify;

    /**
     * True if the timer is waiting for next trigger event.
     */
    volatile private boolean timerWait;

    /**
     * Timer thread.
     */
    private Thread timerThread;


    /**
     * Constructor.
     * @param dataManager      data manager
     * @param triggerManager   trigger manager
     */
    EventDetector ( DataManager dataManager, TriggerManager triggerManager ) {

	this.dataManager = dataManager;
	this.triggerManager = triggerManager;

	this.triggerTable = new TriggerTable();
	this.triggerInfoList = new TriggerInfoList();
	this.pushTriggerList = new PushTriggerList( this.triggerInfoList );
	this.eventList = new EventList();
	this.fileIntervalList = new FileIntervalList();
	this.firedTriggers = null;

	// Initializes flags.
	this.firedTriggersAvailable = false;
	this.firedTriggersNotified = true;
	this.canModify = true;
	this.inModify = false;
	this.timerWait = false;

	// Creates a thread for timer.
	createTimer();
    }


    /**
     * Adds a trigger which has multiple files.
     * @param triggerName trigger name
     * @param triggerId   trigger id
     * @param files       a list of names of source files to be monitored
     * @param startTime   state process time
     * @param interval    time interval between two file checkings in milliseconds
     * @param expireTime  expire time
     * @param return true if the trigger is added successfully; otherwise false
     */ 
    synchronized public boolean addTrigger ( String triggerName, int triggerId, Vector files,
					     Date startTime, long interval, Date expireTime ) {

	for ( int i = 0; i < files.size(); i++ ) {

	    String file = ( String )files.elementAt( i );

	    if ( !addTrigger( triggerName, triggerId, file, startTime, interval, expireTime ) )
		return false;
	}

	return true;
    }


    /**
     * Adds a trigger with a single file.
     * @param triggerName trigger name
     * @param triggerId   trigger id
     * @param file        source file name
     * @param startTime   next start time
     * @param interval    time interval between two file checkings in milliseconds
     * @param expireTime  expire time
     * @param return true if the trigger is added successfully; otherwise false
     */ 
    synchronized public boolean addTrigger ( String triggerName, int triggerId, String file,
					     Date startTime, long interval, Date expireTime ) {
	// "*".
	if ( file.indexOf( WILD_CARD ) != -1 ) {

	    if ( DEBUG )
		System.out.println( "EventDetector: DTD file " + file );

	    Vector files = getSourceForDTD( this.dataManager, file );

	    if ( DEBUG ) {

		for ( int i = 0; i < files.size(); i++ )
		    System.out.println( "EventDetector: Add trigger on " + ( String )files.elementAt( i ) );
	    }

	    if ( files.size() > 0 )
		return addTrigger( triggerName, triggerId, files, startTime, interval, expireTime );
	    else {

		if ( DEBUG )
		    System.err.println( "EventDetector: No xml files found for " + file );
		return false;
	    }
	}

	// Checks trigger name.
	if ( triggerName == null ) {

	    if ( DEBUG )
		System.err.println( "EventDetector: Try to add a null name trigger!" );

	    return false;
	}

	// Checks trigger id.
	if ( triggerId < 0 ) {

	    if ( DEBUG )
		System.err.println( "EventDetector: Try to add a trigger with negative id!"
				    + " -- " + triggerName + " " + triggerId );

	    return false;
	}

	// Checks file name.
	if ( file == null ) {

	    if ( DEBUG )
		System.err.println( "EventDetector: Try to add a trigger with null file name!"
				    + " -- " + triggerName + " " + triggerId );

	    return false;
	}

	// Checks interval.
	if ( ( interval != 0 ) && ( interval < MINIMAL_CHECK_INTERVAL ) ) {

	    if ( DEBUG )
		System.err.println( "EventDetector: Try to add a trigger with invalid check interval!"
				    + " -- " + triggerName + " " + triggerId );

	    return false;
	}

	Date now = new Date( System.currentTimeMillis() );

	// Checks expire time.
	if ( ( expireTime != null ) && ( expireTime.compareTo( now ) <= 0 ) ) {

	    if ( DEBUG )
		System.err.println( "EventDetector: Try to add a trigger with past expire time!"
				    + " -- " + triggerName + " " + triggerId );

	    return false;
	}

	// Notifies data manager to monitor file. Gets true if source file change can be pushed.
	boolean byPush = this.dataManager.monitorTriggerFile( file, interval, now );

	// Critical section starts.
	if ( !lockTimer() )
	    return false;

	// Notifies the data manager if the check interval is big...
	if ( this.fileIntervalList.addInterval( file, interval ) )
	    this.dataManager.setTriggerFileSpan( file, interval );

	// Adds a push trigger.
	if  ( interval == 0 ) {

	    if ( startTime == null )
		startTime = now;

	    // Adds this trigger to the trigger table.
	    if ( !this.triggerTable.addTrigger( triggerName, triggerId, startTime, interval, expireTime ) ) {

		if ( DEBUG )
		    System.err.println( "EventDetector: Adding trigger " + triggerName + " "
					+ triggerId + " to trigger table failed!" );

		unlockTimer();
		return false;
	    }

	    // Adds this trigger to push list and sees if "file"'s already on list.
	    boolean fileFound = this.pushTriggerList.addTrigger( triggerId, file, byPush, startTime, now );

	    if ( DEBUG )
		System.out.println( "EventDetector: Adds a push trigger " + triggerId );

	    // Adds this trigger to trigger info list.
	    this.triggerInfoList.addTrigger( triggerId, file, now );

	    if ( DEBUG )
		System.out.println( "EventDetector: Adds trigger " + triggerId + " and file " + file + " to trigger info list!" );

	    // Adds special trigger for trigger cannot be pushed.
	    if ( !byPush ) {

		if ( DEBUG )
		    System.out.println( "EventDetector: Change on " + file + " cannot be pushed!" );

		// The file has not been monitored by the special trigger.
		if ( !fileFound ) {

		    triggerId = SPECIAL_TRIGGER;
		    interval = defaultCheckInterval;
		    if ( DEBUG )
			System.out.println( "EventDetector: Needs to add special trigger on file " + file );
		}
	    }
	}

	boolean toAdjust = false;

	// Adds trigger to trigger list and event list.
	if ( interval > 0 ) {

	    // Checks start process time.
	    if ( startTime == null ) {

		if ( DEBUG )
		    System.err.println( "EventDetector: Try to add a timer trigger without start time!"
					+ " -- " + triggerName + " " + triggerId );

		unlockTimer();
		return false;
	    }
	    else if ( now.compareTo( startTime ) >= 0 ) {

		if ( DEBUG )
		    System.err.println( "EventDetector: Try to add a timer trigger with past start time!"
					+ " -- " + triggerName + " " + triggerId );

		startTime = new Date( System.currentTimeMillis() + interval );
	    }

	    // Adds this trigger to trigger info list.
	    this.triggerInfoList.addTrigger( triggerId, file, now );

	    toAdjust = this.eventList.addTrigger( triggerId, file, startTime, interval, 1 );

	    // Checks if need to adjust timer.
	    if ( toAdjust )
		adjustTimer( startTime );

	    // Adds this trigger to the trigger table.
	    if ( !isSpecialTrigger( triggerId ) ) {

		if ( !this.triggerTable.addTrigger( triggerName, triggerId, startTime, interval, expireTime ) ) {

		    if ( DEBUG )
			System.err.println( "EventDetector: Adding trigger " + triggerName + " "
					    + triggerId + " to trigger table failed!" );

		    unlockTimer();
		    return false;
		}
	    }
	}

	unlockTimer();

	if ( DEBUG )
	    System.out.println( "EventDetector: Add a trigger: " + triggerName + " "
				+ triggerId + " " + file + " " + startTime + " " + interval );

	return true;
    }


    /**
     * Adjusts timer to a new process time.
     * @param startTime   start process time
     */
    synchronized private void adjustTimer ( Date processTime ) {

	Date now = new Date( System.currentTimeMillis() );

	if ( processTime.compareTo( now ) <= 0 ) {

	    if ( DEBUG )
		System.err.println( "EventDetector: This is not time machine!" );

	    this.sleepInterval = 1000;
	}

	this.sleepInterval = processTime.getTime() - now.getTime();
	if ( this.sleepInterval == 0 )
	    this.sleepInterval = 1000;

	if ( DEBUG )
	    System.out.println( "EventDetector: Updates next event time -- in " + this.sleepInterval + " msec." );

	if ( this.timerWait )
	    toNotifyAll();

	if ( DEBUG )
	    System.out.println( "EventDetector: Resets Timer to " + processTime.toString() );
    }


    /**
     * Creates the timer thread.
     */
    private void createTimer () {

	this.sleepInterval = -1;

	// Create Timer thread.
	this.timerThread = new Thread( this, "timerThread" );
	this.timerThread.start();
    }


    /**
     * Gets the default check interval.
     * @return checking interval in millisecond
     */
    synchronized public long getDefaultCheckInterval () {

	return this.defaultCheckInterval;
    }


    /**
     * Gets a list of trigger ids which should be processed by trigger manager.
     * This method should be called by <code>TriggerManager</code>
     * @return an instance of FiredTriggers
     */
    synchronized public FiredTriggers getFiredTriggers () {

	// Wait for changed being made.
	while ( !this.firedTriggersAvailable ) {

	    if ( DEBUG )
		System.out.println( "EventDetector: Waiting for fired triggers!" );

	    if ( !toWait() )
		return null;
	}

	// Gets fired trigger list, and cleans current one.
	FiredTriggers ft = this.firedTriggers;
	this.firedTriggers = null;

	this.firedTriggersNotified = true;

	this.firedTriggersAvailable = false;
	toNotifyAll();

	if ( DEBUG )
	    System.out.println( "EventDetector: Trigger manager notified!" );

	return ft;
    }


    /**
     * Gets (last process time, current process time] of a trigger on a specified file.
     * This method should be called by DataManager.
     * @param fileName  file name
     * @param triggerId trigger id
     * @return a vector which contains last process time and current process time.
     */
    synchronized public Vector getLastFireTime ( String fileName, int triggerId ) {

	return this.triggerInfoList.getProcessTimeRange( triggerId, fileName );
    }


    /**
     * Asks search engine to get a list of source files which conforms a specified DTD file.
     * @param dm       data manager
     * @param dtdName  dtd file name
     * @return a list of source urls
     */
    public static Vector getSourceForDTD ( DataManager dm, String dtdName ) {

	DTDInfo dtdInfo = null;
	String sequery = null;
	Vector dtdVector = new Vector();
	Vector sourceList = null;

	sequery = " conformsto \"" + dtdName + "\"";
	dtdVector.addElement( sequery );

	dtdInfo = dm.getDTDInfo( dtdVector );

	if ( dtdInfo != null )
	    sourceList = dtdInfo.getURLs();

	return sourceList;
    }


    /**
     * Gets the number of by-push triggers on a file.
     * This method is called by data manager because it needs to know 
     * how many push triggers will retrieve file change from it before
     * it can safely throw the change away.
     * @param file  file name
     * @return the number of triggers.
     */
    synchronized public int getTriggerCount ( String file ) {

	return this.pushTriggerList.getTriggerCount( file );
    }


    /**
     * Checks if a trigger is the special timer-based trigger.
     * @param triggerId  trigger id
     * @return true if it is; otherwise false
     */
    boolean isSpecialTrigger ( int triggerId ) {

	if ( triggerId == SPECIAL_TRIGGER )
	    return true;
	else
	    return false;
    }


    /**
     * Locks timer in order to modify data part.
     * @return true if lock successfully; otherwise false
     */
    synchronized private boolean lockTimer () {

	while ( !this.canModify ) {

	    if ( !toWait() )
		return false;
	}

	this.inModify = true;
	return true;
    }


    /**
     * Processes timer triggers.
     */
    synchronized private void processTrigger () {

	// Gets trigger list to be sent to trigger manager.
	Hashtable fireList = this.eventList.processEvent( this, this.dataManager, this.triggerInfoList );

	// No triggers to be notified to trigger manager.
	if ( ( fireList == null ) || ( fireList.size() == 0 ) ) {

	    if ( DEBUG )
		System.out.println( "EventDetector: No triggers to be notified trigger manager!" );

	    return;
	}

	// Adds triggers to fired trigger list.
	if ( !this.firedTriggersNotified ) {

	    // Disables getFiredTriggers.
	    this.firedTriggersAvailable = false;

	    // Adds triggers to already-there list.
	    this.firedTriggers.addTriggers( fireList );
	}
	else {

	    this.firedTriggers = new FiredTriggers();
	    this.firedTriggers.addTriggers( fireList );
	}

	this.firedTriggersAvailable = true;
	this.firedTriggersNotified = false;
	toNotifyAll();
    }


    /**
     * Processes triggers ( invoked by pushed changes ) and notifies trigger manager.
     * @param file        modified file name
     * @param triggerList to-be-fired trigger list
     */
    synchronized private void processTrigger ( String file, Vector triggerList ) {

	if ( DEBUG )
	    System.out.println( "EventDetector: Processes push triggers!" );

	if ( ( triggerList != null ) && ( triggerList.size() > 0 ) ) {

	    Hashtable fireList = new Hashtable();
	    for( int i = 0; i < triggerList.size(); i++ ) {

		Integer tid = ( Integer )triggerList.elementAt( i );

		Vector files = new Vector();
		files.add( file );
		fireList.put( tid, files );
	    }

	    if ( !this.firedTriggersNotified ) {

		// Disables getFiredTriggers.
		this.firedTriggersAvailable = false;

		this.firedTriggers.addTriggers( fireList );
	    }
	    else {

		this.firedTriggers = new FiredTriggers();
		this.firedTriggers.addTriggers( fireList );
	    }

	    this.firedTriggersAvailable = true;
	    this.firedTriggersNotified = false;
	    toNotifyAll();
	}
    }


    /**
     * Deletes a user defined trigger and all its split triggers.
     * @param triggerName  trigger name
     * @return id list.
     */
    synchronized public Vector removeTrigger ( String triggerName ) {

	if ( DEBUG )
	    System.out.println( "EventDetector: Try to delete trigger " + triggerName );

	// Locks timer first.
	lockTimer();

	// Checks trigger table.
	TriggerEntry trigger = this.triggerTable.removeTrigger( triggerName );

	if ( trigger == null ) {

	    if ( DEBUG )
		System.err.println( "EventDetector: Trigger " + triggerName + " not found in table!" );

	    unlockTimer();
	    return null;
	}

	if ( DEBUG )
	    System.out.println( "EventDetector: Trigger " + triggerName + " is deleted from trigger table!" );

	// Gets detail information.
	Vector idList = trigger.getIdList();
	Vector startTimeList = trigger.getStartTime();
	long interval = trigger.getInterval();

	// For change based triggers.
	if ( interval == 0 ) {

	    for ( int i = 0; i < idList.size(); i++ ) {

		int id = ( ( Integer )idList.elementAt( i ) ).intValue();
		Vector fileList = this.triggerInfoList.removeTrigger( id );

		if ( ( fileList == null ) || ( fileList.size() == 0 ) ) {

		    if ( DEBUG )
			System.err.println( "EventDetector: Trigger " + id + " not found on trigger information list!" );

		    continue;
		}
		else {

		    if ( DEBUG )
			System.out.println( "EventDetector: Trigger " + id + " is deleted from trigger information list!" );
		}

		Date startTime = ( Date )startTimeList.elementAt( 0 );

		// Deletes triggers from push trigger list.
		for ( int j = 0; j < fileList.size(); j++ ) {

		    String file = ( String )fileList.elementAt( i );
		    if ( !this.pushTriggerList.canPush( file ) ) {

			// Deletes special trigger from trigger information list.
			if ( !this.triggerInfoList.removeTrigger( SPECIAL_TRIGGER, file ) ) {

			    if ( DEBUG )
				System.err.println( "EventDetector: Trigger " + SPECIAL_TRIGGER + " and file "
						    + file + " not found on trigger information list!" );
			}

			// Deletes this trigger and file from event list.
			Vector oneFile = new Vector( 1 );
			oneFile.add( file );
			if ( !this.eventList.removeTrigger( SPECIAL_TRIGGER, oneFile, startTime, this.defaultCheckInterval ) ) {

			    if ( DEBUG )
				System.err.println( "EventDetector: Trigger " + SPECIAL_TRIGGER + " and file "
						    + file + " not found on event list!" );
			}
		    }

		    // Removes this trigger from push trigger list.
		    if ( !this.pushTriggerList.removeTrigger( id, file, startTime ) ) {

			if ( DEBUG )
			    System.err.println( "EventDetector: Trigger " + id + " and file "
						+ file + " not found on push trigger list!" );
		    }
		    else {

			if ( DEBUG )
			    System.err.println( "EventDetector: Trigger " + id
						+ " is deleted from push trigger list!" );
		    }
		}
	    }
	}
	else { // For timer based triggers.

	    for ( int i = 0; i < idList.size(); i++ ) {

		int id = ( ( Integer )idList.elementAt( i ) ).intValue();
		Vector fileList = this.triggerInfoList.removeTrigger( id );

		if ( ( fileList == null ) || ( fileList.size() == 0 ) ) {

		    if ( DEBUG )
			System.err.println( "EventDetector: Trigger " + id + " not found on trigger information list!" );

		    continue;
		}
		else {

		    if ( DEBUG )
			System.out.println( "EventDetector: Trigger " + id + " is deleted from trigger information list!" );
		}

		Date startTime = ( Date )startTimeList.elementAt( i );

		// Deletes triggers from event list.
		if ( !this.eventList.removeTrigger( id, fileList, startTime, interval ) ) {

		    if ( DEBUG )
			System.err.println( "EventDetector: Trigger " + id + " not found on event list!" );
		}
		else {

		    if ( DEBUG )
			System.out.println( "EventDetector: Trigger " + id + " is deleted from event list!" );
		}
	    }
	}

	return idList;
    }


    /**
     * Timer thread to process timer-based triggers.
     */
    public void run () {

	Thread myThread = Thread.currentThread();

	// Timer thread.
	if ( myThread == this.timerThread ) {

	    while ( true ) {

		while ( this.sleepInterval < 0 ) {

		    if ( DEBUG )
			System.out.println( "EventDetector: Timer thread is waiting for events..." );

		    this.timerWait = true;
		    toWait();
		}

		if ( DEBUG )
		    System.out.println( "EventDetector: Gets a timer event!" );

		this.timerWait = false;

		try {

		    // Sleep...
		    while ( this.sleepInterval >= 1000 ) {

			Thread.sleep( 1000 );

			this.sleepInterval -= 1000;
		    }

		} catch ( InterruptedException ie ) {

		    ie.printStackTrace();
		    return;
		}

		// Waits until data stucture has been modified.
		while ( this.inModify )
		    toWait();

		// Locks data structure.
		this.canModify = false;

		if ( DEBUG )
		    System.out.println( "EventDetector: Starts processing timer triggers!" );

		// Fires triggers.
		processTrigger();

		// Sets sleep interval.
		setTimer();

		// Unlocks data structure.
		this.canModify = true;
		toNotifyAll();
	    }
	}
    }


    /**
     * Sets the default check interval.
     * @param interval    interval in millisecond
     */
    synchronized public void setDefaultCheckInterval ( long interval ) {

	this.defaultCheckInterval = interval;
    }


    /**
     * Sets file change time stamp.
     * @param file      file name
     * @param timestamp time stamp
     */
    synchronized public void setFileLastModified ( String file, Date timestamp ) {

	Vector triggerList = this.pushTriggerList.setFileLastModified( file, timestamp );
	processTrigger( file, triggerList );
    }


    /**
     * Finds timer triggers to be processed next time and updates timer.
     */
    private void setTimer () {

	Date nextTime = this.eventList.getNextEventTime();

	if ( nextTime == null )
	    this.sleepInterval = -1;
	else {

	    Date now = new Date( System.currentTimeMillis() );

	    if ( nextTime.compareTo( now ) >0 ) {

		this.sleepInterval = nextTime.getTime() - now.getTime();
		if ( this.sleepInterval < 1000 )
		    this.sleepInterval = 1000;
	    }
	    else
		this.sleepInterval = 1000;
	}
    }


    /**
     * To notify all waiting threads.
     */
    synchronized private void toNotifyAll () {

	notifyAll();
    }


    /**
     * To wait.
     * @return false if fails to wait, otherwise true
     */
    synchronized private boolean toWait () {

	try {

	    wait();

	} catch ( InterruptedException ie ) {

	    if ( DEBUG ) {

		ie.printStackTrace();
		System.err.println( ie.getMessage() );
	    }
	    return false;
	}
	return true;
    }


    /**
     * Unlocks timer after modifying data part.
     */
    synchronized private void unlockTimer () {

	this.inModify = false;
	toNotifyAll();
    }
}
