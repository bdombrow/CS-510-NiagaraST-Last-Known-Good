
/**********************************************************************
  $Id: DuplicateHashtable.java,v 1.5 2007/04/30 19:25:43 vpapad Exp $


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


package niagara.utils;

import java.util.Hashtable;
import java.util.Vector;
import java.util.Enumeration;
import java.util.Set;

/**
 * This class provides a hash table implementation in which many objects can be
 * added with the same key. Thus a get, given a key, returns a list of objects
 * rather than a single object as with the JDK1.2 hashtable
 *
 * @version 1.0
 *
 */

public class DuplicateHashtable {

    ////////////////////////////////////////////////////////////////////////
    // These are the private members of the hash table                    //
    ////////////////////////////////////////////////////////////////////////

    // This is a regular hash table as the basis
    //
    Hashtable hashtable;


    ////////////////////////////////////////////////////////////////////////
    // These are private nested classes                                   //
    ////////////////////////////////////////////////////////////////////////

    /**
     * This is the class representing each entry in the hash table
     */

    private class HashtableEntry {

	/////////////////////////////////////////////////////////////
	// These are the private members of the hash table entry   //
	/////////////////////////////////////////////////////////////

	// This is the list of objects in the hash table entry
	//
	Vector objectList;


	/////////////////////////////////////////////////////////////
	// These are the methods of the hash table entry           //
	/////////////////////////////////////////////////////////////

	/**
	 * This is the constructor that initializes the hash table entry
	 */

	public HashtableEntry () {

	    // Initialize the object list
	    //
	    objectList = new Vector();
	}


	/**
	 * This function adds a new object to the entry
	 *
	 * @param objectToAdd The object to be added to the entry
	 */

	public void addObject (Object objectToAdd) {

	    // Add to object list
	    //
	    objectList.add(objectToAdd);
	}


	/**
	 * This function returns the array list of objects associated with this
	 * entry
	 *
	 * @return the vector of objects associated with this entry
	 */

	public Vector getObjects () {

	    // Return the object list
	    //
	    return objectList;
	}


	/**
	 * This function removes an object from the list associated with this entry
	 *
	 * @param objectToRemove The object to remove from this entry
	 *
	 * @return True if the object was present and removed; false otherwise
	 */

	public boolean removeObject (Object objectToRemove) {

	    // Remove object from list
	    //
	    return objectList.remove(objectToRemove);
	}


	/**
	 * This function returns the size of the list associated with this entry
	 *
	 * @return Size of list associated with this entry
	 */

	public int size () {

	    // Return the size of the list
	    //
	    return objectList.size();
	}
    }


    ////////////////////////////////////////////////////////////////////////
    // These are the methods of the duplicate hash table class            //
    ////////////////////////////////////////////////////////////////////////

    /**
     * Constructs a new empty hash table with a default capacity and the default
     * load factor, which is 0.75
     */

    public DuplicateHashtable () {

	// Initialize the hash table
	//
	hashtable = new Hashtable();
    }


    /**
     * Constructs a new empty hash table with the specified initial capacity and
     * the default load factor, which is 0.75
     *
     * @param initialCapacity the initial capacity of the hash table
     *
     * @exception java.lang.IllegalArgumentException if the initial capacity is
     *                                               less than zero
     */

    public DuplicateHashtable (int initialCapacity) {

	// Initialize the hash table
	//
	hashtable = new Hashtable(initialCapacity);
    }


    /**
     * Constructs a new empty hash table with the specified initial capacity and
     * the specified load factor
     *
     * @param initialCapacity the initial capacity of the hash table
     * @param loadFactor the load factor of the hash table
     *
     * @exception java.lang.IllegalArgumentException if the initial capacity is
     *                                               less than zero, or if the
     *                                               load factor is nonpositive
     */

    public DuplicateHashtable (int initialCapacity, float loadFactor) {

	// Initialize the hash table
	//
	hashtable = new Hashtable(initialCapacity, loadFactor);
    }


    /**
     * Maps the specified key to the specified value in this hash table
     *
     * @param key The hashtable key
     * @param value The value
     *
     * @exception java.lang.NullPointerException if the key is null
     */

    public void put (Object key, Object value) {

	// First try to get an entry with the same key, if it exists
	//
	HashtableEntry hashtableEntry = (HashtableEntry) hashtable.get(key);

	// If an entry does not already exist, create one
	//
	if (hashtableEntry == null) {
	    hashtableEntry = new HashtableEntry();
	}

	// Add the value to the hash entry
	//
	hashtableEntry.addObject(value);

	// Add the hashtable entry to the hashtable
	//
	hashtable.put(key, hashtableEntry);
    }


    /**
     * Returns the list of values to which the specified key is mapped to in the
     * hash table
     *
     * @param key A key in the hashtable
     *
     * @return the list of values to which the key is mapped in this hashtable; null
     *         if the key is not mapped to any value in this hashtable
     */

    public Vector get (Object key) {

	// Get the hash table entry corresponding to the key
	//
	HashtableEntry hashtableEntry = (HashtableEntry) hashtable.get(key);

	// If there is no entry, return null
	//
	if (hashtableEntry == null) {

	    return null;
	}
	else {

	    // Return the list of objects in the entry.
	    //
	    return hashtableEntry.getObjects();
	}
    }


    /**
     * Removes the (key, object) pair from the hash table. This method does nothing
     * if the key is not in the hashtable.
     *
     * @param key A key of the object to be removed
     * @param objectToRemove The object to be removed
     *
     * @return True if object was present and removed; false otherwise
     */

    public boolean remove (Object key, Object objectToRemove) {

	// Get the hash table entry corresponding to the key
	//
	HashtableEntry hashtableEntry = (HashtableEntry) hashtable.get(key);

	// If there is no entry, return null
	//
	if (hashtableEntry == null) {

	    return false;
	}
	else {

	    // Remove object from hash table entry
	    //
	    boolean removed = hashtableEntry.removeObject(objectToRemove);

	    // If remove was not successful, return that
	    //
	    if (!removed) return false;

	    // If the size of the hashtable entry is 0, remove entry
	    //
	    if (hashtableEntry.size() == 0) {

		hashtable.remove(key);
	    }

	    // Successful
	    //
	    return true;
	}
    }

    /**
     * Removes the key from the hashtable (and associated values)
     */

    public Object remove(Object key) {
	return hashtable.remove(key);
    }

    /**
     * Returns an enumeration of the keys for the hashtable
     */

    public Enumeration keys() {
	return hashtable.keys();
    }

    public Set keySet() {
    	return hashtable.keySet();
    }

    /**
     * Clears this hash table so that it contains no keys
     */

    public void clear () {

	// Clear the hash table
	//
	hashtable.clear();
    }

    public int size() {
        return hashtable.size();
    }
}
