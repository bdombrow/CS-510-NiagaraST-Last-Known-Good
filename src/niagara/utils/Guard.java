package niagara.utils;

import java.util.Hashtable;
import java.util.Vector;

import niagara.exception.GuardException;
import niagara.logical.Variable;
import niagara.query_engine.TupleSchema;


/**
 * <code>Guard</code> encapsulates a hash table for use as input or output guards.
 * @author rfernand
 * @version 1.0
 *
 */
public class Guard {
	// Fields
	@SuppressWarnings("unchecked")
	private Hashtable _guard;

	// Properties
	/**
	 * Supported hash types
	 */
	public enum HashType { BOOLEAN, INTEGER };

	// Ctor
	/**
	 * Constructor
	 * 
	 * @param t type of hash value
	 */
	public Guard(HashType t) throws GuardException{
		switch(t) {
		case BOOLEAN :
			_guard = new Hashtable<String,Boolean>();
		case INTEGER:
			_guard = new Hashtable<String,Integer>();
		default :
			throw new GuardException("Unsupported Guard type");
		}
	}

	// Methods
	/**
	 * @param groupAttributeList tuple's group-by attributes
	 * @param schema tuple's schema
	 * @param tuple tuple whose values are to be added to the guard
	 * 
	 * @return hash key (string) for use in guards
	 */
	public static String hashKey(Vector groupAttributeList, TupleSchema schema, Tuple tuple) {
		Variable groupAttribute;
		int position;
		String key = "";		
		for( int i=0; i < groupAttributeList.size(); i++ ) {
			groupAttribute = (Variable)groupAttributeList.elementAt(i);
			position = schema.getPosition(groupAttribute.getName());
			key.concat(((BaseAttr)tuple.getAttribute(position)).attrVal().toString());		
		}
		return key;
	}

	/**
	 * 
	 * @param key hash key
	 * @param o hash entry's value
	 */
	public void add(String key, Object o) {
		_guard.put(key, o);
	}

	/** Check if given key is in the hash table
	 * 
	 * @param key hash key
	 * @return true if key is in hash table
	 */
	public boolean inHash(String key) {
		if(_guard.containsKey(key))
			return true;
		else return false;
	}

	/**
	 * 
	 * @param key hash key
	 * @return object in hash table
	 * @throws Exception
	 */
	public Object getValue(String key) throws GuardException{
		if (inHash(key)) {
			return _guard.get(key);
		}
		else throw new GuardException("Can't return value for non-existing key");
	}
}
