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
	private Hashtable<String, GuardObject> _guard;

	private class GuardObject {

		private HashType _type;
		private Object _value;
		private Boolean _status;

		public GuardObject(HashType t, Boolean s) throws GuardException{
			switch(t) {
			case INTEGER:
				_type = t;
				_value = 0;
				_status = s;
			default:
				throw new GuardException("Unsupported type in Guard Object");
			}
		}

		public int getValue() throws GuardException {
			if(this._type == HashType.INTEGER)
				return (Integer)_value;
			else throw new GuardException("Attempted to get integer from non-integer Guard Object");
		}

		public Boolean getStatus(){
			return _status;
		}


		public void updateValue(int o) throws GuardException{
			if(this._type == HashType.INTEGER)
				_value = o;
			else
				throw new GuardException("Attempted to update a non-integer Guard Object.");
		}

		public void updateStatus(Boolean s) {
			_status = s;	
		}
	}

	// Properties
	/**
	 * Supported hash types
	 */
	public enum HashType { INTEGER };

	// Ctor
	/**
	 * Constructor
	 * 
	 * @param t type of hash value
	 */
	public Guard(){
		_guard = new Hashtable<String,GuardObject>();
	}

	// Methods
	/**
	 * @param groupAttributeList tuple's group-by attributes
	 * @param schema tuple's schema
	 * @param tuple tuple whose values are to be added to the guard
	 * 
	 * @return hash key (string) for use in guards
	 */
	public static String hashKey(Vector<Variable> groupAttributeList, TupleSchema schema, Tuple tuple) {
		Variable groupAttribute;
		int position;
		String key = "";		
		for( int i=0; i < groupAttributeList.size(); i++ ) {
			groupAttribute = groupAttributeList.elementAt(i);
			position = schema.getPosition(groupAttribute.getName());
			key.concat(((BaseAttr)tuple.getAttribute(position)).attrVal().toString());		
		}
		return key;
	}

	/**
	 * 
	 * @param key hash key
	 * @param i hash entry's value
	 * @param s status of guard
	 */
	public void add(String key, Integer i, Boolean s) {
		try {
			GuardObject o = new GuardObject(HashType.INTEGER, s);
			o.updateValue(i);
			_guard.put(key, o);
		} catch (GuardException o) {
			System.err.println(o.getMessage());
		}
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
	 * @return integer value in hash table
	 * @throws Exception
	 */
	public int getValue(String key) throws GuardException{
		if (inHash(key)) {
			return _guard.get(key).getValue();
		}
		else throw new GuardException("Can't return value for non-existing key");
	}

	/**
	 * 
	 * @param key hash key
	 * @return guard status
	 * @throws GuardException
	 */
	public Boolean getStatus(String key) throws GuardException {
		if (inHash(key)) {
			return _guard.get(key).getStatus();
		}
		else throw new GuardException("Can't return status for non-existing key");

	}

	/**
	 * 
	 * @param key hash key
	 * @param s status
	 * @throws GuardException
	 */
	public void updateStatus(String key, Boolean s) throws GuardException {
		if(inHash(key)) {
			_guard.get(key).updateStatus(s);
		}
	}
}
