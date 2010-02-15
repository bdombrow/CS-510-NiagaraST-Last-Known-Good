package niagara.logical;

/**
 * StreamSpec - specification of a stream -
 * can be a file or a very simple firehose (hostname and port num)
 */

import java.io.PrintStream;
import java.lang.reflect.Array;
import niagara.utils.BaseAttr;

public class DBScanSpec {
	private String query_string;
	private String[] attr_names;
	private BaseAttr.Type[] attr_types;
	private String timeattr;
	private boolean oneTimeQ = false;

	/**
	 * Initialize the stream spec
	 */
	public DBScanSpec(String _query_string, String[] _attr_names,
			String[] _attr_type_names, String _timeattr, String type) {
		attr_types = new BaseAttr.Type[_attr_type_names.length];
		this.query_string = _query_string;
		this.attr_names = _attr_names;
		for(int i = 0; i< _attr_type_names.length; i++) {
			attr_types[i] = BaseAttr.getDataTypeFromString(_attr_type_names[i]);
		}
		timeattr = _timeattr;
		if (type.compareToIgnoreCase("one_time") == 0)
			oneTimeQ = true;

	}

	public String getQueryString() {
		return query_string;
	}

	public BaseAttr.Type getAttrType(int i) {
		return attr_types[i];
	}

	public String getAttrName(int i) {
		return attr_names[i];
	}

	public int getNumAttrs() {
		return Array.getLength(attr_types);
	}

	public String getTimeAttr() {
		return timeattr;
	}
	public boolean oneTime() {
		return oneTimeQ;
	}

	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof DBScanSpec)) return false;
		DBScanSpec other = (DBScanSpec)obj;
		if(other.query_string.equals(query_string) &&
				other.attr_types.equals(attr_types)) 
			return true;

		return false;
	}    

	public void dump(PrintStream os) {
		os.println("Stream Specification: ");
		os.println("DB Stream: queryString: " + query_string);
		os.println("DB Stream: time attr: " + timeattr);
	}



}
