package niagara.logical;

/**
 * StreamSpec - specification of a CSV stream 
 */

import java.io.PrintStream;
import java.lang.reflect.Array;
import niagara.utils.BaseAttr;

public class CSVSpec {
	private String file_name;
	private String[] attr_names;
	private BaseAttr.Type[] attr_types;
	private boolean oneTimeQ = false;

	/**
	 * Initialize the stream spec
	 */
	public CSVSpec(String _file_name, String[] _attr_names,
			String[] _attr_type_names, String type) {
		attr_types = new BaseAttr.Type[_attr_type_names.length];
		this.file_name = _file_name;
		this.attr_names = _attr_names;
		for(int i = 0; i< _attr_type_names.length; i++) {
			attr_types[i] = BaseAttr.getDataTypeFromString(_attr_type_names[i]);
		}

		if (type.compareToIgnoreCase("one_time") == 0)
			oneTimeQ = true;

	}

	public String getFileName() {
		return file_name;
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

	public boolean oneTime() {
		return oneTimeQ;
	}

	public boolean equals(Object obj) {
		if (obj == null || !(obj instanceof CSVSpec)) return false;
		CSVSpec other = (CSVSpec)obj;
		if(other.file_name.equals(file_name) &&
				other.attr_types.equals(attr_types)) 
			return true;

		return false;
	}    

	public void dump(PrintStream os) {
		os.println("Stream Specification: ");
		os.println("DB Stream: fileName: " + file_name);
	}



}
