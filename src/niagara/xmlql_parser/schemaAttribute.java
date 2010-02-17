package niagara.xmlql_parser;

import niagara.utils.CUtil;

/** 
 * This is analogous to attributes of schema in the relational world. 
 * We build a schema by reading the element and attribute names to scan and
 * storing the parent-child relationship among them.
 * e.g.
 *         <book> <author> $a </> </> ....
 * will give rise to following schema
 *               0       1       2
 *          ---------------------------
 *          | doc root | book | author|
 *          |----------|------|-------|
 *          |    -1    |   0  |   1   |
 *          ---------------------------
 * each such entry is called a SchemaUnit. (3 in the above example)
 * This class stores information about the attribute which includes position 
 * of the SchemaUnit in the given schema. In the given example the 
 * schemaAttribute for $a will contain 2 as the attrNumber.
 *
 */
public class schemaAttribute {
	int attrNumber; // position of the SchemaUnit in the Schema
	int streamid; // by default it is 0, but in case of join
	// the schemaAttribute for right stream will
	// have one as its value
	regExp path; // regular expression for this attribute from the
	// the root

	int type; // CONTENT_VAR,ELEMENT_VAR,TAG_VAR

	String name;

	/**
	 * Constructor
	 * 
	 * @param the
	 *            position of the SchemaUnit
	 */

	public schemaAttribute(int num, String tagName) {
		attrNumber = num;
		path = null;
		type = varType.CONTENT_VAR;
		streamid = 0;
		name = tagName;
	}

	/**
	 * Constructor
	 * 
	 * @param schemaAttribute
	 *            to clone
	 */

	public schemaAttribute(schemaAttribute sa) {
		attrNumber = sa.attrNumber;
		streamid = sa.streamid;
		path = sa.path;
		type = sa.type;
	}

	/**
	 * Constructor
	 * 
	 * @param position
	 *            of the SchemaUnit
	 * @param type
	 *            of the variable which represents this schemaAttribute
	 */

	public schemaAttribute(int num, int _type, String tagName) {
		attrNumber = num;
		path = null;
		type = _type;
		streamid = 0;
		name = tagName;
	}

	/**
	 * Constructor
	 * 
	 * @param the
	 *            position of SchemaUnit
	 * @param regular
	 *            expression that represents the path to this element from the
	 *            root
	 */

	public schemaAttribute(int num, regExp re, String tagName) {
		attrNumber = num;
		path = re;
		streamid = 0;
		name = tagName;
	}

	/**
	 * shift the position of SchemaUnit. Used when two Schemas are merged and
	 * all the schemaAttributes for the second Schema needs to shifted by the
	 * length of the first Schema
	 * 
	 * @param the
	 *            length by which the attribute number has to be increased
	 */

	public void shift(int dist) {
		attrNumber += dist;
	}

	/**
	 * @return the position of the attribute
	 */

	public int getPos() {
		return attrNumber;
	}

	/**
	 * @return the position of the attribute
	 */

	public int getAttrId() {
		return attrNumber;
	}

	/**
	 * @return the stream number (0 for left, 1 for right)
	 */

	public int getStreamId() {
		return streamid;
	}

	/**
	 * @param the
	 *            stream number
	 */

	public void setStreamId(int id) {
		streamid = id;
	}

	/**
	 * @return the regular expression that represents the path to this element
	 *         from the root
	 */

	public regExp getPath() {
		return path;
	}

	/**
	 * @return the attribute number
	 */

	public int value() {
		return attrNumber;
	}

	/**
	 * @return the type of the variable that represents this schemaAttribute
	 */

	public int getType() {
		return type;
	}

	/**
	 * @return the name of the variable that represents this schemaAttribute
	 */
	public String getName() {
		return name;
	}

	/**
	 * prints to the standard output
	 * 
	 * @param number
	 *            of tabs at the beginning of each line
	 */

	public void dump(int depth) {
		CUtil.genTab(depth);
		dump();
	}

	/**
	 * prints to the standard output
	 */

	public void dump() {
		System.out.print("Stream id: " + streamid + "Attr number: $"
				+ attrNumber + "Type: " + type + "Path:  ");
		if (path != null) {
			path.dump(0);
		} else {
			System.out.print("null");
		}
		System.out.println();
	}
}
