package niagara.xmlql_parser;

/**
 * This class declares the diffrent type a data object can be
 * 
 */

public class dataType {
	public static final int IDEN = 1; // for the time being its same
	public static final int STRING = 1; // may have to be changed later

	public static final int VAR = 3; // variables
	public static final int NUMBER = 4; // not used now
	public static final int ATTR = 5; // for schemaAttribute

	public static final int TAGVAR = 10; // Tag Variables
	public static final int ELEMENT_AS = 11; // if the variable is for
	// element_as
	public static final int CONTENT_AS = 12; // if the variable is for
	// content_as
}
