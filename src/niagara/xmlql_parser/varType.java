package niagara.xmlql_parser;

/**
 * This class defines different type of variables
 * 
 */

public class varType {

	public static final int TAG_VAR = 0; // for tag variable
	public static final int ELEMENT_VAR = 1; // for element_as
	public static final int CONTENT_VAR = 2; // for content_as
	public static final int NULL_VAR = 3;

	public static String[] names = { "tag", "element", "content", "null" };
}
