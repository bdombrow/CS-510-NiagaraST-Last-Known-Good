package niagara.xmlql_parser;

/**
 * abstract class to represent three different kind of condition : 1. InClause
 * e.g. Where <book> <author> .... </> </> IN book.xml 2. Set e.g. $v IN
 * {author, editor} 3. Predicates e.g. $v = "Ritchie"
 * 
 */

public interface condition {
	void dump(int i);
}
