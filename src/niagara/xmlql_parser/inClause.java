package niagara.xmlql_parser;

import java.util.Vector;

@SuppressWarnings("unchecked")
/**
 *
 * This class is used to represent the inClause
 *    e.g.
 *         <book>
 *             <author> $a </>
 *         </> IN "*" Conform_To xyz.dtd
 *
 *
 */
public class inClause implements condition {
	private pattern pat; // pattern with the element names
	private Vector source; // source of data to query
	private String dtdType;// dtd type of documents to query (optional)

	/**
	 * Constructor
	 * 
	 * @param pattern
	 *            of the Where clause
	 * @param list
	 *            of documents to query
	 * @param dtd
	 *            type of the documents to query
	 */

	public inClause(pattern _pat, Vector _source, String _dtdType) {
		pat = _pat;
		source = _source;
		dtdType = _dtdType;
	}

	/**
	 * @return the pattern
	 */
	public pattern getPattern() {
		return pat;
	}

	/**
	 * @return the list of documents
	 */
	public Vector getSources() {
		return source;
	}

	/**
	 * @return dtd type of the documents
	 */
	public String getDtdType() {
		return dtdType;
	}

	/**
	 * display this to standard output
	 * 
	 * @param number
	 *            of tabs at the beginning of each line
	 */
	public void dump(int j) {
		System.out.println("IN CLAUSE");
		pat.dump(0);
		System.out.println("Source of Documents");
		for (int i = 0; i < source.size(); i++)
			((data) source.elementAt(i)).dump(1);
		if (dtdType != null)
			System.out.println("ConformsTo: " + dtdType);
	}
}
