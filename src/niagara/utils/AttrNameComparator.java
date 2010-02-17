package niagara.utils;

import java.io.Serializable;
import java.util.Comparator;

import org.w3c.dom.Attr;

/**
 * A <code> Comparator </code> which compares the names of two attributes based
 * on lexigraphical ordering. Used for sorting arrays of attributes based on
 * attribute name.
 * 
 * @version 1.0
 * 
 * @author Kristin Tufte
 */
@SuppressWarnings( { "unchecked", "serial" })
public class AttrNameComparator implements Comparator, Serializable {
	public AttrNameComparator() {
	}

	/**
	 * Compares two DOM Attr objects based on their name. Names are compared
	 * using the standard String.compare function. Arguments must implement the
	 * DOM Attr interface
	 * 
	 * @param o1
	 *            First Attr object to be compared.
	 * @param o2
	 *            Second Attr object to be compared.
	 * 
	 * @return Returns a negative int, zero, or positive int if o1 is less than,
	 *         equal to, or greater than o2, respectively.
	 */
	public int compare(Object o1, Object o2) {
		return ((Attr) o1).getName().compareTo(((Attr) o2).getName());
	}
}
