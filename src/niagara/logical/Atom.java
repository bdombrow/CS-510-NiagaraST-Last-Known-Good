/* $Id: Atom.java,v 1.1 2002/09/20 23:14:19 vpapad Exp $ */
package niagara.logical;

/** Atoms participate in comparisons */
public interface Atom {
    void toXML(StringBuffer sb);
    boolean isConstant();
    boolean isVariable();
}
