/* $Id: Atom.java,v 1.1 2003/12/24 02:03:51 vpapad Exp $ */
package niagara.logical.predicates;

/** Atoms participate in comparisons */
public interface Atom {
    void toXML(StringBuffer sb);
    boolean isConstant();
    boolean isVariable();
}
