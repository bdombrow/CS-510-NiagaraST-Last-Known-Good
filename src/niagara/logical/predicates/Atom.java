/* $Id: Atom.java,v 1.2 2004/05/20 22:10:15 vpapad Exp $ */
package niagara.logical.predicates;

/** Atoms participate in comparisons */
public interface Atom {
    void toXML(StringBuffer sb);
    boolean isConstant();
    boolean isVariable();
    boolean isPath();
}
