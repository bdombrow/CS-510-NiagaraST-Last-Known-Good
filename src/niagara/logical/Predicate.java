/* $Id: Predicate.java,v 1.1 2002/10/06 23:40:13 vpapad Exp $ */
package niagara.logical;

import java.util.*;

import niagara.optimizer.colombia.Attrs;
import niagara.query_engine.PredicateImpl;
import niagara.utils.CUtil;
import niagara.xmlql_parser.syntax_tree.condition;
import niagara.xmlql_parser.syntax_tree.schemaAttribute;
import niagara.xmlql_parser.syntax_tree.varTbl;

abstract public class Predicate implements condition {
    /** Get an implementation for this predicate */
    public abstract PredicateImpl getImplementation();

    /** Gather all the variable names referenced in this predicate */
    public abstract void getReferencedVariables(ArrayList al);

    public abstract Predicate copy();

    /** Split this predicate into a conjunction of two 
     * predicates: one that only references the specified
     * variables, and another with no such restrictions */
    public Predicate split(Attrs variables) {
        // In the default case, we do not know how to split
        return new And(True.getTrue(), this);
    }
    
    abstract Predicate negation();

    public void toXML(StringBuffer sb) {
        beginXML(sb);
        childrenInXML(sb);
        endXML(sb);
    }

    /** starting tag of the predicate's XML representation */
    public abstract void beginXML(StringBuffer sb);

    public void endXML(StringBuffer sb) {
        sb.append("</pred>");
    }

    public abstract void childrenInXML(StringBuffer sb);

    public abstract int hashCode();
    public abstract boolean equals(Object o);

    /** Rough selectivity estimate for this predicate.
     * Heuristics solidly based on thin air! */
    abstract public float selectivity();

    /**print the predicate to stdout */
    public void dump(int depth) {
        CUtil.genTab(depth);
        StringBuffer sb = new StringBuffer();
        toXML(sb);
        System.out.println(sb);
    }

    // Following is only for compatibility with old XML-QL code
    public Vector getVarList() {
        ArrayList al = new ArrayList();
        getReferencedVariables(al);
        Vector result = new Vector();
        for (int i = 0; i < al.size(); i++) {
            Variable var = (Variable) al.get(i);
            result.add(var.getName());
        }
        return result;
    }

    public void replaceVar(varTbl varTable) {
        ArrayList al = new ArrayList();
        getReferencedVariables(al);
        for (int i = 0; i < al.size(); i++) {
            OldVariable var = (OldVariable) al.get(i);
            String varName = var.getName();
            schemaAttribute sa = varTable.lookUp(varName);
            assert(sa != null) : "Could not find binding for variable: "
                + varName;
            var.setSA(new schemaAttribute(sa));
        }
    }

    public void replaceVar(varTbl left, varTbl right) {
        ArrayList al = new ArrayList();
        getReferencedVariables(al);
        for (int i = 0; i < al.size(); i++) {
            OldVariable var = (OldVariable) al.get(i);
            String varName = var.getName();
            schemaAttribute sa = left.lookUp(varName);
            if (sa != null) {
                sa = new schemaAttribute(sa);
            } else {
                sa = right.lookUp(varName);
                assert (sa != null) : "Could not find variable: " + varName;
                sa = new schemaAttribute(sa);
                sa.setStreamId(1);
            }
            
            var.setSA(sa);
        }
    }
}