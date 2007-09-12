/* $Id: Predicate.java,v 1.4 2007/09/12 19:30:21 vpapad Exp $ */
package niagara.logical.predicates;

//import java.util.*;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Vector;

import org.w3c.dom.Element;
import org.w3c.dom.Node;

import niagara.logical.*;
import niagara.logical.UpdateableEquiJoinPredicateList;
import niagara.logical.path.RE;
import niagara.optimizer.colombia.Attrs;
import niagara.optimizer.colombia.LogicalProperty;
import niagara.physical.predicates.PredicateImpl;
import niagara.utils.CUtil;
import niagara.utils.PEException;
import niagara.xmlql_parser.REParser;
import niagara.xmlql_parser.Scanner;
import niagara.xmlql_parser.condition;
import niagara.xmlql_parser.opType;
import niagara.xmlql_parser.regExp;
import niagara.xmlql_parser.schemaAttribute;
import niagara.xmlql_parser.varTbl;
import niagara.connection_server.InvalidPlanException;

/** A generic predicate. Predicate objects are immutable. */
abstract public class Predicate implements condition {
    /** Get an implementation for this predicate */
    public abstract PredicateImpl getImplementation();

    /** Append all the variables referenced in this predicate to al*/
    public abstract void getReferencedVariables(ArrayList al);

    /** Split this predicate into a conjunction of two 
     * predicates: one that only references the specified
     * variables, and another with no such restrictions */
    public And split(Attrs variables) {
        // In the default case, we do not know how to split
        return new And(True.getTrue(), this);
    }

    /** Split this predicate into a conjunction of two 
     * predicates: a conjunction of equality comparisons 
     * between attributes from leftAttrs and rightAttrs (that can be used 
     * in a hash-based join), and another with no such restrictions */
    public Predicate splitEquiJoin(Attrs leftAttrs, Attrs rightAttrs) {
        // In the default case, we do not know how to split
        return new And(True.getTrue(), this);
    }

    public UpdateableEquiJoinPredicateList toEquiJoinPredicateList(
        Attrs leftAttrs,
        Attrs rightAttrs) {
        throw new PEException("Cannot convert an arbitrary predicate to an equijoin predicate list");
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
                assert(sa != null) : "Could not find variable: " + varName;
                sa = new schemaAttribute(sa);
                sa.setStreamId(1);
            }

            var.setSA(sa);
        }
    }

    public static Predicate loadFromXML(Element e,
					LogicalProperty[] inputProperties)
        throws InvalidPlanException {
    	if (e == null)
	    return True.getTrue();

        Element l, r;
        l = r = null;

        Node c = e.getFirstChild();
	if (c != null) {
	    do {
		if (c.getNodeType() == Node.ELEMENT_NODE) {
		    if (l == null)
			l = (Element) c;
		    else if (r == null)
			r = (Element) c;
		}
		c = c.getNextSibling();
	    } while (c != null);
	}

	String nodeName = e.getNodeName();
        if (nodeName.equals("false")) {
	    return False.getFalse();
	}
	else if (nodeName.equals("true")) {
	    return True.getTrue();
	}
        else if (nodeName.equals("and")) {
            Predicate left = loadFromXML(l, inputProperties);
            Predicate right = loadFromXML(r, inputProperties);

            return new And(left, right);
        } else if (nodeName.equals("or")) {
            Predicate left = loadFromXML(l, inputProperties);
            Predicate right = loadFromXML(r, inputProperties);

            return new Or(left, right);
        } else if (nodeName.equals("not")) {
            Predicate child = loadFromXML(l, inputProperties);

            return new Not(child);
        } else { // Relational operator
            Atom left = parseAtom(l, inputProperties);
            Atom right = parseAtom(r, inputProperties);
	    
	    int type = opType.getCode(e.getAttribute("op"));
	    if(type == opType.UNDEF) {
                throw new InvalidPlanException(
                    "Unrecognized predicate op: " + e.getAttribute("op"));
	    }

            return Comparison.newComparison(type, left, right);
            // XXX vpapad: removed various toVarList @#$@#,
            // supposed to help in toXML. Test it!
        }
    }

    // assume leftv is 0 and rightv is 1
    private static Atom parseAtom(Element e, 
				  LogicalProperty[] inputProperties)
        throws InvalidPlanException {

	LogicalProperty left = inputProperties[0];
	LogicalProperty right = null;
	if(inputProperties.length == 2)
	    right = inputProperties[1];

        String name = e.getNodeName();
        if (name.equals("number")) {
            String val = e.getAttribute("value");
            try {
                return new NumericConstant(val);
            } catch (NumberFormatException nfe) {
                throw new InvalidPlanException("Could not parse '" + val + "' as a number.");
            }
        }
        else if (name.equals("string"))
            return new StringConstant(e.getAttribute("value"));
        else if (name.equals("path")) {
            RE path = null;

            String regexpAttr = e.getAttribute("path");
            try {
                Scanner scanner = new Scanner(new StringReader(regexpAttr));
                REParser rep = new REParser(scanner);
                path  = regExp.regExp2RE((regExp) rep.parse().value);
                rep.done_parsing();
               } catch (InvalidPlanException ipe) {
                   String msg = ipe.getMessage();
                   throw new InvalidPlanException("Syntax error in regular expression: " + msg);
            } catch (Exception ex) { // ugh cup throws "Exception!!!"
                ex.printStackTrace();
                throw new InvalidPlanException(
                    "Error while parsing: " + regexpAttr);
            }
            
            return new Path((Variable) Variable.findVariable(inputProperties, 
                    e.getAttribute("var")), path);
        }
        else { //variable 
            String varname = e.getAttribute("value");
            // chop initial $ sign off
            if (varname.charAt(0) == '$')
                varname = varname.substring(1);
            Variable v = (Variable) left.getAttr(varname);
            if (v == null)
                v = (Variable) right.getAttr(varname);
            if (v != null)
                return v;
            else
                throw new InvalidPlanException(
                    "Unknown variable name: " + varname);
        }
    }

}
