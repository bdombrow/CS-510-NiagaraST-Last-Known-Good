/* $Id: Comparison.java,v 1.2 2004/05/20 22:10:15 vpapad Exp $ */
package niagara.logical.predicates;

import niagara.logical.*;
import niagara.optimizer.colombia.Attrs;
import niagara.utils.PEException;
import niagara.xmlql_parser.opType;

abstract public class Comparison extends Predicate {
    protected int operator;

    protected Comparison(int operator) {
        this.operator = operator;
    }

    public static Comparison newComparison(
        int operator,
        Atom left,
        Atom right) {
        if (left.isVariable() && right.isConstant())
            return new VarToConstComparison(
                operator,
                (Variable) left,
                (Constant) right);
        if (left.isVariable() && right.isVariable())
            return new VarToVarComparison(
                operator,
                (Variable) left,
                (Variable) right);
        if (left.isConstant() && right.isVariable())
            return new ConstToVarComparison(
                operator,
                (Constant) left,
                (Variable) right);
        
        if (left.isPath() && right.isConstant())
            return new PathToConstComparison(
                    operator,
                    (Path) left,
                    (Constant) right);
        throw new PEException("Cannot handle this comparison");
    }

    public abstract Atom getLeft();
    public abstract Atom getRight();

    public int getOperator() {
        return operator;
    }

    // Subclasses can and should implement split!
    public abstract And split(Attrs variables);
    
    public void beginXML(StringBuffer sb) {
        sb.append("<pred op='");
        sb.append(opType.getName(operator));
        sb.append("'>");
    }

    public void childrenInXML(StringBuffer sb) {
        getLeft().toXML(sb);
        getRight().toXML(sb);
    }

    /**
     * @see niagara.logical.Predicate#negation()
     */
    Predicate negation() {
        switch (operator) {
            case opType.EQ :
                return newComparison(opType.NEQ, getLeft(), getRight());
            case opType.NEQ :
                return newComparison(opType.EQ, getLeft(), getRight());
            case opType.LEQ :
                return newComparison(opType.GT, getLeft(), getRight());
            case opType.GT:
                return newComparison(opType.LEQ, getLeft(), getRight());
            case opType.LT:
                return newComparison(opType.GEQ, getLeft(), getRight());
            case opType.GEQ:
                return newComparison(opType.LT, getLeft(), getRight());
            default:
                throw new PEException("Unexpected operator code");
        }
    }
    
   
    /**
     * @see niagara.logical.Predicate#selectivity()
     */
    public float selectivity() {
        switch (operator) {
            case opType.EQ:
                return 0.1f;
            case opType.NEQ:
                return 0.9f;
            case opType.LEQ:
            case opType.GT:
            case opType.LT:
            case opType.GEQ:
                return 0.5f;
            default:
                throw new PEException("Unexpected operator code");
        }
    }
}
