package niagara.optimizer.rules;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.w3c.dom.Element;

import niagara.connection_server.Catalog;
import niagara.connection_server.ConfigurationError;
import niagara.optimizer.colombia.*;
import niagara.utils.*;

/** A simple rule which maps (exactly) one logical operator 
 * to one initializable operator. */
public class SimpleRule extends Rule {

    /** A boolean method of the logical operator class that takes no arguments 
     * and checks that the rule is applicable */
    private Method conditionMethod;
    private static final Object[] conditionArgs = new Object[] {
    };
    private static final Class[] conditionArgTypes = new Class[] {
    };

    private SimpleRule(
        String name,
        int arity,
        Expr before,
        Expr after,
        Method conditionMethod) {
        super(name, arity, before, after);
        this.conditionMethod = conditionMethod;
    }

    public Rule copy() {
        return new SimpleRule(
            name,
            arity,
            new Expr(pattern),
            new Expr(substitute),
            conditionMethod);
    }

    public static SimpleRule fromXML(Element e, Catalog catalog) {
        String name = e.getAttribute("name");
        if (name.length() == 0)
            confError("Each rule must have a name attribute");

        ConfigurationError checkPatterns =
            new ConfigurationError("Rules must contain one 'before' and one 'after' pattern");

        Element beforeOp =
            XMLUtils.getOnlyElementChild(
                XMLUtils.getFirstElementChild(e, "before", checkPatterns),
                "logical",
                new ConfigurationError("Before pattern must consist of exactly one logical operator"));
        Element afterOp =
            XMLUtils.getOnlyElementChild(
                XMLUtils.getFirstElementChild(e, "after", checkPatterns),
                "op",
                new ConfigurationError("After pattern must consist of exactly one operator"));

        // Construct before pattern
        String logicalOpName = beforeOp.getAttribute("name");
        Class logicalClass = catalog.getOperatorClass(logicalOpName);
        if (logicalClass == null)
            confError(
                "Could not find logical operator with name: " + logicalOpName);

        Expr beforeExpr = null;
        LogicalOp logicalOp = null;
        Method conditionMethod = null;
        int arity = 0;

        try {
            // Logical operator must have a zero-argument public constructor
            Constructor logicalConstructor =
                logicalClass.getConstructor(new Class[] {
            });

            String condition = beforeOp.getAttribute("condition");
            if (condition.length() != 0) {
                conditionMethod =
                    logicalClass.getMethod(condition, conditionArgTypes);
                if (conditionMethod == null)
                    confError("Could not find condition method " + condition);
                if (conditionMethod.getReturnType() != Boolean.TYPE)
                    confError("Condition methods should return a boolean");
            }

            logicalOp =
                (LogicalOp) logicalConstructor.newInstance(new Object[] {
            });

            arity = logicalOp.getArity();
            beforeExpr = new Expr(logicalOp, new Expr[arity]);
            for (int i = 0; i < arity; i++) {
                beforeExpr.setInput(i, new Expr(new LEAF_OP(i)));
            }
        } catch (NoSuchMethodException nsme) {
            throw new PEException(
                "Constructor could not be found for: " + logicalOpName);
        } catch (InvocationTargetException ite) {
            throw new PEException(
                "Constructor of "
                    + logicalClass
                    + " has thrown an exception: "
                    + ite.getTargetException());
        } catch (IllegalAccessException iae) {
            throw new PEException(
                "An illegal access exception occured: " + iae);
        } catch (InstantiationException ie) {
            throw new PEException("An instantiation exception occured: " + ie);
        }

        String afterOpName = afterOp.getAttribute("name");
        Class afterClass = catalog.getOperatorClass(afterOpName);
        if (afterClass == null)
            confError(
                "Could not find operator with name: "
                    + afterOpName);

        Expr afterExpr = null;
        try {
            // After operator must have a zero-argument public constructor
            Constructor afterConstructor =
                afterClass.getConstructor(new Class[] {
            });
            Op afterOperator =
                (Op) afterConstructor.newInstance(new Object[] {
            });
            afterExpr = new Expr(afterOperator, new Expr[arity]);
            for (int i = 0; i < arity; i++) {
                afterExpr.setInput(i, new Expr(new LEAF_OP(i)));
            }
        } catch (NoSuchMethodException nsme) {
            throw new PEException(
                "Constructor could not be found for: " + afterOpName);
        } catch (InvocationTargetException ite) {
            throw new PEException(
                "Constructor of "
                    + afterClass
                    + " has thrown an exception: "
                    + ite.getTargetException());
        } catch (IllegalAccessException iae) {
            throw new PEException(
                "An illegal access exception occured: " + iae);
        } catch (InstantiationException ie) {
            throw new PEException("An instantiation exception occured: " + ie);
        }

        return new SimpleRule(
            name,
            arity,
            beforeExpr,
            afterExpr,
            conditionMethod);
    }

    private static void confError(String msg) {
        throw new ConfigurationError(msg);
    }

    /**
     * @see niagara.optimizer.colombia.Rule#next_substitute(Expr, MExpr, PhysicalProperty)
     */
    public Expr next_substitute(
        Expr before,
        MExpr mExpr,
        PhysicalProperty ReqdProp) {
        Op op = (Op) substitute.getOp().copy();
        ((Initializable) op).initFrom((LogicalOp) before.getOp());

        Expr result = new Expr(op, new Expr[arity]);
        for (int i = 0; i < arity; i++) {
            result.setInput(i, before.getInput(i));
        }
        return result;
    }

    /**
     * @see niagara.optimizer.colombia.Rule#condition(Expr, MExpr, PhysicalProperty)
     */
    public boolean condition(
        Expr before,
        MExpr mexpr,
        PhysicalProperty ReqdProp) {
        if (conditionMethod == null)
            return true;
        try {
            return (
                (Boolean) conditionMethod.invoke(
                    before.getOp(),
                    conditionArgs))
                .booleanValue();
        } catch (InvocationTargetException ite) {
            throw new PEException("Could not invoke condition method");
        } catch (IllegalAccessException iae) {
            throw new PEException("Could not access condition method");
        }
    }
}