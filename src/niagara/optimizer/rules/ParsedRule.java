/* $Id: ParsedRule.java,v 1.3 2003/08/01 17:29:06 tufte Exp $ */
package niagara.optimizer.rules;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.regex.Pattern;

import org.w3c.dom.Element;

import niagara.connection_server.Catalog;
import niagara.optimizer.colombia.*;
import niagara.utils.*;

/** A simple rule which maps (exactly) one logical operator 
 * to one initializable operator. */
public abstract class ParsedRule extends Rule {

    /** A simple boolean method an operator class that takes no arguments 
     * and checks that the rule is applicable */
    private Method conditionMethod;
    protected static final Object[] emptyConditionArgs = new Object[] {
    };
    protected static final Class[] emptyConditionArgTypes = new Class[] {
    };

    protected void parseCondition(
        Element e,
        Class c,
        Catalog catalog) {
        conditionMethod = null;
        String condition = e.getAttribute("condition");
        if (condition.length() != 0) {
            try {
                conditionMethod = c.getMethod(condition, getConditionArgTypes());
                if (conditionMethod.getReturnType() != Boolean.TYPE)
                    Catalog.confError(
                        "Condition methods should return a boolean");
            } catch (NoSuchMethodException nsme) {
                throw new PEException("Could not find method: " + condition);
            }
        }
    }

    /* If this rule fires, disallow firing these rules on the "before" expression */
    private String[] maskBeforeRuleNames;
    /* If this rule fires, disallow firing these rules on the "after" expression */
    private String[] maskRuleNames;

    protected ParsedRule(
        String name,
        int arity,
        Expr before,
        Expr after,
        String[] maskBeforeRuleNames,
        String[] maskRuleNames) {
        super(name, arity, before, after);
        this.maskBeforeRuleNames = maskBeforeRuleNames;
        this.maskRuleNames = maskRuleNames;
    }

    protected static String[] parseMask(Element e, String attrName) {
        Pattern commaSeparated = Pattern.compile(",");
        String maskStr = e.getAttribute(attrName);
        if (maskStr.length() > 0)
            return commaSeparated.split(maskStr);
        else
            return new String[] {
        };
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
                    getConditionArgs(before)))
                .booleanValue();
        } catch (InvocationTargetException ite) {
            throw new PEException("Could not invoke condition method");
        } catch (IllegalAccessException iae) {
            throw new PEException("Could not access condition method");
        }
    }

    protected Class[] getConditionArgTypes() {
        return emptyConditionArgTypes;
    }
    
    protected Object[] getConditionArgs(Expr before) {
        return emptyConditionArgs;
    }
    
    public void initialize() {
        for (int i = 0; i < maskBeforeRuleNames.length; i++)
            maskRuleBefore(maskBeforeRuleNames[i]);
        for (int i = 0; i < maskRuleNames.length; i++)
            maskRule(maskRuleNames[i]);
    }
}