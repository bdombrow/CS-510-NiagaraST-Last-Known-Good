/* $Id: CustomRule.java,v 1.2 2003/06/03 07:56:53 vpapad Exp $ */
package niagara.optimizer.rules;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.regex.Pattern;

import org.w3c.dom.Element;

import niagara.connection_server.Catalog;
import niagara.optimizer.colombia.*;
import niagara.utils.PEException;

/** Custom rules can be customized in a limited way from the catalog. */
abstract public class CustomRule extends Rule {
    private boolean specifiedPromise;
    private double promise;

    /* If this rule fires, disallow firing any rule whose name
     * appears in maskBefore on the "before" expression */
    protected String[] maskBeforeRuleNames;
    /* If this rule fires, disallow firing any rule whose name
     * appears in mask on the "after" expression */
    protected String[] maskRuleNames;

    protected CustomRule(String name, int arity, Expr pattern, Expr subst) {
        super(name, arity, pattern, subst);
    }

    public static CustomRule fromXML(Element e, Catalog catalog) {
        String name = e.getAttribute("name");
        if (name.length() == 0)
            catalog.confError("Each rule must have a name attribute");

        String className = e.getAttribute("class");
        if (className.length() == 0)
            catalog.confError("Custom rules must specify a class name");

        CustomRule rule = null;
        try {
            Class c = Class.forName(className);
            // Rules must have a public constructor with a single argument
            // (the rule name)
            Constructor ruleConstructor =
                c.getConstructor(new Class[] { String.class });
            rule =
                (CustomRule) ruleConstructor.newInstance(new Object[] { name });
        } catch (ClassNotFoundException cnfe) {
            catalog.confError("Could not load class: " + className);
        } catch (NoSuchMethodException nsme) {
            throw new PEException(
                "Constructor could not be found for: " + className);
        } catch (InvocationTargetException ite) {
            throw new PEException(
                "Constructor of "
                    + className
                    + " has thrown an exception: "
                    + ite.getTargetException());
        } catch (IllegalAccessException iae) {
            throw new PEException(
                "An illegal access exception occured: " + iae);
        } catch (InstantiationException ie) {
            throw new PEException("An instantiation exception occured: " + ie);
        }

        String promiseStr = e.getAttribute("promise");
        boolean specifiedPromise = (promiseStr.length() != 0);
        if (specifiedPromise)
            rule.setPromise(Double.parseDouble(promiseStr));

        String maskBefore[] = {
        };
        String mask[] = {
        };
        Pattern commaSeparated = Pattern.compile(",");
        String beforeMask = e.getAttribute("maskBefore");
        if (beforeMask.length() > 0)
            rule.setMaskBeforeRuleNames(commaSeparated.split(beforeMask));
        String afterMask = e.getAttribute("mask");
        if (afterMask.length() > 0)
            rule.setMaskRuleNames(commaSeparated.split(afterMask));

        return rule;
    }

    private void setPromise(double promise) {
        specifiedPromise = true;
        this.promise = promise;
    }

    public final void initialize() {
        if (maskBeforeRuleNames != null)
            for (int i = 0; i < maskBeforeRuleNames.length; i++)
                maskRuleBefore(maskBeforeRuleNames[i]);
        if (maskRuleNames != null)
            for (int i = 0; i < maskRuleNames.length; i++)
                maskRule(maskRuleNames[i]);
    }

    private void setMaskBeforeRuleNames(String[] maskBeforeRuleNames) {
        this.maskBeforeRuleNames = maskBeforeRuleNames;
    }

    private void setMaskRuleNames(String[] maskRuleNames) {
        this.maskRuleNames = maskRuleNames;
    }

    public final double promise(Op op_arg, Context ContextID) {
        if (specifiedPromise)
            return promise;
        return super.promise(op_arg, ContextID);
    }
}
