/* $Id: ConstructedRule.java,v 1.2 2003/08/01 17:29:06 tufte Exp $ */
package niagara.optimizer.rules;

import java.util.HashMap;

import org.w3c.dom.Element;

import niagara.connection_server.Catalog;
import niagara.connection_server.ConfigurationError;
import niagara.optimizer.colombia.Expr;
import niagara.optimizer.colombia.MExpr;
import niagara.optimizer.colombia.PhysicalProperty;
import niagara.utils.XMLUtils;

/**
 * Constructed rules are rules constructed at run time from an XML 
 * specification (but can have more complex before and after patterns
 * than <code>SimpleRule</code> allows). 
 */
public class ConstructedRule extends ParsedRule {
    Antecedent before;
    Consequent after;

    Class conditionArgClass;
    byte[] conditionArgAddress;

    private ConstructedRule(
        String name,
        Antecedent before,
        Consequent after,
        String conditionArgument,
        String[] maskBefore,
        String[] mask) {
        super(
            name,
            before.toExpr().numLeafOps(),
            before.toExpr(),
            after.toExpr(),
            maskBefore,
            mask);
        this.before = before;
        this.after = after;
        HashMap addresses = new HashMap();
        before.getAddresses(new byte[0], addresses);
        after.resolveAddresses(addresses);

        this.conditionArgClass =
            before.findByName(conditionArgument).getOperator().getClass();
        this.conditionArgAddress = (byte[]) addresses.get(conditionArgument);
    }

    /** Parse a rule from a DOM element */
    public static ConstructedRule fromXML(Element e, Catalog catalog) {
        String tagName = e.getTagName();
        if (!tagName.equals("rule"))
            Catalog.confError("Expected rule, found " + tagName);

        String name = e.getAttribute("name");
        if (name.length() == 0)
            Catalog.confError("Each rule must have a name attribute");

        ConfigurationError checkPatterns =
            new ConfigurationError("Rules must contain one 'before' and one 'after' pattern");

        Element beforeRoot =
            XMLUtils.getFirstElementChild(
                XMLUtils.getFirstElementChild(e, "before", checkPatterns),
                "op",
                new ConfigurationError(
                    "The root of the antecedent pattern in "
                        + name
                        + " must be an op element"));
        Element afterRoot =
            XMLUtils.getFirstElementChild(
                XMLUtils.getFirstElementChild(e, "after", checkPatterns),
                "op",
                new ConfigurationError(
                    "The root of the consequent pattern in "
                        + name
                        + " must be an op element"));

        Antecedent before = Antecedent.fromXML(beforeRoot, catalog);
        Consequent after = Consequent.fromXML(afterRoot, catalog);

        ConstructedRule constructedRule =
            new ConstructedRule(
                name,
                before,
                after,
                e.getAttribute("argument"),
                parseMask(e, "maskBefore"),
                parseMask(e, "mask"));
        constructedRule.parseCondition(
            e,
            before.getOperator().getClass(),
            catalog);

        return constructedRule;
    }

    public Expr next_substitute(
        Expr before,
        MExpr mExpr,
        PhysicalProperty ReqdProp) {
        // Constructed rules can only use the before Expression
        return after.constructSubstitute(before);
    }

    protected Object[] getConditionArgs(Expr before) {
        if (conditionArgAddress == null)
            return emptyConditionArgs;
        else
            return new Object[] {
                 this.before.followAddress(conditionArgAddress, before)};
    }

    protected Class[] getConditionArgTypes() {
        if (conditionArgClass == null)
            return emptyConditionArgTypes;
        else
            return new Class[] { conditionArgClass };
    }
}
