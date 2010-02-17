package niagara.optimizer.rules;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import niagara.connection_server.Catalog;
import niagara.connection_server.ConfigurationError;
import niagara.optimizer.colombia.Context;
import niagara.optimizer.colombia.Expr;
import niagara.optimizer.colombia.LeafOp;
import niagara.optimizer.colombia.LogicalOp;
import niagara.optimizer.colombia.MExpr;
import niagara.optimizer.colombia.Op;
import niagara.optimizer.colombia.PhysicalProperty;
import niagara.utils.PEException;
import niagara.utils.XMLUtils;

import org.w3c.dom.Element;

/**
 * A simple rule which maps (exactly) one logical operator to one initializable
 * operator.
 */
@SuppressWarnings("unchecked")
public class SimpleRule extends ParsedRule {
	private SimpleRule(String name, int arity, Expr before, Expr after,
			String[] maskBeforeRuleNames, String[] maskRuleNames) {
		super(name, arity, before, after, maskBeforeRuleNames, maskRuleNames);
	}

	/* Create a SimpleRule from its description in XML */
	public static SimpleRule fromXML(Element e, Catalog catalog) {
		String name = e.getAttribute("name");
		if (name.length() == 0)
			Catalog.confError("Each rule must have a name attribute");

		String promiseStr = e.getAttribute("promise");
		boolean specifiedPromise = (promiseStr.length() != 0);
		final double promise;
		if (specifiedPromise)
			promise = Double.parseDouble(promiseStr);
		else
			promise = 0;

		String maskBefore[] = parseMask(e, "maskBefore");
		String mask[] = parseMask(e, "mask");
		ConfigurationError checkPatterns = new ConfigurationError(
				"Rules must contain one 'before' and one 'after' pattern");

		Element beforeOp = XMLUtils
				.getOnlyElementChild(
						XMLUtils.getFirstElementChild(e, "before",
								checkPatterns),
						"logical",
						new ConfigurationError(
								"Before pattern must consist of exactly one logical operator"));
		Element afterOp = XMLUtils.getOnlyElementChild(XMLUtils
				.getFirstElementChild(e, "after", checkPatterns), "op",
				new ConfigurationError(
						"After pattern must consist of exactly one operator"));

		// Construct before pattern
		String logicalOpName = beforeOp.getAttribute("name");
		Class logicalClass = catalog.getOperatorClass(logicalOpName);
		if (logicalClass == null)
			Catalog.confError("Could not find logical operator with name: "
					+ logicalOpName);

		Expr beforeExpr = null;
		LogicalOp logicalOp = null;
		int arity = 0;

		try {
			// Logical operator must have a zero-argument public constructor
			Constructor logicalConstructor = logicalClass
					.getConstructor(new Class[] {});

			logicalOp = (LogicalOp) logicalConstructor
					.newInstance(new Object[] {});

			arity = logicalOp.getArity();
			beforeExpr = new Expr(logicalOp, new Expr[arity]);
			for (int i = 0; i < arity; i++) {
				beforeExpr.setInput(i, new Expr(new LeafOp(i)));
			}
		} catch (NoSuchMethodException nsme) {
			throw new PEException("Constructor could not be found for: "
					+ logicalOpName);
		} catch (InvocationTargetException ite) {
			throw new PEException("Constructor of " + logicalClass
					+ " has thrown an exception: " + ite.getTargetException());
		} catch (IllegalAccessException iae) {
			throw new PEException("An illegal access exception occured: " + iae);
		} catch (InstantiationException ie) {
			throw new PEException("An instantiation exception occured: " + ie);
		}

		String afterOpName = afterOp.getAttribute("name");
		Class afterClass = catalog.getOperatorClass(afterOpName);
		if (afterClass == null)
			Catalog.confError("Could not find operator with name: "
					+ afterOpName);

		Expr afterExpr = null;
		try {
			// After operator must have a zero-argument public constructor
			Constructor afterConstructor = afterClass
					.getConstructor(new Class[] {});
			Op afterOperator = (Op) afterConstructor
					.newInstance(new Object[] {});
			afterExpr = new Expr(afterOperator, new Expr[arity]);
			for (int i = 0; i < arity; i++) {
				afterExpr.setInput(i, new Expr(new LeafOp(i)));
			}
		} catch (NoSuchMethodException nsme) {
			throw new PEException("Constructor could not be found for: "
					+ afterOpName);
		} catch (InvocationTargetException ite) {
			throw new PEException("Constructor of " + afterClass
					+ " has thrown an exception: " + ite.getTargetException());
		} catch (IllegalAccessException iae) {
			throw new PEException("An illegal access exception occured: " + iae);
		} catch (InstantiationException ie) {
			throw new PEException("An instantiation exception occured: " + ie);
		}

		SimpleRule simpleRule;

		if (specifiedPromise)
			simpleRule = new SimpleRule(name, arity, beforeExpr, afterExpr,
					maskBefore, mask) {
				public double promise(Op op_arg, Context ContextID) {
					return promise;
				}
			};
		else
			simpleRule = new SimpleRule(name, arity, beforeExpr, afterExpr,
					maskBefore, mask);

		simpleRule.parseCondition(beforeOp, logicalClass, catalog);
		return simpleRule;
	}

	/**
	 * @see niagara.optimizer.colombia.Rule#next_substitute(Expr, MExpr,
	 *      PhysicalProperty)
	 */
	public Expr nextSubstitute(Expr before, MExpr mExpr,
			PhysicalProperty ReqdProp) {
		Op op = (Op) substitute.getOp().copy();
		((Initializable) op).initFrom((LogicalOp) before.getOp());

		Expr result = new Expr(op, new Expr[arity]);
		for (int i = 0; i < arity; i++) {
			result.setInput(i, before.getInput(i));
		}
		return result;
	}
}
