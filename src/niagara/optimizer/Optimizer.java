/**
 * $Id: Optimizer.java,v 1.9 2002/12/10 01:18:27 vpapad Exp $
 */
package niagara.optimizer;

import niagara.xmlql_parser.op_tree.logNode;
import niagara.connection_server.ConfigurationError;
import niagara.connection_server.NiagraServer;
import niagara.connection_server.Catalog;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Stack;

import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import niagara.optimizer.colombia.*;
import niagara.query_engine.PhysicalOperator;
import niagara.query_engine.SchedulablePlan;

public class Optimizer implements Tracer {
    private RuleSet consolidationRules;
    private RuleSet rules;

    private SSP consolidationSSP;
    private SSP ssp;
    Catalog catalog;

    public Optimizer() {
        catalog = NiagraServer.getCatalog();

        consolidationRules =
            new RuleSet(catalog.getRules("consolidation ruleset"), true);
        consolidationSSP = new SSP(consolidationRules, catalog);
        consolidationSSP.setTracer(this);

        rules = new RuleSet(catalog.getRules("normal ruleset"), false);
        ssp = new SSP(rules, catalog);
        ssp.setTracer(this);
    }

    private void parseRules(ArrayList rules) {
        Catalog catalog = NiagraServer.getCatalog();

    }

    public Plan consolidate(Expr expr) {
        consolidationSSP.optimize(expr);
        Plan consPlan =
            Plan.getPlan(
                consolidationSSP.extractLastLogicalExpression(consolidationSSP.getGroup(0)),
                catalog);
        consolidationSSP.clear();
        return consPlan;
    }

    public Plan optimize(Expr expr) {
        ssp.optimize(expr);
        Plan optPlan =
            Plan.getPlan(
                ssp.CopyOut(ssp.getGroup(0), PhysicalProperty.ANY, new HashMap()),
                catalog);
        ssp.clear();
        return removeNoOps(optPlan);
    }

    private Plan removeNoOps(Plan optPlan) {
        Plan p = optPlan;
        if (p.getOperator() instanceof PhysicalNoOp)
            return removeNoOps((Plan) p.getInput(0));
        for (int i = 0; i  < p.getArity(); i++)
            p.setInput(i, removeNoOps((Plan) optPlan.getInput(i)));
            
        return p;
    }
    
    // Empty implementations of tracing methods 
    
    /**
     * @see niagara.optimizer.colombia.Tracer#addedMExprToGroup(MExpr)
     */
    public void addedMExprToGroup(MExpr mexpr) {
    }

    /**
     * @see niagara.optimizer.colombia.Tracer#addingTask(Task)
     */
    public void addingTask(Task task) {
    }

    /**
     * @see niagara.optimizer.colombia.Tracer#afterNewGroup(Group)
     */
    public void afterNewGroup(Group group) {
    }

    /**
     * @see niagara.optimizer.colombia.Tracer#beforeNewGroup(MExpr)
     */
    public void beforeNewGroup(MExpr mexpr) {
    }

    /**
     * @see niagara.optimizer.colombia.Tracer#duplicateMExprFound(MExpr)
     */
    public void duplicateMExprFound(MExpr mexpr) {
    }

    /**
     * @see niagara.optimizer.colombia.Tracer#endingOptimization()
     */
    public void endingOptimization() {
    }

    /**
     * @see niagara.optimizer.colombia.Tracer#newMExpr(MExpr)
     */
    public void newMExpr(MExpr mexpr) {
    }

    /**
     * @see niagara.optimizer.colombia.Tracer#newWinner(Group, Winner)
     */
    public void newWinner(Group g, Winner w) {
    }

    /**
     * @see niagara.optimizer.colombia.Tracer#performingTask(Task)
     */
    public void performingTask(Task task) {
    }

    /**
     * @see niagara.optimizer.colombia.Tracer#ruleMasked(Rule, MExpr)
     */
    public void ruleMasked(Rule rule, MExpr mexpr) {
    }

    /**
     * @see niagara.optimizer.colombia.Tracer#startingOptimization()
     */
    public void startingOptimization() {
    }

}
