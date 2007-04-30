package niagara.optimizer;

public interface PlanVisitor {
    /** Visit a plan node
     * @return false if the visits should continue */
    boolean visit(Plan p);
}
