package niagara.optimizer.colombia;

/**
 * A tracer object handles tracing events from the optimizer
 */
public interface Tracer {
    /** About to start optimization */
    void startingOptimization();
    
    /** Optimization just finished */
    void endingOptimization();
    
    /** About to create a new group with this multiexpression */
    void beforeNewGroup(MExpr mexpr);
    
    /** A new group was created */
    void afterNewGroup(Group group);

    /** A new multiexpression was created */
    void newMExpr(MExpr mexpr);

    /** A new winner was found for a group */
    void newWinner(Group g, Winner w);
        
    /** A multiexpression was added to a group */
    void addedMExprToGroup(MExpr mexpr);
    
    /** A new task is scheduled */
    void addingTask(Task task);
    
    /** A task is about to be performed*/
    void performingTask(Task task);
    
    /** Application of a rule was masked for a multiexpression */
    void ruleMasked(Rule rule, MExpr mexpr);
    
    /** A duplicate multiexpression was found */
    void duplicateMExprFound(MExpr mexpr);
}
