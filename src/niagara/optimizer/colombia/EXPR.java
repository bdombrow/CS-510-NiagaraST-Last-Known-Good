package niagara.optimizer.colombia;

/**
 * Expressions 
 * 
 * An Expr corresponds to a detailed solution to the original query or
 * a subquery.  An Expr is modeled as an operator with arguments
 * (class Op), plus input expressions (class Expr). EXPRs are used to
 * calculate the initial query and the final plan, and are also used
 * in rules.
 */
public class Expr {
    Op op; //Operator
    Expr[] inputs; //Input expressions
    int arity; //Number of input expressions.

    public Expr(Op op) {
        this.op = op;
        inputs = new Expr[0];
        arity = 0;
    }

    public Expr(Op op, Expr e) {
        this.op = op;
        inputs = new Expr[1];
        inputs[0] = e;
        arity = 1;
    }

    public Expr(Op op, Expr e1, Expr e2) {
        this.op = op;
        inputs = new Expr[2];
        inputs[0] = e1;
        inputs[1] = e2;
        arity = 2;
    }

    public Expr(Op op, Expr[] inputs) {
        this.op = op;
        arity = inputs.length;

        this.inputs = new Expr[arity];
        for (int i = 0; i < arity; i++) {
            this.inputs[i] = inputs[i];
        }
    }

    public Expr(Expr e) {
        op = e.op.copy();
        arity = e.getArity();
        inputs = new Expr[arity];
        for (int i = 0; i < arity; i++)
            inputs[i] = new Expr((e.getInput(i)));
    }

    public Op getOp() {
        return op;
    }
    public int getArity() {
        return arity;
    }
    public Expr getInput(int i) {
        return inputs[i];
    }
    public void setInput(int i, Expr e) {
        inputs[i] = e;
    }

    /** Return the number of Leaf operators in this
     * expression, viewed as a pattern */
    public int numLeafOps() {
        if (op.is_leaf())
            return 1;

        int count = 0;
        for (int i = 0; i < inputs.length; i++)
            count += inputs[i].numLeafOps();
        return count;
    }

    //Caching the computed cost for this physical expression
    double cachedCost = -1;

    /** the cost of a physical plan with this expression as root */
    public Cost getCost(ICatalog catalog) {
        if (cachedCost != -1)
            return new Cost(cachedCost);
        Cost cost;
        LogicalProperty localLogProp = ((PhysicalOp) getOp()).getLogProp();
        LogicalProperty[] inputLogProps = new LogicalProperty[arity];
        for (int i = 0; i < arity; i++) {
            inputLogProps[i] = ((PhysicalOp) getInput(i).getOp()).getLogProp();
        }
        cost = ((PhysicalOp) getOp()).findLocalCost(catalog, inputLogProps);

        for (int i = 0; i < arity; i++) {
            cost.add(getInput(i).getCost(catalog));
        }

        cachedCost = cost.getValue();
        return cost;
    }

    // // Return the string representing the expression
    // // And meanwhile, output it with indentation
    // String Dump(int ntabs=0)
    // {
    // 	String os;
    // 	String temp;

    // 	for(int i=0; i<ntabs; i++) os += "\t";
    // 	os += (*Op).Dump();	

    // 	//OUTPUTN(ntabs, os); 

    //         if ((int) cachedCost == -1) { // XXX Vassilis: weak!
    //             cachedCost = GetCost().getValue(); 
    //         }

    // 	temp.Format(", cost=%f\n", cachedCost); 
    // 	os += temp;
    // 	for(int i=0; i<arity; i++)
    // 	{
    // 	//	OUTPUTN(ntabs, "\r\n"); 
    // // 		for(int j=0; j<ntabs;j++) os += "\t"; // XXX 
    // // 		os +="\n";                            // XXX
    // 		os += Inputs[i].Dump(ntabs+1);
    // 	}

    // 	// temp.Format("%d ", (int)CostVal); // XXX Vassilis: why do 
    //                                              // XXX we need this?
    // 	// os += temp;                       // XXX
    // 	return os;
    // }
}
