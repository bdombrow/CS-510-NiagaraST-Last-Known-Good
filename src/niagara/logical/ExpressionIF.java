package niagara.logical;

/**
 * Classes to be used with Expression must
 * implement the <code>ExpressionIF</code> interface.
 *
 * @version 1.0
 */

import niagara.query_engine.TupleSchema;
import niagara.utils.BaseAttr;
import niagara.utils.Tuple;

public interface ExpressionIF {
	BaseAttr processTuple(Tuple ste);

	void setupSchema(TupleSchema ts);
}
