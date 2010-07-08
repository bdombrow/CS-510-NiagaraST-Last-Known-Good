/**
 * 
 */
package niagara.utils;

import niagara.query_engine.TupleSchema;

/**
 * @author rfernand
 * @version 1.0
 *
 */
public final class FeedbackPunctuation extends Punctuation {

	private FeedbackType _type;
	private TupleSchema _schema;

	public FeedbackType Type() {
		return _type;
	}
	
	public TupleSchema Schema() {
		return _schema;
	}
	
	public FeedbackPunctuation(FeedbackType type, TupleSchema schema, Tuple tuple) {
		super(false);
		this._type = type;
		this._schema = schema;
		this.tuple = tuple.getTuple();
	}

	public String toString() {
		String name = "[Feedback Punctuation].[Type = '" + _type.Type() + "'].[" + this.tuple.toString() + "]";
		return name;
	}

}
