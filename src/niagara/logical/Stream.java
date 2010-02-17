package niagara.logical;

import niagara.optimizer.colombia.Attribute;

abstract public class Stream extends NullaryOperator {
	protected StreamSpec streamSpec;
	protected Attribute variable;

	public StreamSpec getSpec() {
		return streamSpec;
	}

	public Attribute getVariable() {
		return variable;
	}

	public boolean isSourceOp() {
		return true;
	}
}