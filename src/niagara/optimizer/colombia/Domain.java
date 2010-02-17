package niagara.optimizer.colombia;

/** Domains/Types */
abstract public class Domain {
	protected String name;

	public Domain(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public boolean equals(Object obj) {
		// KT - is this correct?
		if (obj == null || !(obj instanceof Domain))
			return false;
		// can't handle inheritance in this abstract class
		if (obj.getClass() != this.getClass())
			return false;
		return name.equals(((Domain) obj).name);
	}
}
