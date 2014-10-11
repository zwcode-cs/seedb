package common;

public class Attribute implements Comparable<Attribute>{
	public String name;
	public static enum AttributeType {ORDINAL, CARDINAL, NUMERIC, GEOGRAPHIC, 
		TIME_SERIES, NONE}; 			// not used right now
	public AttributeType type;			// not used right now
	public int numDistinctValues;
	
	public Attribute(String name) {
		this.name = name;
		this.type = AttributeType.NONE;
		this.numDistinctValues = 0;
	}
	
	public Attribute(String name, int numDistinct) {
		this.name = name;
		this.type = AttributeType.NONE;
		this.numDistinctValues = numDistinct;
	}
	
	public static Attribute selectAllAttribute() {
		return new Attribute("*");
	}

	public boolean equals(Object o) {
		if ((o == null) || (o.getClass() != this.getClass())) return false;
		Attribute at = (Attribute) o;
		return (at.name.equals(this.name) && (at.type == this.type) && 
				(at.numDistinctValues == this.numDistinctValues));
	}
	
	public String toString() {
		return name + ":" + type + "," + numDistinctValues;
	}

	@Override
	public int compareTo(Attribute a) {
		return this.name.compareTo(a.name);
	}
}
