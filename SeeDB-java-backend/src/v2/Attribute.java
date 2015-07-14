package v2;

public class Attribute {
	public static enum AttributeType { DIMENSION, MEASURE, OTHER, UNKNOWN }; 
	public static int maxDistinct = 10;
	
	// attribute types translated from postgres
	public String name;
	public AttributeType type;
	public int numDistinct;
	
	// TODO: add way to specify dimension and measure from table metadata
	
	public Attribute(String name, AttributeType type, int numDistinct) {
		this.name = name;
		this.type = type;
		this.numDistinct = numDistinct;
	}

	public boolean isDimension() {
		return (type == AttributeType.DIMENSION);
	}
	
	public boolean isMeasure() {
		return (type == AttributeType.MEASURE);
	}
	
	public String toString() {
		return name + ";" + type.toString() + ";" + numDistinct;
	}
}