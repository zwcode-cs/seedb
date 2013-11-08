package utils;

public class DistributionUnit {
	public double fraction; // fraction of distribution with the associated value
	public Object attributeValue; // specific value of attribute such as month of year (string), zipcode (int)
	
	public DistributionUnit(Object attributeValue, double fraction) {
		this.fraction = fraction;
		this.attributeValue = attributeValue;
	}
	
	public boolean equals(Object o) {
		if ((o != null) && (o instanceof DistributionUnit)) {
			DistributionUnit unit = (DistributionUnit) o;
			return (Math.abs(unit.fraction - this.fraction) < 1E-5) && (this.attributeValue.equals(unit.attributeValue));
		}
		return false;
	}
	
	public String toString() {
		return attributeValue + ": " + fraction;
	}
}
