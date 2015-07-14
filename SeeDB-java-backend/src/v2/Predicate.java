package v2;

public class Predicate {
	public enum ComparisonOperator {LT, EQ, GT, LE, GE, NEQ};
	
	public Attribute attribute;
	public String value; // TODO: must be parsed
	public ComparisonOperator op;
}
