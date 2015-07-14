package v2;

import java.util.ArrayList;

public class DBMetadata {

	private String table;
	private ArrayList<Attribute> attributes;
	
	public DBMetadata(String table) {
		this.table = table;
		attributes = new ArrayList<Attribute>();
	}
	
	public void addAttribute(Attribute attr) {
		this.attributes.add(attr);
	}
	
	public ArrayList<Attribute> getAttributes() {
		return this.attributes;
	}

}
