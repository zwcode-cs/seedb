package core_v1;

import java.util.ArrayList;

import core.Metadata;

public class AllTableMetadata {
	private ArrayList<Metadata> metadataList;
	private ArrayList<String> allAttributes;
	private ArrayList<String> measureAttributes;
	private ArrayList<String> dimensionAttributes;
	
	public AllTableMetadata() {
		this.metadataList = new ArrayList<Metadata>();
		this.allAttributes = new ArrayList<String>();
		this.measureAttributes = new ArrayList<String>();
		this.dimensionAttributes = new ArrayList<String>();
	}
	
	public void addMetadata(Metadata metadata) {
		this.metadataList.add(metadata);
		this.allAttributes.addAll(metadata.getAllAttributes());
		this.dimensionAttributes.addAll(metadata.getDimensionAttributes());
		this.measureAttributes.addAll(metadata.getMeasureAttributes());
	}
	
	public ArrayList<String> getAllAttributes() {
		return allAttributes;
	}
	
	public ArrayList<String> getDimensionAttributes() {
		return dimensionAttributes;
	}
	
	public ArrayList<String> getMeasureAttributes() {
		return measureAttributes;
	}

}
