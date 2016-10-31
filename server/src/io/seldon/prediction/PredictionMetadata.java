package io.seldon.prediction;

public class PredictionMetadata {

	String puid;
	String modelName;
	String variation;

	
	public PredictionMetadata()
	{
		
	}


	public PredictionMetadata(String puid, String modelName, String variation) {
		super();
		this.puid = puid;
		this.modelName = modelName;
		this.variation = variation;
	}


	public String getPuid() {
		return puid;
	}


	public void setPuid(String puid) {
		this.puid = puid;
	}


	public String getModelName() {
		return modelName;
	}


	public void setModelName(String modelName) {
		this.modelName = modelName;
	}


	public String getVariation() {
		return variation;
	}


	public void setVariation(String variation) {
		this.variation = variation;
	}
	

	
	
}
