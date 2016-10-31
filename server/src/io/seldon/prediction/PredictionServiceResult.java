package io.seldon.prediction;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;

public class PredictionServiceResult {

	public PredictionMetadata meta;
	public List<PredictionResult> predictions;
	public JsonNode custom;
	
	public PredictionServiceResult()
	{
		
	}

	public PredictionServiceResult(PredictionMetadata meta, List<PredictionResult> predictions, JsonNode custom) {
		super();
		this.meta = meta;
		this.predictions = predictions;
		this.custom = custom;
	}

	public PredictionMetadata getMeta() {
		return meta;
	}

	public void setMeta(PredictionMetadata meta) {
		this.meta = meta;
	}

	public List<PredictionResult> getPredictions() {
		return predictions;
	}

	public void setPredictions(List<PredictionResult> predictions) {
		this.predictions = predictions;
	}

	public JsonNode getCustom() {
		return custom;
	}

	public void setCustom(JsonNode custom) {
		this.custom = custom;
	}
	
	

	
	
}
