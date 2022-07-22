package com.ibm.dgwservice.modelsJobDetails;

public class DispatchInfo {
	 private String dispatchType;
	 private float duration;
	 private float dispatchCorrelationId;
	 private String estimatedTimeOfArrival;


	 // Getter Methods 

	 public String getDispatchType() {
	  return dispatchType;
	 }

	 public float getDuration() {
	  return duration;
	 }

	 public float getDispatchCorrelationId() {
	  return dispatchCorrelationId;
	 }

	 public String getEstimatedTimeOfArrival() {
	  return estimatedTimeOfArrival;
	 }

	 // Setter Methods 

	 public void setDispatchType(String dispatchType) {
	  this.dispatchType = dispatchType;
	 }

	 public void setDuration(float duration) {
	  this.duration = duration;
	 }

	 public void setDispatchCorrelationId(float dispatchCorrelationId) {
	  this.dispatchCorrelationId = dispatchCorrelationId;
	 }

	 public void setEstimatedTimeOfArrival(String estimatedTimeOfArrival) {
	  this.estimatedTimeOfArrival = estimatedTimeOfArrival;
	 }
	}
