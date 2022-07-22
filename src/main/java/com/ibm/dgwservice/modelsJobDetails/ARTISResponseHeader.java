package com.ibm.dgwservice.modelsJobDetails;

public class ARTISResponseHeader {
	 private String aRTISCorrelationId;
	 ARTISInfoObject ARTISInfoObjectObject;
	 ResponderId ResponderIdObject;


	 // Getter Methods 

	 public String getARTISCorrelationId() {
	  return aRTISCorrelationId;
	 }

	 public ARTISInfoObject getARTISInfoObject() {
	  return ARTISInfoObjectObject;
	 }

	 public ResponderId getResponderId() {
	  return ResponderIdObject;
	 }

	 // Setter Methods 

	 public void setARTISCorrelationId(String aRTISCorrelationId) {
	  this.aRTISCorrelationId = aRTISCorrelationId;
	 }

	 public void setARTISInfoObject(ARTISInfoObject aRTISInfoObjectObject) {
	  this.ARTISInfoObjectObject = aRTISInfoObjectObject;
	 }

	 public void setResponderId(ResponderId responderIdObject) {
	  this.ResponderIdObject = responderIdObject;
	 }
	}
