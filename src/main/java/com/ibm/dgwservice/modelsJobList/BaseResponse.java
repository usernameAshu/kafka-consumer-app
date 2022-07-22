package com.ibm.dgwservice.modelsJobList;

public class BaseResponse {
	 private String message;
	 private String responseStatus;
	 private float reasonCode;


	 // Getter Methods 

	 public String getMessage() {
	  return message;
	 }

	 public String getResponseStatus() {
	  return responseStatus;
	 }

	 public float getReasonCode() {
	  return reasonCode;
	 }

	 // Setter Methods 

	 public void setMessage(String message) {
	  this.message = message;
	 }

	 public void setResponseStatus(String responseStatus) {
	  this.responseStatus = responseStatus;
	 }

	 public void setReasonCode(float reasonCode) {
	  this.reasonCode = reasonCode;
	 }
	}