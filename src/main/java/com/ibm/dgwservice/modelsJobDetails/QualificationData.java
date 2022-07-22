package com.ibm.dgwservice.modelsJobDetails;

public class QualificationData {
	 TN TNObject;
	 SvcAddress SvcAddressObject;
	 private String requestType;
	 private String network;


	 // Getter Methods 

	 public TN getTN() {
	  return TNObject;
	 }

	 public SvcAddress getSvcAddress() {
	  return SvcAddressObject;
	 }

	 public String getRequestType() {
	  return requestType;
	 }

	 public String getNetwork() {
	  return network;
	 }

	 // Setter Methods 

	 public void setTN(TN tNObject) {
	  this.TNObject = tNObject;
	 }

	 public void setSvcAddress(SvcAddress svcAddressObject) {
	  this.SvcAddressObject = svcAddressObject;
	 }

	 public void setRequestType(String requestType) {
	  this.requestType = requestType;
	 }

	 public void setNetwork(String network) {
	  this.network = network;
	 }
	}
