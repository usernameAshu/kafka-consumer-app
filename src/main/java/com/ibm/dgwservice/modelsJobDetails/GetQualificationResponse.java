package com.ibm.dgwservice.modelsJobDetails;

import java.util.ArrayList;

public class GetQualificationResponse {
	 ARTISResponseHeader ARTISResponseHeaderObject;
	 QualificationData QualificationDataObject;
	 private String overallStatusCode;
	 ArrayList < Object > qualProductCategory = new ArrayList < Object > ();


	 // Getter Methods 

	 public ARTISResponseHeader getARTISResponseHeader() {
	  return ARTISResponseHeaderObject;
	 }

	 public QualificationData getQualificationData() {
	  return QualificationDataObject;
	 }

	 public String getOverallStatusCode() {
	  return overallStatusCode;
	 }

	 // Setter Methods 

	 public void setARTISResponseHeader(ARTISResponseHeader aRTISResponseHeaderObject) {
	  this.ARTISResponseHeaderObject = aRTISResponseHeaderObject;
	 }

	 public void setQualificationData(QualificationData qualificationDataObject) {
	  this.QualificationDataObject = qualificationDataObject;
	 }

	 public void setOverallStatusCode(String overallStatusCode) {
	  this.overallStatusCode = overallStatusCode;
	 }
	}
