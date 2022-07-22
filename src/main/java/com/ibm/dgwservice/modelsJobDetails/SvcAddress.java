package com.ibm.dgwservice.modelsJobDetails;

public class SvcAddress {
	 private String streetNumber;
	 private String streetName;
	 private String city;
	 private String stateProvince;


	 // Getter Methods 

	 public String getStreetNumber() {
	  return streetNumber;
	 }

	 public String getStreetName() {
	  return streetName;
	 }

	 public String getCity() {
	  return city;
	 }

	 public String getStateProvince() {
	  return stateProvince;
	 }

	 // Setter Methods 

	 public void setStreetNumber(String streetNumber) {
	  this.streetNumber = streetNumber;
	 }

	 public void setStreetName(String streetName) {
	  this.streetName = streetName;
	 }

	 public void setCity(String city) {
	  this.city = city;
	 }

	 public void setStateProvince(String stateProvince) {
	  this.stateProvince = stateProvince;
	 }
	}
