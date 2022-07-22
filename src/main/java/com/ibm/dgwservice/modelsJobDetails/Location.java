package com.ibm.dgwservice.modelsJobDetails;

public class Location {
	 private String city;
	 private String state;
	 private String zipCode;
	 private String district;
	 private String country;
	 private String addressLine1;
	 GeoLocation GeoLocationObject;
	 private String region;
	 private String area;


	 // Getter Methods 

	 public String getCity() {
	  return city;
	 }

	 public String getState() {
	  return state;
	 }

	 public String getZipCode() {
	  return zipCode;
	 }

	 public String getDistrict() {
	  return district;
	 }

	 public String getCountry() {
	  return country;
	 }

	 public String getAddressLine1() {
	  return addressLine1;
	 }

	 public GeoLocation getGeoLocation() {
	  return GeoLocationObject;
	 }

	 public String getRegion() {
	  return region;
	 }

	 public String getArea() {
	  return area;
	 }

	 // Setter Methods 

	 public void setCity(String city) {
	  this.city = city;
	 }

	 public void setState(String state) {
	  this.state = state;
	 }

	 public void setZipCode(String zipCode) {
	  this.zipCode = zipCode;
	 }

	 public void setDistrict(String district) {
	  this.district = district;
	 }

	 public void setCountry(String country) {
	  this.country = country;
	 }

	 public void setAddressLine1(String addressLine1) {
	  this.addressLine1 = addressLine1;
	 }

	 public void setGeoLocation(GeoLocation geoLocationObject) {
	  this.GeoLocationObject = geoLocationObject;
	 }

	 public void setRegion(String region) {
	  this.region = region;
	 }

	 public void setArea(String area) {
	  this.area = area;
	 }
	}
