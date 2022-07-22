package com.ibm.dgwservice.modelsJobDetails;

public class CustomerInfo {
	 private String customerName;
	 private String maintenancePlan;
	 private String serviceClass;
	 private String circuitId;
	 private String customerType;
	 private String marketUnit;
	 private String billingSystem;
	 private String primaryContactNumber;


	 // Getter Methods 

	 public String getCustomerName() {
	  return customerName;
	 }

	 public String getMaintenancePlan() {
	  return maintenancePlan;
	 }

	 public String getServiceClass() {
	  return serviceClass;
	 }

	 public String getCircuitId() {
	  return circuitId;
	 }

	 public String getCustomerType() {
	  return customerType;
	 }

	 public String getMarketUnit() {
	  return marketUnit;
	 }

	 public String getBillingSystem() {
	  return billingSystem;
	 }

	 public String getPrimaryContactNumber() {
	  return primaryContactNumber;
	 }

	 // Setter Methods 

	 public void setCustomerName(String customerName) {
	  this.customerName = customerName;
	 }

	 public void setMaintenancePlan(String maintenancePlan) {
	  this.maintenancePlan = maintenancePlan;
	 }

	 public void setServiceClass(String serviceClass) {
	  this.serviceClass = serviceClass;
	 }

	 public void setCircuitId(String circuitId) {
	  this.circuitId = circuitId;
	 }

	 public void setCustomerType(String customerType) {
	  this.customerType = customerType;
	 }

	 public void setMarketUnit(String marketUnit) {
	  this.marketUnit = marketUnit;
	 }

	 public void setBillingSystem(String billingSystem) {
	  this.billingSystem = billingSystem;
	 }

	 public void setPrimaryContactNumber(String primaryContactNumber) {
	  this.primaryContactNumber = primaryContactNumber;
	 }
	}
