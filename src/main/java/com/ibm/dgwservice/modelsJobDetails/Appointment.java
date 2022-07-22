package com.ibm.dgwservice.modelsJobDetails;

public class Appointment {
	 private String appointmentStartDateTime;
	 private String appointmentEndDateTime;
	 private String dueDate;
	 private String accessAfterDateTime;
	 private String accessBeforeDateTime;
	 private String techName;
	 private String appointmentOrderId;
	 private String inquiryReservationNumber;
	 private String appointmentEarlyStartTime;


	 // Getter Methods 

	 public String getAppointmentStartDateTime() {
	  return appointmentStartDateTime;
	 }

	 public String getAppointmentEndDateTime() {
	  return appointmentEndDateTime;
	 }

	 public String getDueDate() {
	  return dueDate;
	 }

	 public String getAccessAfterDateTime() {
	  return accessAfterDateTime;
	 }

	 public String getAccessBeforeDateTime() {
	  return accessBeforeDateTime;
	 }

	 public String getTechName() {
	  return techName;
	 }

	 public String getAppointmentOrderId() {
	  return appointmentOrderId;
	 }

	 public String getInquiryReservationNumber() {
	  return inquiryReservationNumber;
	 }

	 public String getAppointmentEarlyStartTime() {
	  return appointmentEarlyStartTime;
	 }

	 // Setter Methods 

	 public void setAppointmentStartDateTime(String appointmentStartDateTime) {
	  this.appointmentStartDateTime = appointmentStartDateTime;
	 }

	 public void setAppointmentEndDateTime(String appointmentEndDateTime) {
	  this.appointmentEndDateTime = appointmentEndDateTime;
	 }

	 public void setDueDate(String dueDate) {
	  this.dueDate = dueDate;
	 }

	 public void setAccessAfterDateTime(String accessAfterDateTime) {
	  this.accessAfterDateTime = accessAfterDateTime;
	 }

	 public void setAccessBeforeDateTime(String accessBeforeDateTime) {
	  this.accessBeforeDateTime = accessBeforeDateTime;
	 }

	 public void setTechName(String techName) {
	  this.techName = techName;
	 }

	 public void setAppointmentOrderId(String appointmentOrderId) {
	  this.appointmentOrderId = appointmentOrderId;
	 }

	 public void setInquiryReservationNumber(String inquiryReservationNumber) {
	  this.inquiryReservationNumber = inquiryReservationNumber;
	 }

	 public void setAppointmentEarlyStartTime(String appointmentEarlyStartTime) {
	  this.appointmentEarlyStartTime = appointmentEarlyStartTime;
	 }
	}
