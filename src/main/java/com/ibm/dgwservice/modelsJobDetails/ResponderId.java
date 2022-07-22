package com.ibm.dgwservice.modelsJobDetails;

public class ResponderId {
	 private String value;
	 private String serviceName;
	 private String hostName;
	 private String serverName;


	 // Getter Methods 

	 public String getValue() {
	  return value;
	 }

	 public String getServiceName() {
	  return serviceName;
	 }

	 public String getHostName() {
	  return hostName;
	 }

	 public String getServerName() {
	  return serverName;
	 }

	 // Setter Methods 

	 public void setValue(String value) {
	  this.value = value;
	 }

	 public void setServiceName(String serviceName) {
	  this.serviceName = serviceName;
	 }

	 public void setHostName(String hostName) {
	  this.hostName = hostName;
	 }

	 public void setServerName(String serverName) {
	  this.serverName = serverName;
	 }
	}
