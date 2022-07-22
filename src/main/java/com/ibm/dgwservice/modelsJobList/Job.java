package com.ibm.dgwservice.modelsJobList;

public class Job {
	private String correlationId;
	private String status;

	public String getCorrelationId() {
		return correlationId;
	}

	public void setCorrelationId(String correlationId) {
		this.correlationId = correlationId;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	@Override
	public String toString() {
		return "Job [correlationId=" + correlationId + ", status=" + status + "]";
	}
	
	

}
