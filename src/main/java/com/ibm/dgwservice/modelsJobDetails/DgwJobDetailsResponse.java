package com.ibm.dgwservice.modelsJobDetails;

public class DgwJobDetailsResponse {
	BaseResponse BaseResponseObject;
	JobDetails JobDetailsObject;

	// Getter Methods

	public BaseResponse getBaseResponse() {
		return BaseResponseObject;
	}

	public JobDetails getJobDetails() {
		return JobDetailsObject;
	}

	// Setter Methods

	public void setBaseResponse(BaseResponse baseResponseObject) {
		this.BaseResponseObject = baseResponseObject;
	}

	public void setJobDetails(JobDetails jobDetailsObject) {
		this.JobDetailsObject = jobDetailsObject;
	}
}



