package com.ibm.dgwservice.modelsJobList;

import java.util.ArrayList;
import java.util.List;

public class DgwJobListResponse {
	private BaseResponse baseResponse;
	private List<Job> jobList = new ArrayList<>();

	public BaseResponse getBaseResponse() {
		return baseResponse;
	}

	public void setBaseResponse(BaseResponse baseResponse) {
		this.baseResponse = baseResponse;
	}

	public List<Job> getJobList() {
		return jobList;
	}

	public void setJobList(List<Job> jobList) {
		this.jobList = jobList;
	}

	@Override
	public String toString() {
		return "DgwJobListResponse [baseResponse=" + baseResponse + ", jobList=" + jobList + "]";
	}
	
	

}
