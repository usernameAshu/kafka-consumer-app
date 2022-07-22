package com.ibm.dgwservice.modelsJobDetails;

import java.util.ArrayList;

public class JobDetails {
	private float priority;
	private String status;
	private String openDate;
	private String id;
	private String alternateTrackingNumber;
	private String hostTrackingNumber;
	private String businessUnit;
	private  DispatchInfo DispatchInfoObject;
	private Appointment AppointmentObject;
	private CustomerInfo CustomerInfoObject;
	private Location LocationObject;
	private String productType;
	private String orderType;
	private String installType;
	private ArrayList<TroubleType> repair = new ArrayList<>();
	private ArrayList<RemarkDetails> remark = new ArrayList<>();
	private String wireCenter;
	private String downstreamRate;
	private GetQualificationResponse GetQualificationResponseObject;
	private String taskType;
	private String taskTypeDescription;
	private String alternateCorrelationId;
	private String techId;
	private String onhold;
	private LmosDetails LmosDetailsObject;
	private String missedCommitment;
	private String selfAssignFlag;
	private IwmpDetails IwmpDetailsObject;
	private String tCompanyCode;
	private String wireCenter8;
	private String isJobOnly;
	private String serviceAppointmentNumber;
	private String billingSystem;
	private String dispatchSystem;
	private String originatingSystem;
	private String serviceApptId;
	private String workTypeName;
	private String workOrderId;
	private String inJeopardy;
	private String troubleType;
	private ArrayList<JobsStatusHistoryDetails> jobsStatusHistory = new ArrayList<>();
	private Facilities FacilitiesObject;

	// Getter Methods

	public float getPriority() {
		return priority;
	}

	public String getStatus() {
		return status;
	}

	public String getOpenDate() {
		return openDate;
	}

	public String getId() {
		return id;
	}

	public String getAlternateTrackingNumber() {
		return alternateTrackingNumber;
	}

	public String getHostTrackingNumber() {
		return hostTrackingNumber;
	}

	public String getBusinessUnit() {
		return businessUnit;
	}

	public DispatchInfo getDispatchInfo() {
		return DispatchInfoObject;
	}

	public Appointment getAppointment() {
		return AppointmentObject;
	}

	public CustomerInfo getCustomerInfo() {
		return CustomerInfoObject;
	}

	public Location getLocation() {
		return LocationObject;
	}

	public String getProductType() {
		return productType;
	}

	public String getOrderType() {
		return orderType;
	}

	public String getInstallType() {
		return installType;
	}

	public String getWireCenter() {
		return wireCenter;
	}

	public String getDownstreamRate() {
		return downstreamRate;
	}

	public GetQualificationResponse getGetQualificationResponse() {
		return GetQualificationResponseObject;
	}

	public String getTaskType() {
		return taskType;
	}

	public String getTaskTypeDescription() {
		return taskTypeDescription;
	}

	public String getAlternateCorrelationId() {
		return alternateCorrelationId;
	}

	public String getTechId() {
		return techId;
	}

	public String getOnhold() {
		return onhold;
	}

	public LmosDetails getLmosDetails() {
		return LmosDetailsObject;
	}

	public String getMissedCommitment() {
		return missedCommitment;
	}

	public String getSelfAssignFlag() {
		return selfAssignFlag;
	}

	public IwmpDetails getIwmpDetails() {
		return IwmpDetailsObject;
	}

	public String getTCompanyCode() {
		return tCompanyCode;
	}

	public String getWireCenter8() {
		return wireCenter8;
	}

	public String getIsJobOnly() {
		return isJobOnly;
	}

	public String getServiceAppointmentNumber() {
		return serviceAppointmentNumber;
	}

	public String getBillingSystem() {
		return billingSystem;
	}

	public String getDispatchSystem() {
		return dispatchSystem;
	}

	public String getOriginatingSystem() {
		return originatingSystem;
	}

	public String getServiceApptId() {
		return serviceApptId;
	}

	public String getWorkTypeName() {
		return workTypeName;
	}

	public String getWorkOrderId() {
		return workOrderId;
	}

	public String getInJeopardy() {
		return inJeopardy;
	}

	public String getTroubleType() {
		return troubleType;
	}

	public Facilities getFacilities() {
		return FacilitiesObject;
	}

	// Setter Methods

	public void setPriority(float priority) {
		this.priority = priority;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public void setOpenDate(String openDate) {
		this.openDate = openDate;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setAlternateTrackingNumber(String alternateTrackingNumber) {
		this.alternateTrackingNumber = alternateTrackingNumber;
	}

	public void setHostTrackingNumber(String hostTrackingNumber) {
		this.hostTrackingNumber = hostTrackingNumber;
	}

	public void setBusinessUnit(String businessUnit) {
		this.businessUnit = businessUnit;
	}

	public void setDispatchInfo(DispatchInfo dispatchInfoObject) {
		this.DispatchInfoObject = dispatchInfoObject;
	}

	public void setAppointment(Appointment appointmentObject) {
		this.AppointmentObject = appointmentObject;
	}

	public void setCustomerInfo(CustomerInfo customerInfoObject) {
		this.CustomerInfoObject = customerInfoObject;
	}

	public void setLocation(Location locationObject) {
		this.LocationObject = locationObject;
	}

	public void setProductType(String productType) {
		this.productType = productType;
	}

	public void setOrderType(String orderType) {
		this.orderType = orderType;
	}

	public void setInstallType(String installType) {
		this.installType = installType;
	}

	public void setWireCenter(String wireCenter) {
		this.wireCenter = wireCenter;
	}

	public void setDownstreamRate(String downstreamRate) {
		this.downstreamRate = downstreamRate;
	}

	public void setGetQualificationResponse(GetQualificationResponse getQualificationResponseObject) {
		this.GetQualificationResponseObject = getQualificationResponseObject;
	}

	public void setTaskType(String taskType) {
		this.taskType = taskType;
	}

	public void setTaskTypeDescription(String taskTypeDescription) {
		this.taskTypeDescription = taskTypeDescription;
	}

	public void setAlternateCorrelationId(String alternateCorrelationId) {
		this.alternateCorrelationId = alternateCorrelationId;
	}

	public void setTechId(String techId) {
		this.techId = techId;
	}

	public void setOnhold(String onhold) {
		this.onhold = onhold;
	}

	public void setLmosDetails(LmosDetails lmosDetailsObject) {
		this.LmosDetailsObject = lmosDetailsObject;
	}

	public void setMissedCommitment(String missedCommitment) {
		this.missedCommitment = missedCommitment;
	}

	public void setSelfAssignFlag(String selfAssignFlag) {
		this.selfAssignFlag = selfAssignFlag;
	}

	public void setIwmpDetails(IwmpDetails iwmpDetailsObject) {
		this.IwmpDetailsObject = iwmpDetailsObject;
	}

	public void setTCompanyCode(String tCompanyCode) {
		this.tCompanyCode = tCompanyCode;
	}

	public void setWireCenter8(String wireCenter8) {
		this.wireCenter8 = wireCenter8;
	}

	public void setIsJobOnly(String isJobOnly) {
		this.isJobOnly = isJobOnly;
	}

	public void setServiceAppointmentNumber(String serviceAppointmentNumber) {
		this.serviceAppointmentNumber = serviceAppointmentNumber;
	}

	public void setBillingSystem(String billingSystem) {
		this.billingSystem = billingSystem;
	}

	public void setDispatchSystem(String dispatchSystem) {
		this.dispatchSystem = dispatchSystem;
	}

	public void setOriginatingSystem(String originatingSystem) {
		this.originatingSystem = originatingSystem;
	}

	public void setServiceApptId(String serviceApptId) {
		this.serviceApptId = serviceApptId;
	}

	public void setWorkTypeName(String workTypeName) {
		this.workTypeName = workTypeName;
	}

	public void setWorkOrderId(String workOrderId) {
		this.workOrderId = workOrderId;
	}

	public void setInJeopardy(String inJeopardy) {
		this.inJeopardy = inJeopardy;
	}

	public void setTroubleType(String troubleType) {
		this.troubleType = troubleType;
	}

	public void setFacilities(Facilities facilitiesObject) {
		this.FacilitiesObject = facilitiesObject;
	}

	@Override
	public String toString() {
		return "JobDetails [priority=" + priority + ", status=" + status + ", openDate=" + openDate + ", id=" + id
				+ ", alternateTrackingNumber=" + alternateTrackingNumber + ", hostTrackingNumber=" + hostTrackingNumber
				+ ", businessUnit=" + businessUnit + ", DispatchInfoObject=" + DispatchInfoObject
				+ ", AppointmentObject=" + AppointmentObject + ", CustomerInfoObject=" + CustomerInfoObject
				+ ", LocationObject=" + LocationObject + ", productType=" + productType + ", orderType=" + orderType
				+ ", installType=" + installType + ", repair=" + repair + ", remark=" + remark + ", wireCenter="
				+ wireCenter + ", downstreamRate=" + downstreamRate + ", GetQualificationResponseObject="
				+ GetQualificationResponseObject + ", taskType=" + taskType + ", taskTypeDescription="
				+ taskTypeDescription + ", alternateCorrelationId=" + alternateCorrelationId + ", techId=" + techId
				+ ", onhold=" + onhold + ", LmosDetailsObject=" + LmosDetailsObject + ", missedCommitment="
				+ missedCommitment + ", selfAssignFlag=" + selfAssignFlag + ", IwmpDetailsObject=" + IwmpDetailsObject
				+ ", tCompanyCode=" + tCompanyCode + ", wireCenter8=" + wireCenter8 + ", isJobOnly=" + isJobOnly
				+ ", serviceAppointmentNumber=" + serviceAppointmentNumber + ", billingSystem=" + billingSystem
				+ ", dispatchSystem=" + dispatchSystem + ", originatingSystem=" + originatingSystem + ", serviceApptId="
				+ serviceApptId + ", workTypeName=" + workTypeName + ", workOrderId=" + workOrderId + ", inJeopardy="
				+ inJeopardy + ", troubleType=" + troubleType + ", jobsStatusHistory=" + jobsStatusHistory
				+ ", FacilitiesObject=" + FacilitiesObject + "]";
	}
	
	
}
