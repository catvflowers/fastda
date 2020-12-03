package com.sm.fastda.util;


public class RightHelp {

	private static final ThreadLocal<RightHelp> currentContext =new ThreadLocal<RightHelp>();
	
	private Integer authType;
	private String code;
	private Integer valueType;
	private boolean isAdmin;
	private String sessionId;
	
	
	
	/*private String areaCode;
	
	private String sessionId;
	private String deptCode;
	private String companyCode;
	private Integer userId;
	
	private String likeStr;*/
	
	/*public String getDeptCode() {
		return deptCode;
	}

	public void setDeptCode(String deptCode) {
		this.deptCode = deptCode;
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public String getAreaCode() {
		return areaCode;
	}

	public void setAreaCode(String ownerCode) {
		this.areaCode = ownerCode;
		this.likeStr=genLike(ownerCode);
	}
	
	public String getCompanyCode() {
		return companyCode;
	}

	public void setCompanyCode(String companyCode) {
		this.companyCode = companyCode;
	}
	
	public Integer getUserId() {
		return userId;
	}

	public void setUserId(Integer userId) {
		this.userId = userId;
	}

	public String getSQL() {
		return this.likeStr;
	}
	*/

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

	public Integer getAuthType() {
		return authType;
	}

	public void setAuthType(Integer authType) {
		this.authType = authType;
	}

	public Integer getValueType() {
		return valueType;
	}

	public void setValueType(Integer valueType) {
		this.valueType = valueType;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public void setAdmin(boolean isAdmin) {
		this.isAdmin = isAdmin;
	}

	public boolean isAdmin() {
		return this.isAdmin;
	}
	// compatible old 
	public String getAreaCode() {
		return this.code;
	}

	public static boolean hasRight() {
		RightHelp context=currentContext.get();
		if(context==null) return false;
		else {
			return true;
		}
	}
	
	public static RightHelp getValue() {
		RightHelp context=currentContext.get();
		return context;
	}
	public static void setValue(RightHelp val) {
		currentContext.set(val);
	}
	
	/*private static String genLike(String areacodes) {
		if(areacodes==null||areacodes.length()==0) return null;
		String[] temp=areacodes.split(",");
		StringBuffer sb=new StringBuffer();
		for(int i=0;i<temp.length;i++) {
			sb.append("area_code like '");
			sb.append(temp[i]);
			sb.append("%' or");
		}
		sb.delete(sb.length()-3, sb.length());
		if(temp.length>1) {
			sb.insert(0,"(");
			sb.append(")");
		}
		return sb.toString();
	}*/
}
