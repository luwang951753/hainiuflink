package com.zkdn.warehouse.dwd.warehousemodel;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-02-24-5:11 下午
 * @Description:
 */
public class AccessModel {
    private Long ip;
    private Long time;
    private String requestType;
    private String requestURL;
    private String status;
    private String refer;
    private String agent;
    private Map<String, String> requestAttr;
    private String userId;
    private String loginName;

    public AccessModel() {
    }

    public AccessModel(Long ip, Long time, String requestType, String requestURL, String status, String refer, String agent, Map<String, String> requestAttr, String userId, String loginName) {
        this.ip = ip;
        this.time = time;
        this.requestType = requestType;
        this.requestURL = requestURL;
        this.status = status;
        this.refer = refer;
        this.agent = agent;
        this.requestAttr = requestAttr;
        this.userId = userId;
        this.loginName = loginName;
    }

    public Long getIp() {
        return ip;
    }

    public void setIp(Long ip) {
        this.ip = ip;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public String getRequestType() {
        return requestType;
    }

    public void setRequestType(String requestType) {
        this.requestType = requestType;
    }

    public String getRequestURL() {
        return requestURL;
    }

    public void setRequestURL(String requestURL) {
        this.requestURL = requestURL;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getRefer() {
        return refer;
    }

    public void setRefer(String refer) {
        this.refer = refer;
    }

    public String getAgent() {
        return agent;
    }

    public void setAgent(String agent) {
        this.agent = agent;
    }

    public Map<String, String> getRequestAttr() {
        return requestAttr;
    }

    public void setRequestAttr(Map<String, String> requestAttr) {
        this.requestAttr = requestAttr;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getLoginName() {
        return loginName;
    }

    public void setLoginName(String loginName) {
        this.loginName = loginName;
    }

    @Override
    public String toString() {
        return "AccessModel{" +
                "ip=" + ip +
                ", time=" + time +
                ", requestType='" + requestType + '\'' +
                ", requestURL='" + requestURL + '\'' +
                ", status='" + status + '\'' +
                ", refer='" + refer + '\'' +
                ", agent='" + agent + '\'' +
                ", requestAttr=" + requestAttr +
                ", userId='" + userId + '\'' +
                ", loginName='" + loginName + '\'' +
                '}';
    }
}
