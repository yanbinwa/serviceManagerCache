package yanbinwa.iCache.service;

import org.springframework.beans.factory.InitializingBean;

import yanbinwa.common.iInterface.ServiceLifeCycle;
import yanbinwa.iCache.exception.ServiceUnavailableException;

public interface CacheService  extends InitializingBean, ServiceLifeCycle 
{
    
    public static final String SERVICE_IP = "ip";
    public static final String SERVICE_SERVICENAME = "serviceName";
    public static final String SERVICE_SERVICEGROUPNAME = "serviceGroupName";
    public static final String SERVICE_PORT = "port";
    public static final String SERVICE_ROOTURL = "rootUrl";
    public static final String SERVICE_TOPICINFO = "topicInfo";
    
    String getServiceName() throws ServiceUnavailableException;
    
    boolean isServiceReady() throws ServiceUnavailableException;
    
    String getServiceDependence() throws ServiceUnavailableException;

    void startManageService();

    void stopManageService();
}
