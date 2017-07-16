package yanbinwa.iCache.service;

import org.springframework.beans.factory.InitializingBean;

import yanbinwa.common.exceptions.RedisErrorException;
import yanbinwa.common.exceptions.ServiceUnavailableException;
import yanbinwa.common.iInterface.ServiceLifeCycle;

public interface CacheService  extends InitializingBean, ServiceLifeCycle 
{
    public static final String SERVICE_IP = "ip";
    public static final String SERVICE_SERVICENAME = "serviceName";
    public static final String SERVICE_SERVICEGROUPNAME = "serviceGroupName";
    public static final String SERVICE_PORT = "port";
    public static final String SERVICE_ROOTURL = "rootUrl";
    public static final String SERVICE_TOPICINFO = "topicInfo";
    
    public static final String REDIS_SERVICE_GROUP_KEY = "redis";
    public static final String REDIS_INFO_NEED = "redisInfoNeed";
    
    public static final int REDIS_MAX_TOTAL_DEFAULT = 10;
    public static final int REDIS_MAX_IDEL_DEFAULT = 10;
    public static final long REDIS_MAX_WAIT_DEFAULT = -1;
    public static final boolean REDIS_TEST_ON_BORROW_DEFAULT = true;
    
    String getServiceName() throws ServiceUnavailableException;
    
    boolean isServiceReady() throws ServiceUnavailableException;
    
    String getServiceDependence() throws ServiceUnavailableException;

    void startManageService();

    void stopManageService();
    
    void setString(String key, String value) throws RedisErrorException;
    
    String getString(String key) throws RedisErrorException;
}
