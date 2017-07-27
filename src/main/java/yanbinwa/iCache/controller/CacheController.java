package yanbinwa.iCache.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import yanbinwa.common.exceptions.RedisErrorException;
import yanbinwa.common.exceptions.ServiceUnavailableException;
import yanbinwa.iCache.service.CacheService;

@RestController
@RequestMapping("/cache")
public class CacheController
{
    @Autowired
    CacheService cacheService;
    
    @RequestMapping(value="/getServiceName",method=RequestMethod.GET)
    public String getServiceName() throws ServiceUnavailableException
    {
        return cacheService.getServiceName();
    }
    
    @RequestMapping(value="/isServiceReady",method=RequestMethod.GET)
    public boolean isServiceReady() throws ServiceUnavailableException
    {
        return cacheService.isServiceReady();
    }
    
    @RequestMapping(value="/getServiceDependence",method=RequestMethod.GET)
    public String getServiceDependence() throws ServiceUnavailableException
    {
        return cacheService.getServiceDependence();
    }
    
    @RequestMapping(value="/startManageService",method=RequestMethod.POST)
    public void startManageService()
    {
        cacheService.startManageService();
    }
    
    @RequestMapping(value="/stopManageService",method=RequestMethod.POST)
    public void stopManageService()
    {
        cacheService.stopManageService();
    }
    
    @RequestMapping(value="/setString",method=RequestMethod.GET)
    void setString(@RequestParam("key") String key, @RequestParam("value") String value) throws RedisErrorException, ServiceUnavailableException
    {
        cacheService.setString(key, value);
    }
    
    @RequestMapping(value="/getString",method=RequestMethod.GET)
    String getString(@RequestParam("key") String key) throws RedisErrorException, ServiceUnavailableException
    {
        return cacheService.getString(key);
    }
    
    @ResponseStatus(value=HttpStatus.NOT_FOUND, reason="webService is stop")
    @ExceptionHandler(ServiceUnavailableException.class)
    public void serviceUnavailableExceptionHandler() 
    {
        
    }
    
    @ResponseStatus(value=HttpStatus.INTERNAL_SERVER_ERROR , reason="redis error")
    @ExceptionHandler(RedisErrorException.class)
    public void redisErrorExceptionHandler()
    {
        
    }
    
}
