package yanbinwa.iCache.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

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
    
    @ResponseStatus(value=HttpStatus.NOT_FOUND, reason="webService is stop")
    @ExceptionHandler(ServiceUnavailableException.class)
    public void serviceUnavailableExceptionHandler() 
    {
        
    }
    
}
