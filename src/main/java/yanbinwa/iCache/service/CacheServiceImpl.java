package yanbinwa.iCache.service;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Service;

import yanbinwa.common.kafka.consumer.IKafkaCallBack;
import yanbinwa.common.kafka.consumer.IKafkaConsumer;
import yanbinwa.common.kafka.consumer.IKafkaConsumerImpl;
import yanbinwa.common.kafka.message.KafkaMessage;
import yanbinwa.common.orchestrationClient.OrchestartionCallBack;
import yanbinwa.common.orchestrationClient.OrchestrationClient;
import yanbinwa.common.orchestrationClient.OrchestrationClientImpl;
import yanbinwa.common.orchestrationClient.OrchestrationServiceState;
import yanbinwa.common.zNodedata.ZNodeData;
import yanbinwa.common.zNodedata.ZNodeServiceDataWithKafkaTopic;
import yanbinwa.iCache.exception.ServiceUnavailableException;

@Service("cacheService")
@EnableAutoConfiguration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "serviceProperties")
public class CacheServiceImpl implements CacheService
{
    
    private static final Logger logger = Logger.getLogger(CacheServiceImpl.class);
    
    Map<String, String> serviceDataProperties;
    Map<String, String> zNodeInfoProperties;
    Map<String, Object> kafkaConsumerProperties;

    public void setServiceDataProperties(Map<String, String> properties)
    {
        this.serviceDataProperties = properties;
    }
    
    public Map<String, String> getServiceDataProperties()
    {
        return this.serviceDataProperties;
    }
    
    public void setZNodeInfoProperties(Map<String, String> properties)
    {
        this.zNodeInfoProperties = properties;
    }
    
    public Map<String, String> getZNodeInfoProperties()
    {
        return this.zNodeInfoProperties;
    }
    
    public void setKafkaConsumerProperties(Map<String, Object> properties)
    {
        this.kafkaConsumerProperties = properties;
    }
    
    public Map<String, Object> getKafkaConsumerProperties()
    {
        return this.kafkaConsumerProperties;
    }
    
    ZNodeData serviceData = null;
    
    OrchestrationClient client = null;
    
    Map<String, IKafkaConsumer> kafkaConsumerMap = new HashMap<String, IKafkaConsumer>();
    
    boolean isRunning = false;
    
    OrchestrationWatcher watcher = new OrchestrationWatcher();
    
    IKafkaCallBack callback = new IKafkaCallBackImpl();
    
    @Override
    public void afterPropertiesSet() throws Exception
    {
        String zookeeperHostIp = zNodeInfoProperties.get(OrchestrationClient.ZOOKEEPER_HOSTPORT_KEY);
        if(zookeeperHostIp == null)
        {
            logger.error("Zookeeper host and port should not be null");
            return;
        }
        String serviceGroupName = serviceDataProperties.get(CacheService.SERVICE_SERVICEGROUPNAME);
        String serviceName = serviceDataProperties.get(CacheService.SERVICE_SERVICENAME);
        String ip = serviceDataProperties.get(CacheService.SERVICE_IP);
        String portStr = serviceDataProperties.get(CacheService.SERVICE_PORT);
        int port = Integer.parseInt(portStr);
        String rootUrl = serviceDataProperties.get(CacheService.SERVICE_ROOTURL);
        String topicInfo = serviceDataProperties.get(CacheService.SERVICE_TOPICINFO);
        serviceData = new ZNodeServiceDataWithKafkaTopic(ip, serviceGroupName, serviceName, port, rootUrl, topicInfo);
        
        client = new OrchestrationClientImpl(serviceData, watcher, zookeeperHostIp, zNodeInfoProperties);
        createKafkaConsumerMap(kafkaConsumerProperties);
        
        start();
    }

    private void createKafkaConsumerMap(Map<String, Object> kafkaConsumerProperties)
    {
        if (kafkaConsumerProperties == null)
        {
            logger.error("kafka properties shoud not be null");
            return;
        }
        for(Map.Entry<String, Object> entry : kafkaConsumerProperties.entrySet())
        {
            if(entry.getValue() instanceof Map)
            {
                @SuppressWarnings("unchecked")
                Map<String, String> kafkaProperty = (Map<String, String>)entry.getValue();
                String consumerTopic = entry.getKey();
                IKafkaConsumer kafkaConsumer = new IKafkaConsumerImpl(kafkaProperty, consumerTopic, callback);
                kafkaConsumerMap.put(consumerTopic, kafkaConsumer);
            }
            else
            {
                logger.error("kafka property shoud not be null " + entry.getKey());
                continue;
            }
        }
    }
    
    @Override
    public void start()
    {
        if(!isRunning)
        {
            logger.info("Start cache service ...");
            client.start();
            isRunning = true;
        }
        else
        {
            logger.info("Cache service has already started ...");
        }
    }

    @Override
    public void stop()
    {
        if(isRunning)
        {
            logger.info("Stop cache service ...");
            client.stop();
            isRunning = false;
        }
        else
        {
            logger.info("Cache service has already stopped ...");
        }
    }

    @Override
    public String getServiceName() throws ServiceUnavailableException
    {
        if(!isRunning)
        {
            throw new ServiceUnavailableException();
        }
        return serviceData.getServiceName();
    }

    @Override
    public boolean isServiceReady() throws ServiceUnavailableException
    {
        if(!isRunning)
        {
            throw new ServiceUnavailableException();
        }
        return client.isReady();
    }

    @Override
    public String getServiceDependence() throws ServiceUnavailableException
    {
        if(!isRunning)
        {
            throw new ServiceUnavailableException();
        }
        return client.getDepData().toString();
    }

    @Override
    public void startManageService()
    {
        if(!isRunning)
        {
            start();
        }
    }

    @Override
    public void stopManageService()
    {
        if(isRunning)
        {
            stop();
        }
    }

    class IKafkaCallBackImpl implements IKafkaCallBack
    {

        @Override
        public void handleOnData(KafkaMessage msg)
        {
            logger.info("Get kafka message: " + msg);
        }
        
    }
    
    class OrchestrationWatcher implements OrchestartionCallBack
    {

        OrchestrationServiceState curState = OrchestrationServiceState.NOTREADY;
        
        @Override
        public void handleServiceStateChange(OrchestrationServiceState state)
        {
            logger.info("Service state is: " + state);
            //由Unready -> ready
            if (state == OrchestrationServiceState.READY && curState == OrchestrationServiceState.NOTREADY)
            {
                logger.info("The service is started");
                for(Map.Entry<String, IKafkaConsumer> entry : kafkaConsumerMap.entrySet())
                {
                    entry.getValue().start();
                }
                curState = state;
            }
            else if(state == OrchestrationServiceState.NOTREADY && curState == OrchestrationServiceState.READY)
            {
                logger.info("The service is stopped");
                for(Map.Entry<String, IKafkaConsumer> entry : kafkaConsumerMap.entrySet())
                {
                    entry.getValue().stop();
                }
                curState = state;
            }
            else if(state == OrchestrationServiceState.DEPCHANGE)
            {
                logger.info("The dependence is changed");
            }
        }
    }
}