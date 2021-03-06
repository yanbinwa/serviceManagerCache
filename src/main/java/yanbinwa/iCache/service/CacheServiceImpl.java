package yanbinwa.iCache.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Service;

import yanbinwa.common.configClient.ConfigCallBack;
import yanbinwa.common.configClient.ConfigClient;
import yanbinwa.common.configClient.ConfigClientImpl;
import yanbinwa.common.configClient.ServiceConfigState;
import yanbinwa.common.exceptions.RedisErrorException;
import yanbinwa.common.exceptions.ServiceUnavailableException;
import yanbinwa.common.kafka.consumer.IKafkaConsumer;
import yanbinwa.common.kafka.producer.IKafkaProducer;
import yanbinwa.common.orchestrationClient.OrchestartionCallBack;
import yanbinwa.common.orchestrationClient.OrchestrationClient;
import yanbinwa.common.orchestrationClient.OrchestrationClientImpl;
import yanbinwa.common.orchestrationClient.OrchestrationServiceState;
import yanbinwa.common.redis.RedisClient;
import yanbinwa.common.zNodedata.ZNodeDataUtil;
import yanbinwa.common.zNodedata.ZNodeDependenceData;
import yanbinwa.common.zNodedata.ZNodeServiceData;
import yanbinwa.common.zNodedata.ZNodeServiceDataImpl;
import yanbinwa.common.zNodedata.decorate.ZNodeDecorateType;
import yanbinwa.common.zNodedata.decorate.ZNodeDependenceDataDecorateRedis;

@Service("cacheService")
@EnableAutoConfiguration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "serviceProperties")
public class CacheServiceImpl implements CacheService
{
    
    private static final Logger logger = Logger.getLogger(CacheServiceImpl.class);
    
    Map<String, String> serviceDataProperties;
    Map<String, String> zNodeInfoProperties;

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
    
    ZNodeServiceData serviceData = null;
    
    OrchestrationClient client = null;
    
    Map<String, IKafkaProducer> kafkaProducerMap = new HashMap<String, IKafkaProducer>();
    
    Map<String, IKafkaConsumer> kafkaConsumerMap = new HashMap<String, IKafkaConsumer>();
    
    boolean isRunning = false;
    
    boolean isConfiged = false;
    
    OrchestrationWatcher watcher = new OrchestrationWatcher();
    
    private ConfigClient configClient = null;
    
    ConfigCallBack configCallBack = new CacheConfigCallBack();
    
    private String zookeeperHostIp = null;
    
    // 记录了Redis与Partition的Mapping关系，其中hash值一律是通过key自带的hash来进行的
    Map<ZNodeServiceData, RedisClient> redisServiceDataToRedisClientMap = new HashMap<ZNodeServiceData, RedisClient>();
    Map<Integer, RedisClient> partitionKeyToRedisClientMap = new HashMap<Integer, RedisClient>();
    int partitionMask = -1;
    ReentrantLock lock = new ReentrantLock();
            
    @Override
    public void afterPropertiesSet() throws Exception
    {
        zookeeperHostIp = zNodeInfoProperties.get(OrchestrationClient.ZOOKEEPER_HOSTPORT_KEY);
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
        serviceData = new ZNodeServiceDataImpl(ip, serviceGroupName, serviceName, port, rootUrl);
        
        String redisInfoNeedStr = serviceDataProperties.get(CacheService.REDIS_INFO_NEED);
        if(redisInfoNeedStr != null && redisInfoNeedStr.trim().equals("true"))
        {
            serviceData.addServiceDataDecorate(ZNodeDecorateType.REDIS, true);
        }
        
        configClient = new ConfigClientImpl(serviceData, configCallBack, zookeeperHostIp, zNodeInfoProperties);
        client = new OrchestrationClientImpl(serviceData, watcher, zookeeperHostIp, zNodeInfoProperties);
        
        start();
    }
    
    @Override
    public void start()
    {
        if(!isRunning)
        {
            isRunning = true;
            configClient.start();
            logger.info("Start cache service ...");
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
            isRunning = false;
            configClient.stop();
            logger.info("Stop cache service ...");            
        }
        else
        {
            logger.info("Cache service has already stopped ...");
        }
    }

    @Override
    public String getServiceName() throws ServiceUnavailableException
    {
        if(!isServiceReadyToWork())
        {
            throw new ServiceUnavailableException();
        }
        return serviceData.getServiceName();
    }

    @Override
    public boolean isServiceReady() throws ServiceUnavailableException
    {
        if(!isServiceReadyToWork())
        {
            throw new ServiceUnavailableException();
        }
        return client.isReady();
    }

    @Override
    public String getServiceDependence() throws ServiceUnavailableException
    {
        if(!isServiceReadyToWork())
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
    
    @Override
    public void setString(String key, String value) throws RedisErrorException, ServiceUnavailableException
    {
        if(!isServiceReadyToWork())
        {
            throw new ServiceUnavailableException();
        }
        RedisClient redisClient = getRedisClientFromKey(key);
        if (redisClient == null)
        {
            throw new RedisErrorException();
        }
        try
        {
            boolean ret = redisClient.getJedisConnection();
            if (!ret)
            {
                logger.error("fail to get the redis connection");
                throw new RedisErrorException();
            }
            redisClient.setString(key, value);
        }
        catch (InterruptedException e)
        {
            logger.error("get redis connection timeout");
            e.printStackTrace();
        }
        finally
        {
            redisClient.returnJedisConnection();
        }
    }

    @Override
    public String getString(String key) throws RedisErrorException, ServiceUnavailableException
    {
        if(!isServiceReadyToWork())
        {
            throw new ServiceUnavailableException();
        }
        RedisClient redisClient = getRedisClientFromKey(key);
        if (redisClient == null)
        {
            throw new RedisErrorException();
        }
        String value = null;
        try
        {
            boolean ret = redisClient.getJedisConnection();
            if (!ret)
            {
                logger.error("fail to get the redis connection");
                throw new RedisErrorException();
            }
            value = redisClient.getString(key);
        }
        catch (InterruptedException e)
        {
            logger.error("get redis connection timeout");
            e.printStackTrace();
        }
        finally
        {
            redisClient.returnJedisConnection();
        }
        return value;
    }
    
    @Override
    public void startWork()
    {
        logger.info("Start work cache service ...");
        init();
        client.start();
    }

    @Override
    public void stopWork()
    {
        logger.info("Stop work cache service ...");
        client.stop();
        reset();
    }
    
    private void init()
    {
        
    }
    
    private void reset()
    {
        
    }
    
    private RedisClient getRedisClientFromKey(String key)
    {
        if (key == null)
        {
            return null;
        }
        int partitionKey =  key.hashCode() % partitionMask;
        return partitionKeyToRedisClientMap.get(partitionKey);
    }
    
    // 这里还要对RedisClient进行创建和删除操作
    private void buildOrUpdateRedisPartitionInfo()
    {
        ZNodeDependenceData depData = client.getDepData();
        if (depData == null)
        {
            logger.error("buildOrUpdateRedisPartitionInfo the depData Should not be null for Cache service");
            return;
        }
        if (!depData.isContainedDecorate(ZNodeDecorateType.REDIS))
        {
            logger.error("depDate should contain redis decorate " + depData);
            return;
        }
        ZNodeDependenceDataDecorateRedis decorate = (ZNodeDependenceDataDecorateRedis)depData.getDependenceDataDecorate(ZNodeDecorateType.REDIS);
        Map<String, Set<Integer>> redisServiceNameToPartitionKeyMap = decorate.getRedisServiceNameToPartitionMap();
        if (redisServiceNameToPartitionKeyMap == null)
        {
            logger.error("redisServiceNameToPartitionKeyMap should not be empty");
            return;
        }
        Set<ZNodeServiceData> redisServiceData = depData.getDependenceData().get(REDIS_SERVICE_GROUP_KEY);
        if (redisServiceData == null)
        {
            logger.error("Cache service dependence should contain the redis service data");
            return;
        }
        lock.lock();
        try
        {
            Set<ZNodeServiceData> currentRedisServiceData = redisServiceDataToRedisClientMap.keySet();
            List<ZNodeServiceData> addRedisServiceData = new ArrayList<ZNodeServiceData>();
            for (ZNodeServiceData data : redisServiceData)
            {
                if (!currentRedisServiceData.contains(data))
                {
                    addRedisServiceData.add(data);
                }
            }
            List<ZNodeServiceData> delRedisServiceData = new ArrayList<ZNodeServiceData>();
            for (ZNodeServiceData data : currentRedisServiceData)
            {
                if (!redisServiceData.contains(data))
                {
                    delRedisServiceData.add(data);
                }
            }
            if (addRedisServiceData.size() > 0 || delRedisServiceData.size() > 0)
            {
                updateRedisClient(addRedisServiceData, delRedisServiceData);
            }
            updatePartitionKeyToRedisClientMap(redisServiceNameToPartitionKeyMap);
        }
        finally
        {
            lock.unlock();
        }
    }
    
    private void updateRedisClient(List<ZNodeServiceData> addRedisServiceData, List<ZNodeServiceData> delRedisServiceData)
    {
        lock.lock();
        try
        {
            Map<ZNodeServiceData, RedisClient> redisServiceDataToRedisClientMapTmp = new HashMap<ZNodeServiceData, RedisClient>(redisServiceDataToRedisClientMap);
            for (ZNodeServiceData data : delRedisServiceData)
            {
                RedisClient client = redisServiceDataToRedisClientMapTmp.remove(data);
                if (client == null)
                {
                    logger.error("Redis client should not be empty " + data);
                    continue;
                }
                client.closePool();
            }
            for (ZNodeServiceData data : addRedisServiceData)
            {
                if (redisServiceDataToRedisClientMapTmp.containsKey(data))
                {
                    logger.error("partitionKeyToRedisClientMapTmp should not contain " + data);
                    continue;
                }
                RedisClient client = new RedisClient(data.getIp(), data.getPort(), REDIS_MAX_TOTAL_DEFAULT,
                        REDIS_MAX_IDEL_DEFAULT, REDIS_MAX_WAIT_DEFAULT, REDIS_TEST_ON_BORROW_DEFAULT);
                redisServiceDataToRedisClientMapTmp.put(data, client);
            }
            redisServiceDataToRedisClientMap = redisServiceDataToRedisClientMapTmp;
        }
        finally
        {
            lock.unlock();
        }
    }
    
    private void updatePartitionKeyToRedisClientMap(Map<String, Set<Integer>> redisServiceNameToPartitionKeyMap)
    {
        lock.lock();
        try
        {
            Map<Integer, RedisClient> partitionKeyToRedisClientMapTmp = new HashMap<Integer, RedisClient>();
            Set<ZNodeServiceData> currentRedisServiceData = redisServiceDataToRedisClientMap.keySet();
            for (String redisServiceName : redisServiceNameToPartitionKeyMap.keySet())
            {
                for (ZNodeServiceData data : currentRedisServiceData)
                {
                    if (data.getServiceName().equals(redisServiceName))
                    {
                        Set<Integer> partitionKeys = redisServiceNameToPartitionKeyMap.get(redisServiceName);
                        RedisClient client = redisServiceDataToRedisClientMap.get(data);
                        for (Integer partitionKey : partitionKeys)
                        {
                            partitionKeyToRedisClientMapTmp.put(partitionKey, client);
                        }
                        break;
                    }
                }
            }
            partitionKeyToRedisClientMap = partitionKeyToRedisClientMapTmp;
        }
        finally
        {
            lock.unlock();
        }
    }
    
    private void clearRedisPartitionInfo()
    {
        lock.lock();
        try
        {
            //不能通过partitionKeyToRedisClientMap来去做，应该用redisServiceDataToRedisClientMap
            for(RedisClient redisClient : redisServiceDataToRedisClientMap.values())
            {
                redisClient.closePool();
            }
            partitionKeyToRedisClientMap.clear();
            redisServiceDataToRedisClientMap.clear();
            partitionMask = -1;
        }
        finally
        {
            lock.unlock();
        }
    }
    
    private void updateServiceConfigProperties(JSONObject serviceConfigPropertiesObj)
    {
        if (!isConfiged)
        {
            logger.info("Update the serviceProperties for Cache");
            startWork();
        }
        isConfiged = true;
    }
    
    private boolean isServiceReadyToWork()
    {
        return isRunning && isConfiged;
    }
    
    /**
     * 通过dependenceData来构建redisClientMap
     * 
     * @author yanbinwa
     *
     */
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
                curState = state;
                buildOrUpdateRedisPartitionInfo();
            }
            else if(state == OrchestrationServiceState.NOTREADY && curState == OrchestrationServiceState.READY)
            {
                logger.info("The service is stopped");
                curState = state;
                clearRedisPartitionInfo();
            }
            else if(state == OrchestrationServiceState.DEPCHANGE)
            {
                logger.info("The dependence is changed");
                buildOrUpdateRedisPartitionInfo();
            }
        }
    }
    
    class CacheConfigCallBack implements ConfigCallBack
    {

        @Override
        public void handleServiceConfigChange(ServiceConfigState state)
        {
            logger.info("Service config state is: " + state);
            if (state == ServiceConfigState.CREATED || state == ServiceConfigState.CHANGED)
            {
                JSONObject serviceConfigPropertiesObj = configClient.getServiceConfigProperties();
                if (ZNodeDataUtil.validateServiceConfigProperties(serviceData, serviceConfigPropertiesObj))
                {
                    updateServiceConfigProperties(serviceConfigPropertiesObj);
                }
                else
                {
                    logger.error("Un valid service config properties: " + serviceConfigPropertiesObj);
                }
            }
            else if (state == ServiceConfigState.DELETED || state == ServiceConfigState.CLOSE)
            {
                if (isConfiged)
                {
                    stopWork();
                }
                isConfiged = false;
            }
            else
            {
                logger.error("Unknow ServiceConfigState: " + state);
            }
        }
    }
}
