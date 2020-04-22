package epl.pubsub.location.tester;

import epl.pubsub.location.indexperf.Index;
import epl.pubsub.location.indexperf.IndexFactory;

import epl.pubsub.location.pulsarclient.PulsarLocationClient;
import epl.pubsub.location.pulsarclient.PulsarLocationClientBuilder;
import epl.pubsub.location.pulsarclient.PulsarLocationProducer;
import epl.pubsub.location.pulsarclient.PulsarLocationConsumer;
import epl.pubsub.location.pulsarclient.SubscriptionChangedCallback;
import epl.pubsub.location.LocationManager;
import epl.pubsub.location.LocationManagerImpl;
import epl.pubsub.location.LocationSubscriptionHandler;
import epl.pubsub.location.LocationSubscriptionHandlerSingleTopicImpl;
import epl.pubsub.location.LocationSubscriptionHandlerMultiTopicImpl;
import epl.pubsub.location.LocationChangedCallback;
import epl.pubsub.location.pulsarclient.ConsumerMetrics;
import epl.pubsub.location.pulsarclient.ProducerMetrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;

import java.util.Properties;
import java.util.List;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.time.StopWatch; 

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class TestRunner {
    private Config config;
    private StringBuilder payload = new StringBuilder();
    private Index index;
    private IndexConfig indexConfig;
 
    ExecutorService consumerExecutor;
    ExecutorService producerExecutor;
    ExecutorService locationManagerExecutor;

    private static final Logger log = LoggerFactory.getLogger(TestRunner.class);

    public TestRunner(Config config){
        this.config = config;
        this.indexConfig = config.indexConfig;
        try{
            BufferedReader br = new BufferedReader(new FileReader(config.payloadFile)); 
  
            String st; 
            while ((st = br.readLine()) != null){
                payload.append(st);
            } 
        } catch(IOException ex){
            ex.printStackTrace();
        }       
        log.info("read payload successfully");
        Properties props = new Properties();
        if(indexConfig.indexType.equals("GEOHASH")){
            index = IndexFactory.getInitializedIndex(indexConfig.minX, indexConfig.minY, indexConfig.maxX, indexConfig.maxY, indexConfig.blockSize, IndexFactory.IndexType.GEOHASH, props); 
        }
        else {
            index = IndexFactory.getInitializedIndex(indexConfig.minX, indexConfig.minY, indexConfig.maxX, indexConfig.maxY, indexConfig.blockSize, IndexFactory.IndexType.RTREE, props); 
 
        }
        log.info("created index");
    
        consumerExecutor = Executors.newFixedThreadPool(config.numConsumers);
        producerExecutor = Executors.newFixedThreadPool(config.numProducers);
        locationManagerExecutor = Executors.newFixedThreadPool(config.numProducers + config.numConsumers);

        
    }
    
    public List<PulsarLocationProducer> getProducers(PulsarLocationClient client, int numProducers){
        List<PulsarLocationProducer> producers = new ArrayList<>();
        for(int i = 0; i < numProducers; ++i){
            producers.add(client.getNewProducer());
        }
        return producers;
    }

    public List<PulsarLocationConsumer> getConsumers(PulsarLocationClient client, int numConsumers){
        List<PulsarLocationConsumer> consumers = new ArrayList<>();
        for(int i = 0; i < numConsumers; ++i){
            consumers.add(client.getNewConsumer());
        }
        return consumers;
    }

    public List<ConsumerTask> getConsumerTasks(List<PulsarLocationConsumer> consumers){
        List<ConsumerTask> consumerTasks = new ArrayList<>();
        List<String> topics = new ArrayList<>();
        String topicStr = "test";
        topics.add(topicStr);
        for(int i = 0; i < consumers.size(); ++i){
            ConsumerTask consumerTask = new ConsumerTask(consumers.get(i), topics, topicStr + "_sub_" + i);
            consumerTask.start();
            consumerExecutor.execute(consumerTask);
            consumerTasks.add(consumerTask);
        }
        return consumerTasks;
    }    

    public List<ProducerTask> getProducerTasks(List<PulsarLocationProducer> producers){
        List<ProducerTask> producerTasks = new ArrayList<>();
        String topic = "test";
        for(int i = 0; i < producers.size(); ++i){
            ProducerTask producerTask = new ProducerTask(producers.get(i), payload.toString(), 1, topic);
            producerTask.start();
            producerExecutor.execute(producerTask);
            producerTasks.add(producerTask);
        }
        return producerTasks;
    }    

    public List<LocationSubscriptionHandlerSingleTopicImpl> getProducerLocationHandlers(List<PulsarLocationProducer> producers, Index index){
        List<LocationSubscriptionHandlerSingleTopicImpl> handlers = new ArrayList<>();
        for(int i = 0; i < producers.size(); ++i){
            LocationSubscriptionHandlerSingleTopicImpl handler = new LocationSubscriptionHandlerSingleTopicImpl(index);
            handler.initSubscriptionChangedCallback(producers.get(i));
            handlers.add(handler);
        }
        return handlers;
    }
    
    public List<LocationSubscriptionHandlerMultiTopicImpl> getConsumerLocationHandlers(List<PulsarLocationConsumer> consumers, Index index){
        List<LocationSubscriptionHandlerMultiTopicImpl> handlers = new ArrayList<>();
        for(int i = 0; i < consumers.size(); ++i){
            LocationSubscriptionHandlerMultiTopicImpl handler = new LocationSubscriptionHandlerMultiTopicImpl(index);
            handler.initSubscriptionChangedCallback(consumers.get(i));
            handlers.add(handler);
        }
        return handlers;
    }

    public List<LocationManager> getLocationManagersSingleTopic(List<LocationSubscriptionHandlerSingleTopicImpl> locationHandlers, List<String> trajectoryFiles){
        List<LocationManager> locationManagers = new ArrayList<>();
        for(int i = 0; i < locationHandlers.size(); ++i){
            LocationManagerImpl lm = new LocationManagerImpl(config.locationChangeInterval, trajectoryFiles.get(i));
            lm.initManager(locationHandlers.get(i));
            lm.start();
            locationManagerExecutor.execute(lm);
            locationManagers.add(lm); 
        }
        return locationManagers;
    }
 
   public List<LocationManager> getLocationManagersMultiTopic(List<LocationSubscriptionHandlerMultiTopicImpl> locationHandlers, List<String> trajectoryFiles){
        List<LocationManager> locationManagers = new ArrayList<>();
        for(int i = 0; i < locationHandlers.size(); ++i){
            LocationManagerImpl lm = new LocationManagerImpl(config.locationChangeInterval, trajectoryFiles.get(i));
            lm.initManager(locationHandlers.get(i));
            lm.start();
            locationManagerExecutor.execute(lm); 
            locationManagers.add(lm);
        }
        return locationManagers;
    }

    public class PubSubMetrics{
        public List<ConsumerMetrics> consumerMetrics;
        public List<ProducerMetrics> producerMetrics;
    }

    public void runTest(){
        if(config.numProducers != config.producerTrajectoryFiles.size()){
            System.out.println("Number of producers and trajectories don't match");
            return;    
        }
        if(config.numConsumers != config.consumerTrajectoryFiles.size()){
            System.out.println("Number of consumers and trajectories don't match");
            return;
        }
        StopWatch sw = new StopWatch();
        sw.start();
        PulsarLocationClient client = PulsarLocationClientBuilder.getPulsarLocationClient(config.pulsarConfigFile);
        List<PulsarLocationProducer> producers = getProducers(client, config.numProducers);
        sw.stop();
        log.info("created {} producers in {} ms", config.numProducers, sw.getTime());
        sw.reset();
        List<ProducerTask> producerTasks = getProducerTasks(producers);
        log.info("created producer tasks");

        sw.start();
        List<PulsarLocationConsumer> consumers = getConsumers(client, config.numConsumers);
        sw.stop();
        log.info("created {} consumers in {} ms", config.numConsumers, sw.getTime());

        List<ConsumerTask> consumerTasks = getConsumerTasks(consumers);
        log.info("created consumer tasks");    
        List<LocationSubscriptionHandlerSingleTopicImpl> producerHandlers = getProducerLocationHandlers(producers, index);
        log.info("created subscription handlers for producers");        

        List<LocationSubscriptionHandlerMultiTopicImpl> consumerHandlers = getConsumerLocationHandlers(consumers, index);
        log.info("created subscription handlers for consumers");
        
        List<LocationManager> producerLocationManagers = getLocationManagersSingleTopic(producerHandlers, config.producerTrajectoryFiles);
        log.info("Created producer location managers");

        List<LocationManager> consumerLocationManagers = getLocationManagersMultiTopic(consumerHandlers, config.consumerTrajectoryFiles);
        log.info("Created consumer location managers");
    
        try {
            Thread.sleep(1000 * config.testDurationSeconds);
        } catch(InterruptedException e){
            e.printStackTrace();
        }
       
        for(int i = 0; i < producerLocationManagers.size(); ++i){
            producerLocationManagers.get(i).stop();
        } 
        for(int i = 0; i < producerTasks.size(); ++i){
            producerTasks.get(i).stop();
        }
        for(int i = 0; i < consumerLocationManagers.size(); ++i){
            consumerLocationManagers.get(i).stop();
        }
        for(int i = 0; i < consumerTasks.size(); ++i){
            consumerTasks.get(i).stop();
        }
        consumerExecutor.shutdown();
        producerExecutor.shutdown();

        locationManagerExecutor.shutdown();
        
        PubSubMetrics metrics = new PubSubMetrics();
        metrics.producerMetrics = new ArrayList<>();
        metrics.consumerMetrics = new ArrayList<>();
        for(int i = 0; i < consumers.size(); ++i){
            metrics.consumerMetrics.add(consumers.get(i).getConsumerMetrics());
        }
        for(int i = 0; i < producers.size(); ++i){
            metrics.producerMetrics.add(producers.get(i).getProducerMetrics());
       }
       try {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.writeValue(new File(config.outputFile), metrics);
        }catch(IOException ex){
            ex.printStackTrace();
        }
    }
}
