package epl.pubsub.location.tester;

import epl.pubsub.location.pulsarclient.PulsarLocationConsumer;
import epl.pubsub.location.pulsarclient.MessageCallback;
import epl.pubsub.location.pulsarclient.ConsumerMetrics;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.List;

import org.apache.pulsar.client.api.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ConsumerTask implements MessageCallback, Runnable {
    
    private PulsarLocationConsumer consumer;
    private int interval = 10;
    private AtomicBoolean isStarted = new AtomicBoolean();
    private List<String> initialTopics;   
    private String subName;

    private static final Logger log = LoggerFactory.getLogger(ConsumerTask.class);
    

    public ConsumerTask(PulsarLocationConsumer consumer, List<String> topics, String subName){
        this.consumer = consumer;
        isStarted.set(false);
        this.subName = subName;
        this.initialTopics = topics;
    }

    @Override
    public void onMessageReceived(Message<byte[]> msg){
        log.info("Received msg");
        
    }
    
    @Override
    public void run(){
        try {
            while(isStarted.get()){
                Thread.sleep(interval);
            }
        } catch(InterruptedException e){
            e.printStackTrace();
        }
    }
    
    public void start(){
        consumer.start(initialTopics, subName, this);
        isStarted.set(true);
    
    }

    public void stop(){
        isStarted.set(false);
        consumer.shutdown();
    }

    public double getAggregatedLatency(){
        ConsumerMetrics metrics = consumer.getConsumerMetrics();
        if(metrics.numMessagesConsumed.get() == 0){
            return 0.0;
        }
        return metrics.aggregateEndToEndLatency.get() / metrics.numMessagesConsumed.get();        
    }
    
    public long getNumMessagesReceived(){
        ConsumerMetrics metrics = consumer.getConsumerMetrics();
        return metrics.numMessagesConsumed.get();        
 
    }
    
    public double getSubscriptionChangeLatency(){
        ConsumerMetrics metrics = consumer.getConsumerMetrics();
        if(metrics.numTopicChanges.get() == 0){
            return 0.0;
        }
        return metrics.aggregateTopicChangeLatency.get() / metrics.numTopicChanges.get();        
 
    }
}
