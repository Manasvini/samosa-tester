package epl.pubsub.location.tester;

import epl.pubsub.location.pulsarclient.PulsarLocationProducer;
import epl.pubsub.location.pulsarclient.ProducerMetrics;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ProducerTask implements Runnable {
    
    private PulsarLocationProducer producer;
    private String payload;
    private int interval;
    private String topic;
    private AtomicBoolean isStarted = new AtomicBoolean();   

    private static final Logger log = LoggerFactory.getLogger(ProducerTask.class);

    private int numMessagesSent = 0;

    public ProducerTask(PulsarLocationProducer producer, String payload, int interval, String topic){
        this.producer = producer;
        this.payload = payload;
        this.interval = interval;
        this.topic = topic;
        isStarted.set(false);
    }

    @Override
    public void run(){
        while(isStarted.get()){
            try {
                producer.sendMessage(payload.getBytes());
//.thenRun(()-> ++numMessagesSent);
                ++numMessagesSent;
                log.info("sent message");
                Thread.sleep(interval);
            } catch(InterruptedException e){
                e.printStackTrace();
            }
        }
    }

    void start(){
        isStarted.set(true);
        producer.start(topic);
    }

    void stop(){
        isStarted.set(false);
        producer.shutdown();
    }

    public double getAggregatedPublishLatency(){
        ProducerMetrics metrics = producer.getProducerMetrics();
        if(metrics.numMessagesPublished.get() == 0){
            return 0.0;
        }
        return metrics.aggregatePublishLatency.get() / metrics.numMessagesPublished.get();        
    }
    
    public long getNumMessagesPublished(){
        ProducerMetrics metrics = producer.getProducerMetrics();
        return metrics.numMessagesPublished.get();        
 
    }
    
    public double getSubscriptionChangeLatency(){
        ProducerMetrics metrics = producer.getProducerMetrics();
        if(metrics.numTopicChanges.get() == 0){
            return 0.0;
        }
        return metrics.aggregateTopicChangeLatency.get() / metrics.numTopicChanges.get();        
 
    }
}
