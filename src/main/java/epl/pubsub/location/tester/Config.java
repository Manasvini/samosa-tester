package epl.pubsub.location.tester;

import java.util.List;

public class Config{
    public int numProducers;
    public int numConsumers;
    public int numPartitionsPerTopic;
    public int testDurationSeconds; 
   
    public IndexConfig indexConfig;

    public List<String> consumerTrajectoryFiles;
    public List<String> producerTrajectoryFiles;

    public int locationChangeInterval;

    public String payloadFile;    
    public String pulsarConfigFile;
    
    public String outputFile;
}
