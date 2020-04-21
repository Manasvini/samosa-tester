package epl.pubsub.location.tester;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;


class Main{
    public static void main(String [] args){
        try {
            String configFile = System.getProperty("configFile");;
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            mapper.findAndRegisterModules();
            Config config = mapper.readValue(new File(configFile), Config.class);
            System.out.println("creating test runner");
            TestRunner tr = new TestRunner(config);
            tr.runTest();
            System.out.println("Ran test");
         
        }
        catch(IOException ex){
            ex.printStackTrace();
        }
        System.exit(0);    
    }
}
