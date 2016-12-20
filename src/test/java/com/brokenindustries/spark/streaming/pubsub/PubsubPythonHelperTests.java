package com.brokenindustries.spark.streaming.pubsub;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by developer on 12/16/16.
 */
public class PubsubPythonHelperTests {
    private  JavaStreamingContext jsc;

    @Before
    public void setup() throws Exception {
        SparkConf conf = new SparkConf(false).setMaster("local[2]").setAppName("Test");
        jsc = new JavaStreamingContext(conf, new Duration(1000));
    }

    @Test
    public void test_create_stream(){
        try {
            PubsubPythonHelper helper = new PubsubPythonHelper();
            ReceiverInputDStream<String> stream = helper.createStream(jsc, "<PROJECT_NAME>", "<PUBSUB_TOPIC_NAME>", "<SUBSCRIPTION_NAME>", Seconds.apply(1), StorageLevel.DISK_ONLY_2());
            stream.print();
            stream.start();

            jsc.start();
            jsc.awaitTerminationOrTimeout(10000);
        }catch(ClassNotFoundException e){
            Assert.fail();
        }
        catch(InterruptedException e){

        }
    }
}
