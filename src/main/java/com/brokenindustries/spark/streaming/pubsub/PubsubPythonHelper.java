package com.brokenindustries.spark.streaming.pubsub;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;

/**
 * Created by developer on 12/16/16.
 */
public class PubsubPythonHelper {

    public ReceiverInputDStream createStream(JavaStreamingContext jsc, String projectName, String topic, String subscription, Duration checkpointInterval,
                                                     StorageLevel storageLevel) throws ClassNotFoundException{
        return new PubsubInputDStream<String>(jsc, projectName, topic, subscription, checkpointInterval, storageLevel);

    }
}
