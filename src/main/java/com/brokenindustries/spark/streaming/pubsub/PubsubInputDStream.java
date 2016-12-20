package com.brokenindustries.spark.streaming.pubsub;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import org.apache.spark.streaming.receiver.Receiver;
import scala.reflect.ClassTag$;

/**
 * Created by developer on 12/16/16.
 */
public class PubsubInputDStream<T> extends ReceiverInputDStream<T> {

    private java.lang.String projectName;
    private java.lang.String topic;
    private java.lang.String subscription;
    private Duration checkpointInterval;
    private StorageLevel storageLevel;

    public PubsubInputDStream(JavaStreamingContext jsc, java.lang.String projectName, java.lang.String topic, java.lang.String subscription, Duration checkpointInterval,
                              StorageLevel storageLevel) throws ClassNotFoundException {
        super(jsc.ssc(), ClassTag$.MODULE$.apply(java.lang.String.class));
        this.projectName = projectName;
        this.topic = topic;
        this.subscription = subscription;
        this.checkpointInterval = checkpointInterval;
        this.storageLevel = storageLevel;
    }

    @Override
    public Receiver<T> getReceiver() {
        return new PubsubReceiver(this.projectName, this.topic, this.subscription, this.checkpointInterval, this.storageLevel);
    }

    public ReceiverInputDStream dstream(){
        return this;
    }

}
