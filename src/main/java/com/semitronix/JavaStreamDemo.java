package com.semitronix;

import org.apache.spark.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;

import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class JavaStreamDemo {
    public static void main(String[] args) throws Exception{

        if(args.length < 2) {
            System.err.println("Usage: JavaStreamDemo <hostname> <port>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("streamDemo");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 7777);

        lines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.contains("error");
            }
        });

        lines.print();

        jssc.start();

        jssc.awaitTermination();
    }
}
