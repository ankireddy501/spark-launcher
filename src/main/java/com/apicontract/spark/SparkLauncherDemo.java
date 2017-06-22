package com.apicontract.spark;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.util.concurrent.CountDownLatch;

/**
 * Created by anki on 6/21/2017.
 */
public class SparkLauncherDemo {

    public static final String SPARK_HOME = "U:\\spark-2.1.1-bin-hadoop2.7";
    public static final String APPLICATION_JAR = "U:\\pocs\\spark-launcher\\target\\launcher-1.0-SNAPSHOT.jar";
    public static final String APPLICATION_CLASS = "com.apicontract.spark.ElasticSearchApp";

    public static void main(String args[]) throws Exception {

        SparkLauncher sparkLauncher = new SparkLauncher()
                .setSparkHome(SPARK_HOME)
                .setAppResource(APPLICATION_JAR)
                .addJar("U:\\pocs\\spark-launcher\\lib\\elasticsearch-spark-20_2.11-5.3.1.jar")
                .setMainClass(APPLICATION_CLASS)
                .setMaster("local[*]");
        SparkAppHandle handle = sparkLauncher.startApplication();
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        handle.addListener(new SparkAppHandle.Listener() {
            @Override
            public void stateChanged(SparkAppHandle handle) {
                if (handle.getState().isFinal()) {
                    countDownLatch.countDown();
                }
            }
            @Override
            public void infoChanged(SparkAppHandle handle) {
            }
        });
        countDownLatch.await();
    }

}
