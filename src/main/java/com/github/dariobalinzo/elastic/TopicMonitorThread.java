package com.github.dariobalinzo.elastic;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


/**
 * Thread that monitors Elastic for changes to the set of topics.
 */
public class TopicMonitorThread extends Thread {
  private static final Logger log = LoggerFactory.getLogger(TopicMonitorThread.class);
  private static final long timeout = 10000L;

  private final ConnectorContext context;
  private final CountDownLatch shutdownLatch;
  private final long pollMs;
  private final ElasticRepository elasticRepository;
  private final String topicPrefix;
  private List<String> topics;
  
  public TopicMonitorThread(ConnectorContext context, long pollMs, ElasticRepository elasticRepository, String topicPrefix) {
    this.context = context;
    this.shutdownLatch = new CountDownLatch(1);
    this.pollMs = pollMs;
    this.elasticRepository = elasticRepository;
    this.topicPrefix = topicPrefix;
    this.topics = new ArrayList<>();
  }

  public static  long getTimeout() {
    return timeout;
  }

  @Override
  public void run() {
    while (shutdownLatch.getCount() > 0) {
      try {
        if (updateTopics()) {
          context.requestTaskReconfiguration();
        }
      } catch (Exception e) {
          context.raiseError(e);
          throw e;
      }

      try {
        boolean shuttingDown = shutdownLatch.await(pollMs, TimeUnit.MILLISECONDS);
        if (shuttingDown) {
          return;
        }
      } catch (InterruptedException e) {
        log.error("Unexpected InterruptedException, ignoring: ", e);
      }
    }
  }

  public synchronized List<String> topics() {
    
    long started = System.currentTimeMillis();
    long now = started;
    while (topics.size() == 0 && now - started < timeout) {
      try {
        wait(timeout - (now - started));
      } catch (InterruptedException e) {
        // Ignore
      }
      now = System.currentTimeMillis();
    }
    if (topics.size() == 0) {
      throw new ConnectException("Topics could not be updated quickly enough.");
    }
    return topics;
  }

  public void shutdown() {
    shutdownLatch.countDown();
  }

  private synchronized boolean updateTopics() {
    final List<String> topics;
    try {
      topics = elasticRepository.catIndices(this.topicPrefix);
      log.debug("Got the following topics: " + Arrays.toString(topics.toArray()));
    } catch (RuntimeException e) {
      log.error("Error while trying to get updated topics list, ignoring and waiting for next table poll interval", e);
      return false;
    }

    if (!topics.equals(this.topics)) {
      log.debug("After filtering we got topics: " + Arrays.toString(topics.toArray()));
      List<String> previousTopics = this.topics;
      this.topics = topics;
      notifyAll();
      // Only return true if the table list wasn't previously null, i.e. if this was not the
      // first table lookup
      return previousTopics != null;
    }

    return false;
  }
}
