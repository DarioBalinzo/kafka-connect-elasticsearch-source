package com.github.dariobalinzo.elastic;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


/**
 * Thread that monitors Elastic for changes to the set of topics.
 */
public class ElasticIndexMonitorThread extends Thread {
  private static final Logger log = LoggerFactory.getLogger(ElasticIndexMonitorThread.class);
  private static final long TIMEOUT = 10_000L;

  private final ConnectorContext context;
  private final CountDownLatch shutdownLatch;
  private final long pollMs;
  private final ElasticRepository elasticRepository;
  private final String prefix;
  private List<String> indexes;
  
  public ElasticIndexMonitorThread(ConnectorContext context, long pollMs, ElasticRepository elasticRepository, String prefix) {
    this.context = context;
    this.shutdownLatch = new CountDownLatch(1);
    this.pollMs = pollMs;
    this.elasticRepository = elasticRepository;
    this.prefix = prefix;
    this.indexes = new ArrayList<>();
  }

  public static  long getTimeout() {
    return TIMEOUT;
  }

  @Override
  public void run() {
    while (shutdownLatch.getCount() > 0) {
      try {
        if (updateIndexes()) {
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

  public synchronized List<String> indexes() {
    
    long started = System.currentTimeMillis();
    long now = started;
    while (indexes.isEmpty() && now - started < TIMEOUT) {
      try {
        wait(TIMEOUT - (now - started));
      } catch (InterruptedException e) {
        // Ignore
      }
      now = System.currentTimeMillis();
    }
    if (indexes.isEmpty()) {
      throw new ConnectException("Cannot find any elasticsearch index");
    }
    return indexes;
  }

  public void shutdown() {
    shutdownLatch.countDown();
  }

  private synchronized boolean updateIndexes() {
    final List<String> indexes;
    try {
      indexes = elasticRepository.catIndices(this.prefix);
      log.debug("Got the following topics: {}", indexes);
    } catch (RuntimeException e) {
      log.error("Error while trying to get updated topics list, ignoring and waiting for next table poll interval", e);
      return false;
    }

    if (!indexes.equals(this.indexes)) {
      log.debug("After filtering we got topics: {}", indexes);
      List<String> previousIndexes = this.indexes;
      this.indexes = indexes;
      notifyAll();
      // Only return true if the table list wasn't previously null, i.e. if this was not the
      // first table lookup
      return !previousIndexes.isEmpty();
    }
    return false;
  }
}
