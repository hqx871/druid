/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server.scheduling;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.collections.ResourceGroupScheduler;
import org.apache.druid.collections.SemaphoreResourceGroupScheduler;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.query.AbstractLanePrioritizedCallable;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.LanePrioritizedRunnable;
import org.apache.druid.query.PrioritizedCallable;
import org.apache.druid.query.PrioritizedRunnable;
import org.apache.druid.server.QueryLaningStrategy;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.internal.AssumptionViolatedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
@RunWith(Parameterized.class)
public class MultilevelQueueExecutorServiceTest
{
  private static final String defaultLane = null;
  private static final String singleThreadLane = "singleThreadLane";
  private static final String twoThreadLane = "twoThreadLane";

  private static final String[] lanes = new String[]{
      defaultLane, twoThreadLane, singleThreadLane
  };

  private MultilevelQueueExecutorService exec;
  private final int numThreads;
  private final boolean useFifo;
  private final boolean soleUseMaxCapacity;
  private final DruidProcessingConfig config;

  private CountDownLatch runLatch;
  private final ResourceGroupScheduler resourceGroupScheduler;

  @Parameterized.Parameters
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{6, true, true},
        new Object[]{6, true, false},
        new Object[]{6, false, true},
        new Object[]{6, false, false}
    );
  }

  public MultilevelQueueExecutorServiceTest(
      final int numThreads,
      final boolean useFifo,
      final boolean soleUseMaxCapacity
  )
  {
    this.numThreads = numThreads;
    this.useFifo = useFifo;
    this.soleUseMaxCapacity = soleUseMaxCapacity;
    this.config = new DruidProcessingConfig()
    {
      @Override
      public String getFormatString()
      {
        return "test-mlq-%s";
      }

      @Override
      public int getNumThreads()
      {
        return numThreads;
      }

      @Override
      public boolean isFifo()
      {
        return useFifo;
      }
    };
    QueryLaningStrategy laningStrategy = new ManualQueryLaningStrategy(
        ImmutableMap.of(singleThreadLane, 1, twoThreadLane, 2),
        false
    );

    Object2IntMap<String> configMap = laningStrategy.getLaneLimits(numThreads);
    this.resourceGroupScheduler = new SemaphoreResourceGroupScheduler(numThreads, configMap);
  }

  @Before
  public void setUp()
  {
    exec = MultilevelQueueExecutorService.create(
        new Lifecycle(),
        config,
        Integer.MAX_VALUE,
        resourceGroupScheduler,
        soleUseMaxCapacity
    );
    this.runLatch = new CountDownLatch(1);
  }

  @After
  public void tearDown()
  {
    exec.shutdownNow();
  }

  /**
   * Submits a normal priority task to block the queue, followed by low, high, normal priority tasks.
   * Tests to see that the high priority task is executed first, followed by the normal and low priority tasks.
   *
   * @throws Exception
   */
  @Test
  public void testSubmit() throws Exception
  {
    int[] priorities = new int[]{-1, 0, 2};
    int taskCont = priorities.length;
    final ConcurrentLinkedQueue<Integer> order = new ConcurrentLinkedQueue<Integer>();
    final CountDownLatch finishLatch = new CountDownLatch(taskCont);
    final CountDownLatch occupyRunnerLatch = new CountDownLatch(numThreads);
    addOccupyRunnerLatch(occupyRunnerLatch, singleThreadLane, numThreads);
    for (int priority : priorities) {
      exec.submit(
          new AbstractLanePrioritizedCallable<Integer>(singleThreadLane, priority)
          {
            @Override
            public Integer call() throws InterruptedException
            {
              runLatch.await();
              order.add(priority);
              finishLatch.countDown();
              return priority;
            }
          }
      );
    }
    occupyRunnerLatch.await();
    runLatch.countDown();
    finishLatch.await();

    Assert.assertEquals(3, order.size());
    List<Integer> expected = ImmutableList.of(2, 0, -1);
    if (useFifo && !soleUseMaxCapacity) {
      Assert.assertEquals(expected, ImmutableList.copyOf(order));
    }
  }

  // Make sure entries are processed FIFO
  @Test
  public void testOrderedExecutionEqualPriorityRunnable() throws ExecutionException, InterruptedException
  {
    final int numTasks = 100;
    final List<ListenableFuture<?>> futures = Lists.newArrayListWithExpectedSize(numTasks);
    final AtomicInteger localHasRun = new AtomicInteger(0);
    final AtomicInteger globalHasRun = new AtomicInteger(0);
    final Reporter reporter = new Reporter(singleThreadLane);
    final CountDownLatch occupyRunnerLatch = new CountDownLatch(numThreads);
    addOccupyRunnerLatch(occupyRunnerLatch, singleThreadLane, numThreads);
    for (int i = 0; i < numTasks; ++i) {
      futures.add(exec.submit(getCheckingLanePrioritizedRunnable(
          i,
          localHasRun,
          globalHasRun,
          singleThreadLane,
          reporter
      )));
    }
    occupyRunnerLatch.await();
    runLatch.countDown();
    checkFutures(futures);
  }

  @Test
  public void testOrderedExecutionEqualPriorityCallable() throws ExecutionException, InterruptedException
  {
    final int numTasks = 1_000;
    final List<ListenableFuture<?>> futures = Lists.newArrayListWithExpectedSize(numTasks);
    final AtomicInteger localHasRun = new AtomicInteger(0);
    final AtomicInteger globalHasRun = new AtomicInteger(0);
    final Reporter reporter = new Reporter(singleThreadLane);
    final CountDownLatch occupyRunnerLatch = new CountDownLatch(numThreads);
    addOccupyRunnerLatch(occupyRunnerLatch, singleThreadLane, numThreads);
    for (int i = 0; i < numTasks; ++i) {
      futures.add(exec.submit(getCheckingLanePrioritizedCallable(
          i,
          localHasRun,
          globalHasRun,
          singleThreadLane,
          reporter
      )));
    }
    occupyRunnerLatch.await();
    runLatch.countDown();
    checkFutures(futures);
  }

  @Test
  public void testOrderedExecutionEqualPriorityMix() throws ExecutionException, InterruptedException
  {
    exec = new MultilevelQueueExecutorService(exec.threadPoolExecutor, true, 0, config, defaultLane);
    final int numTasks = 1_000;
    final List<ListenableFuture<?>> futures = Lists.newArrayListWithExpectedSize(numTasks);
    final AtomicInteger localHasRun = new AtomicInteger(0);
    final AtomicInteger globalHasRun = new AtomicInteger(0);
    final Random random = new Random(789401);
    final Reporter reporter = new Reporter(defaultLane);
    final CountDownLatch occupyRunnerLatch = new CountDownLatch(numThreads);
    addOccupyRunnerLatch(occupyRunnerLatch, singleThreadLane, numThreads);
    for (int i = 0; i < numTasks; ++i) {
      switch (random.nextInt(2)) {
        case 0:
          futures.add(exec.submit(getCheckingLanePrioritizedCallable(
              i,
              localHasRun,
              globalHasRun,
              singleThreadLane,
              reporter
          )));
          break;
        case 1:
          futures.add(exec.submit(getCheckingLanePrioritizedRunnable(
              i,
              localHasRun,
              globalHasRun,
              singleThreadLane,
              reporter
          )));
          break;
        default:
          Assert.fail("Bad random result");
      }
    }
    occupyRunnerLatch.await();
    runLatch.countDown();
    checkFutures(futures);
  }

  @Test
  public void testOrderedExecutionMultipleLaneMix() throws ExecutionException, InterruptedException
  {
    exec = new MultilevelQueueExecutorService(exec.threadPoolExecutor, true, 0, config, defaultLane);
    final int numTasks = 999999;
    final int numLanes = lanes.length;
    final int laneNumTasks = numTasks / numLanes;
    final List<ListenableFuture<?>>[] futures = new List[]{
        Lists.newArrayListWithExpectedSize(laneNumTasks),
        Lists.newArrayListWithExpectedSize(laneNumTasks),
        Lists.newArrayListWithExpectedSize(laneNumTasks)
    };
    final AtomicInteger globalHasRun = new AtomicInteger(0);
    final AtomicInteger[] laneHasRun = new AtomicInteger[]{
        new AtomicInteger(0), new AtomicInteger(0), new AtomicInteger(0)
    };
    final List<Reporter> reporters = ImmutableList.of(
        new Reporter(defaultLane), new Reporter(twoThreadLane), new Reporter(singleThreadLane)
    );
    final Random random = new Random(789401);

    final CountDownLatch occupyRunnerLatch = new CountDownLatch(numThreads);
    addOccupyRunnerLatch(occupyRunnerLatch, singleThreadLane, numThreads);
    for (int i = 0; i < numTasks; ++i) {
      final int laneBucket = i % numLanes;
      final int expectedPriorityOrder = i / numLanes;
      final String lane = lanes[laneBucket];
      final AtomicInteger localHasRun = laneHasRun[laneBucket];
      final Reporter reporter = reporters.get(laneBucket);
      if (random.nextBoolean()) {
        futures[laneBucket].add(
            exec.submit(
                getCheckingLanePrioritizedCallable(
                    expectedPriorityOrder,
                    localHasRun,
                    globalHasRun,
                    lane,
                    0,
                    reporter
                )
            )
        );
      } else {
        futures[laneBucket].add(
            exec.submit(
                getCheckingLanePrioritizedRunnable(
                    expectedPriorityOrder,
                    localHasRun,
                    globalHasRun,
                    lane,
                    0,
                    reporter
                )
            )
        );
      }
    }
    occupyRunnerLatch.await();
    runLatch.countDown();
    for (int i = 0; i < numLanes; i++) {
      checkFutures(futures[i]);
    }
    Assert.assertTrue(
        String.format("lanes finish order should keepace with their capacity,\n%s", reporters),
        reporters.get(0).getMax() < reporters.get(1).getMax() && reporters.get(1).getMax() < reporters.get(2).getMax()
    );
    Assert.assertTrue(
        String.format("lanes run order should keepace with their capacity,\n%s", reporters),
        reporters.get(0).getAvg() < reporters.get(1).getAvg() && reporters.get(1).getAvg() < reporters.get(2).getAvg()
    );

    Assert.assertTrue(
        String.format("some lane harving to die,\n%s", reporters),
        reporters.get(0).getMax() > reporters.get(1).getMin() && reporters.get(1).getMax() > reporters.get(2).getMin()
    );
    Assert.assertEquals(String.format(
        "last task order should be %d, but actual is %d", numTasks - 1, reporters.get(2).getMax()
    ), numTasks - 1, reporters.get(2).getMax());
  }

  private void checkFutures(Iterable<ListenableFuture<?>> futures)
      throws InterruptedException, ExecutionException
  {
    for (ListenableFuture<?> future : futures) {
      try {
        future.get();
      }
      catch (ExecutionException e) {
        if (!(e.getCause() instanceof AssumptionViolatedException)) {
          throw e;
        }
      }
    }
  }

  private void addOccupyRunnerLatch(CountDownLatch queueOccupyLatch, String lane, int count)
  {
    for (int i = 0; i < count; i++) {
      exec.submit(
          new AbstractLanePrioritizedCallable<Void>(lane, 9)
          {
            @Override
            public Void call() throws InterruptedException
            {
              queueOccupyLatch.countDown();
              runLatch.await();
              return null;
            }
          }
      );
    }
  }

  private PrioritizedCallable<Boolean> getCheckingLanePrioritizedCallable(
      final int myOrder,
      final AtomicInteger localHasRun,
      final AtomicInteger globalHasRun,
      final String lane,
      final Reporter reporter
  )
  {
    return getCheckingLanePrioritizedCallable(myOrder, localHasRun, globalHasRun, lane, 0, reporter);
  }

  private PrioritizedCallable<Boolean> getCheckingLanePrioritizedCallable(
      final int myOrder,
      final AtomicInteger localHasRun,
      final AtomicInteger globalHasRun,
      final String lane,
      final int priority,
      final Reporter reporter
  )
  {
    final Callable<Boolean> delegate = getCheckingCallable(myOrder, localHasRun, globalHasRun, reporter);
    return new AbstractLanePrioritizedCallable<Boolean>(lane, priority)
    {
      @Override
      public Boolean call() throws Exception
      {
        return delegate.call();
      }
    };
  }

  private Callable<Boolean> getCheckingCallable(
      final int myOrder,
      final AtomicInteger localHasRun,
      final AtomicInteger globalHasRun,
      final Reporter reporter
  )
  {
    final Runnable runnable = getCheckingRunnable(myOrder, localHasRun, globalHasRun, reporter);
    return new Callable<Boolean>()
    {
      @Override
      public Boolean call()
      {
        runnable.run();
        return true;
      }
    };
  }

  private Runnable getCheckingRunnable(
      final int myOrder,
      final AtomicInteger localHasRun,
      final AtomicInteger globalHasRun,
      final Reporter reporter
  )
  {
    return new Runnable()
    {
      @Override
      public void run()
      {
        try {
          runLatch.await();
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
        synchronized (globalHasRun) {
          int actualLocalHasRun = localHasRun.getAndIncrement();
          int numLaneThreads = resourceGroupScheduler.getGroupCapacity(reporter.lane);
          if (numLaneThreads == 1 && !soleUseMaxCapacity && useFifo) {
            Assert.assertEquals(myOrder, actualLocalHasRun);
          }
          reporter.report(globalHasRun.getAndIncrement());
        }
      }
    };
  }


  private PrioritizedRunnable getCheckingLanePrioritizedRunnable(
      final int myOrder,
      final AtomicInteger localHasRun,
      final AtomicInteger globalHasRun,
      final String lane,
      final Reporter reporter
  )
  {
    return getCheckingLanePrioritizedRunnable(myOrder, localHasRun, globalHasRun, lane, 0, reporter);
  }

  private PrioritizedRunnable getCheckingLanePrioritizedRunnable(
      final int myOrder,
      final AtomicInteger localHasRun,
      final AtomicInteger globalHasRun,
      final String lane,
      final int priority,
      final Reporter reporter
  )
  {
    final Runnable delegate = getCheckingRunnable(myOrder, localHasRun, globalHasRun, reporter);
    return new LanePrioritizedRunnable()
    {
      @Override
      public String getLane()
      {
        return lane;
      }

      @Override
      public int getPriority()
      {
        return priority;
      }

      @Override
      public void run()
      {
        delegate.run();
      }
    };
  }

  private static class Reporter
  {
    private final String lane;
    private long min = Long.MAX_VALUE;
    private long max;
    private long count;
    private long sum;

    private Reporter(String lane)
    {
      this.lane = lane;
    }

    public void report(long value)
    {
      min = Math.min(value, min);
      max = Math.max(value, max);
      count++;
      sum += value;
    }

    public long getMin()
    {
      return min;
    }

    public long getMax()
    {
      return max;
    }

    public long getAvg()
    {
      return count > 0 ? sum / count : 0;
    }

    @Override
    public String toString()
    {
      return "Reporter{" +
             "name=" + lane +
             ", min=" + min +
             ", max=" + max +
             ", avg=" + getAvg() +
             '}';
    }
  }
}
