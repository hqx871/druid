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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.druid.collections.ResourceGroupScheduler;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.LanePrioritizedCallable;
import org.apache.druid.query.LanePrioritizedRunnable;
import org.apache.druid.query.PrioritizedExecutorService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MultilevelQueueExecutorService extends PrioritizedExecutorService
{
  public static MultilevelQueueExecutorService create(
      Lifecycle lifecycle,
      DruidProcessingConfig config,
      int capacity,
      ResourceGroupScheduler laneCapacityScheduler,
      boolean soleUseMaxCapacity
  )
  {
    MultilevelBlocingkQueue<Runnable> multilevelBlocingkQueue = new MultilevelBlocingkQueue<>(
        laneCapacityScheduler,
        capacity,
        soleUseMaxCapacity,
        new Comparator<ResourceHolder<Runnable>>()
        {
          @Override
          public int compare(ResourceHolder<Runnable> o1, ResourceHolder<Runnable> o2)
          {
            if (o1.get() instanceof PrioritizedListenableFutureTask
                && o2.get() instanceof PrioritizedListenableFutureTask) {
              return PrioritizedListenableFutureTask.PRIORITY_COMPARATOR.compare(
                  (PrioritizedListenableFutureTask) o1.get(),
                  (PrioritizedListenableFutureTask) o2.get()
              );
            }
            return 0;
          }
        }
    );
    TaskQueue<Runnable> runnables = new TaskQueue<>(multilevelBlocingkQueue);
    ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
        config.getNumThreads(),
        config.getNumThreads(),
        0L,
        TimeUnit.MILLISECONDS,
        runnables,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat(config.getFormatString()).build()
    );
    final MultilevelQueueExecutorService service = new MultilevelQueueExecutorService(
        threadPoolExecutor, config, multilevelBlocingkQueue.getDefaultGroupName()
    );

    lifecycle.addHandler(
        new Lifecycle.Handler()
        {
          @Override
          public void start()
          {
          }

          @Override
          public void stop()
          {
            service.shutdownNow();
          }
        }
    );

    return service;
  }

  private final AtomicLong queuePosition = new AtomicLong(Long.MAX_VALUE);
  private final boolean allowRegularTasks;
  private final DruidProcessingConfig config;
  final ThreadPoolExecutor threadPoolExecutor; // Used in unit tests
  private final String defaultLane;

  public MultilevelQueueExecutorService(
      ThreadPoolExecutor threadPoolExecutor,
      DruidProcessingConfig config,
      String defaultLane
  )
  {
    this(threadPoolExecutor, false, 0, config, defaultLane);
  }

  public MultilevelQueueExecutorService(
      ThreadPoolExecutor threadPoolExecutor,
      boolean allowRegularTasks,
      int defaultPriority,
      DruidProcessingConfig config,
      String defaultLane
  )
  {
    super(threadPoolExecutor, allowRegularTasks, defaultPriority, config);
    this.threadPoolExecutor = threadPoolExecutor;
    this.allowRegularTasks = allowRegularTasks;
    this.config = config;
    this.defaultLane = defaultLane;
  }

  @Override
  protected <T> LanePrioritizedListenableFutureTask<T> newTaskFor(Runnable runnable, T value)
  {
    Preconditions.checkArgument(
        allowRegularTasks || runnable instanceof LanePrioritizedRunnable,
        "task does not implement LanePrioritizedRunnable"
    );
    return new LanePrioritizedListenableFutureTask<>(
        ListenableFutureTask.create(runnable, value),
        getLane(runnable),
        getPriority(runnable),
        config.isFifo() ? queuePosition.decrementAndGet() : 0
    );
  }

  @Override
  protected <T> LanePrioritizedListenableFutureTask<T> newTaskFor(Callable<T> callable)
  {
    Preconditions.checkArgument(
        allowRegularTasks || callable instanceof LanePrioritizedCallable,
        "task does not implement LanePrioritizedCallable"
    );
    return new LanePrioritizedListenableFutureTask<>(
        ListenableFutureTask.create(callable),
        getLane(callable),
        getPriority(callable),
        config.isFifo() ? queuePosition.decrementAndGet() : 0
    );
  }

  private String getLane(Runnable runnable)
  {
    String lane = null;
    if (runnable instanceof LanePrioritizedRunnable) {
      lane = ((LanePrioritizedRunnable) runnable).getLane();
    }
    return Strings.isNullOrEmpty(lane) ? defaultLane : lane;
  }

  private <T> String getLane(Callable<T> callable)
  {
    String lane = null;
    if (callable instanceof LanePrioritizedCallable) {
      lane = ((LanePrioritizedCallable) callable).getLane();
    }
    return Strings.isNullOrEmpty(lane) ? defaultLane : lane;
  }

  private static class LanePrioritizedListenableFutureTask<V> extends PrioritizedListenableFutureTask<V>
      implements LanePrioritizedRunnable
  {
    private final String lane;

    LanePrioritizedListenableFutureTask(ListenableFutureTask<V> delegate, String lane, int priority, long position)
    {
      super(delegate, priority, position);
      this.lane = lane;
    }

    @Override
    public String getLane()
    {
      return lane;
    }
  }

  private static class TaskQueue<E extends Runnable> extends AbstractQueue<E> implements BlockingQueue<E>
  {
    private final MultilevelBlocingkQueue<E> delegate;

    private TaskQueue(MultilevelBlocingkQueue<E> delegate)
    {
      this.delegate = delegate;
    }

    @Nonnull
    @Override
    public Iterator<E> iterator()
    {
      Iterator<GroupResourceHolder<E>> iter = delegate.iterator();
      return new Iterator<E>()
      {
        @Override
        public boolean hasNext()
        {
          return iter.hasNext();
        }

        @Override
        public E next()
        {
          return iter.next().get();
        }
      };
    }

    @Override
    public int size()
    {
      return delegate.size();
    }

    @Override
    public void put(E e) throws InterruptedException
    {
      GroupResourceHolder<E> resourceHolder = getGroupResourceHolder(e);
      delegate.put(resourceHolder);
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException
    {
      GroupResourceHolder<E> resourceHolder = getGroupResourceHolder(e);
      return delegate.offer(resourceHolder, timeout, unit);
    }

    @Override
    public E take() throws InterruptedException
    {
      GroupResourceHolder<E> resourceHolder = delegate.take();
      resourceHolder = addCloser(resourceHolder);
      return resourceHolder.get();
    }

    @Nullable
    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException
    {
      GroupResourceHolder<E> resourceHolder = delegate.poll(timeout, unit);
      resourceHolder = addCloser(resourceHolder);
      return resourceHolder == null ? null : resourceHolder.get();
    }

    @Override
    public int remainingCapacity()
    {
      return delegate.remainingCapacity();
    }

    @Override
    public int drainTo(Collection<? super E> c)
    {
      return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements)
    {
      List<GroupResourceHolder<E>> buffer = new ArrayList<>(Math.min(maxElements, delegate.size()));
      delegate.drainTo(buffer, maxElements);
      buffer.forEach(item -> c.add(item.get()));
      return buffer.size();
    }

    @Override
    public boolean offer(E e)
    {
      GroupResourceHolder<E> resourceHolder = getGroupResourceHolder(e);
      return delegate.offer(resourceHolder);
    }

    @Nullable
    @Override
    public E poll()
    {
      GroupResourceHolder<E> resourceHolder = delegate.poll();
      resourceHolder = addCloser(resourceHolder);
      return resourceHolder == null ? null : resourceHolder.get();
    }

    @Nullable
    @Override
    public E peek()
    {
      GroupResourceHolder<E> resourceHolder = delegate.peek();
      return resourceHolder == null ? null : resourceHolder.get();
    }

    private GroupResourceHolder<E> addCloser(GroupResourceHolder<E> resourceHolder)
    {
      if (resourceHolder != null) {
        ((ListenableFuture) resourceHolder.get()).addListener(new Runnable()
        {
          @Override
          public void run()
          {
            resourceHolder.close();
          }
        }, Execs.directExecutor());
      }
      return resourceHolder;
    }

    private GroupResourceHolder<E> getGroupResourceHolder(E e)
    {
      LanePrioritizedListenableFutureTask<?> task = (LanePrioritizedListenableFutureTask<?>) e;
      return new GroupResourceHolder<>(task.getLane(), e);
    }
  }
}
