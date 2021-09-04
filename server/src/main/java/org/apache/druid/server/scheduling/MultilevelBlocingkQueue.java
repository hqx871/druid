package org.apache.druid.server.scheduling;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ForwardingBlockingQueue;
import org.apache.druid.collections.ResourceGroupScheduler;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.IAE;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MultilevelBlocingkQueue<E> extends AbstractQueue<GroupResourceHolder<E>>
    implements BlockingQueue<GroupResourceHolder<E>>
{
  private final ResourceGroupScheduler resourceGroupScheduler;
  private final Map<String, ThrottleBlockingQueue<E>> childQueueMap;
  private final List<ThrottleBlockingQueue<E>> childQueueList;
  private final int[] childQueueResourceCapacities;
  private final int[] childQueueRunOpportunities;
  private final Set<String> runningQueues;
  private final int capacity;
  private int size;
  private int remainCapaicty;
  private static final String defaultGroupName = null;
  private final boolean soleUseMaxCapacity;

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition notEmpty = lock.newCondition();
  private final Condition notFull = lock.newCondition();

  private int nextChildQueuePosition;
  private final Comparator<ResourceHolder<E>> priorityComparator;

  public MultilevelBlocingkQueue(
      ResourceGroupScheduler resourceGroupScheduler,
      int capacity,
      boolean soleUseMaxCapacity,
      Comparator<ResourceHolder<E>> priorityComparator
  )
  {
    this.resourceGroupScheduler = resourceGroupScheduler;
    this.capacity = capacity;
    this.soleUseMaxCapacity = soleUseMaxCapacity;
    this.priorityComparator = priorityComparator;
    this.childQueueMap = new HashMap<>();
    this.childQueueList = new ArrayList<>();
    this.runningQueues = new HashSet<>();
    this.size = 0;

    addThrottleBlockingQueue(defaultGroupName, true);
    for (String resourceGroup : resourceGroupScheduler.getAllGroup()) {
      if (Objects.equals(defaultGroupName, resourceGroup)) {
        throw new IAE("resourceGroup cannot named %s", defaultGroupName);
      }
      addThrottleBlockingQueue(resourceGroup, false);
    }
    childQueueList.sort(ThrottleBlockingQueue.PRIORITY_COMPARATOR);
    childQueueResourceCapacities = childQueueList.stream().mapToInt(ThrottleBlockingQueue::getCapacity).toArray();
    childQueueRunOpportunities = Arrays.copyOf(childQueueResourceCapacities, childQueueResourceCapacities.length);
  }

  @Override
  public Iterator<GroupResourceHolder<E>> iterator()
  {
    lock.lock();
    try {
      List<GroupResourceHolder<E>> stream = childQueueList.stream()
                                                          .flatMap(q -> wrapResourceHolderStream(q.getGroup(), q))
                                                          .collect(Collectors.toList());
      return ImmutableList.copyOf(stream).iterator();
    }
    finally {
      lock.unlock();
    }
  }

  private Stream<GroupResourceHolder<E>> wrapResourceHolderStream(
      String group,
      Collection<ResourceHolder<E>> childQueue
  )
  {
    return childQueue.stream().map(item -> wrapResourceHolder(group, item));
  }

  private GroupResourceHolder<E> wrapResourceHolder(
      String resourceGroup,
      @Nonnull ResourceHolder<E> resourceHolder
  )
  {
    return new GroupResourceHolder<E>(resourceGroup, resourceHolder.get())
    {
      @Override
      public void close()
      {
        resourceHolder.close();
      }
    };
  }

  @Override
  public int size()
  {
    return size;
  }

  @Override
  public void put(GroupResourceHolder<E> r) throws InterruptedException
  {
    checkNonNull(r);
    ReentrantLock lock = this.lock;
    lock.lockInterruptibly();
    try {
      while (size >= capacity) {
        notFull.await();
      }
      ThrottleBlockingQueue<E> childQueue = getThrottleBlockingQueue(r);
      childQueue.put(r);
      size++;
      remainCapaicty = capacity - size;
      runningQueues.add(childQueue.getGroup());
      notEmpty.signal();
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  public boolean offer(GroupResourceHolder<E> r, long timeout, @Nonnull TimeUnit unit)
      throws InterruptedException
  {
    checkNonNull(r);
    if (timeout <= 0) {
      return false;
    }
    ReentrantLock lock = this.lock;
    long startNano = System.nanoTime();
    long waitNano = unit.toNanos(timeout);
    if (!lock.tryLock(waitNano, TimeUnit.NANOSECONDS)) {
      return false;
    }
    waitNano = startNano + waitNano - System.nanoTime();
    try {
      while (waitNano > 0 && size >= capacity) {
        waitNano = notFull.awaitNanos(waitNano);
      }
      if (waitNano <= 0) {
        return false;
      }
      ThrottleBlockingQueue<E> childQueue = getThrottleBlockingQueue(r);
      boolean added = childQueue.offer(r, waitNano, TimeUnit.NANOSECONDS);
      if (added) {
        size++;
        remainCapaicty = capacity - size;
        runningQueues.add(childQueue.getGroup());
        notEmpty.signal();
      }
      return added;
    }
    finally {
      lock.unlock();
    }
  }

  private ThrottleBlockingQueue<E> getThrottleBlockingQueue(GroupResourceHolder<E> r)
  {
    String resourceGroup = r.getGroup();
    ThrottleBlockingQueue<E> childQueue = childQueueMap.get(resourceGroup);
    if (childQueue == null) {
      throw new IAE(String.format("resourceGroup %s not exist", resourceGroup));
    }
    return childQueue;
  }

  private void addThrottleBlockingQueue(String resourceGroup, boolean isDefault)
  {
    int groupCapacity = resourceGroupScheduler.getGroupCapacity(resourceGroup);
    ThrottleBlockingQueue<E> queueWrapper = new ThrottleBlockingQueue<>(
        new PriorityBlockingQueue<>(16, priorityComparator),
        resourceGroup,
        resourceGroupScheduler,
        () -> isDefault || soleUseMaxCapacity && runningQueues.size() == 1,
        groupCapacity
    );
    childQueueList.add(queueWrapper);
    childQueueMap.put(resourceGroup, queueWrapper);
  }

  public String getDefaultGroupName()
  {
    return defaultGroupName;
  }

  @Override
  public boolean offer(@Nonnull GroupResourceHolder<E> r)
  {
    checkNonNull(r);
    if (size >= capacity) {
      return false;
    }
    ReentrantLock lock = this.lock;
    lock.lock();
    try {
      if (size >= capacity) {
        return false;
      }
      ThrottleBlockingQueue<E> childQueue = getThrottleBlockingQueue(r);
      boolean added = childQueue.offer(r);
      if (added) {
        size++;
        remainCapaicty = capacity - size;
        runningQueues.add(childQueue.getGroup());
        notEmpty.signal();
      }
      return added;
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  public GroupResourceHolder<E> take() throws InterruptedException
  {
    ReentrantLock lock = this.lock;
    lock.lockInterruptibly();
    try {
      GroupResourceHolder<E> r = pollInternal();
      while (r == null) {
        notEmpty.await();
        r = pollInternal();
      }
      notFull.signal();
      return r;
    }
    finally {
      lock.unlock();
    }
  }

  @Nullable
  private GroupResourceHolder<E> pollInternal()
  {
    if (size > 0) {
      for (int i = childQueueList.size(); i > 0; i--) {
        if (nextChildQueuePosition >= childQueueList.size()) {
          nextChildQueuePosition = 0;
        }
        ThrottleBlockingQueue<E> childQueue = childQueueList.get(nextChildQueuePosition);
        childQueueRunOpportunities[nextChildQueuePosition]--;

        ResourceHolder<E> r = childQueue.poll();
        if (r != null) {
          size--;
          remainCapaicty = capacity - size;
          if (childQueue.isEmpty()) {
            runningQueues.remove(childQueue.getGroup());
          }
          if (childQueueRunOpportunities[nextChildQueuePosition] <= 0) {
            childQueueRunOpportunities[nextChildQueuePosition] = childQueueResourceCapacities[nextChildQueuePosition];
            nextChildQueuePosition++;
          }
          return wrapResourceHolder(childQueue.getGroup(), r);
        } else {
          if (childQueueRunOpportunities[nextChildQueuePosition] <= 0) {
            childQueueRunOpportunities[nextChildQueuePosition] = childQueueResourceCapacities[nextChildQueuePosition];
          }
          nextChildQueuePosition++;
        }
      }
    }
    return null;
  }

  @Nullable
  @Override
  public GroupResourceHolder<E> poll()
  {
    ReentrantLock lock = this.lock;
    lock.lock();
    try {
      GroupResourceHolder<E> r = pollInternal();
      if (r != null) {
        notFull.signal();
      }
      return r;
    }
    finally {
      lock.unlock();
    }
  }

  @Nullable
  @Override
  public GroupResourceHolder<E> poll(long timeout, TimeUnit unit) throws InterruptedException
  {
    ReentrantLock lock = this.lock;
    lock.lockInterruptibly();
    try {
      long waitNano = unit.toNanos(timeout);
      GroupResourceHolder<E> r = pollInternal();
      while (waitNano > 0 && r == null) {
        waitNano = notEmpty.awaitNanos(waitNano);
        r = pollInternal();
      }
      if (r != null) {
        notFull.signal();
      }
      return r;
    }
    finally {
      lock.unlock();
    }
  }

  @Nullable
  @Override
  public GroupResourceHolder<E> peek()
  {
    if (size == 0) {
      return null;
    }
    ReentrantLock lock = this.lock;
    lock.lock();
    try {
      for (int i = childQueueList.size(); i > 0; i--) {
        if (nextChildQueuePosition >= childQueueList.size()) {
          nextChildQueuePosition = 0;
        }
        ThrottleBlockingQueue<E> childQueue = childQueueList.get(nextChildQueuePosition++);
        ResourceHolder<E> r = childQueue.peek();
        if (r != null) {
          return wrapResourceHolder(childQueue.getGroup(), r);
        }
      }
      return null;
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  public int remainingCapacity()
  {
    return remainCapaicty;
  }

  @Override
  public int drainTo(@Nonnull Collection<? super GroupResourceHolder<E>> dst)
  {
    return drainTo(dst, Integer.MAX_VALUE);
  }

  @Override
  public int drainTo(@Nonnull Collection<? super GroupResourceHolder<E>> dst, int maxElements)
  {
    ReentrantLock lock = this.lock;
    lock.lock();
    try {
      int drainedCount = 0;
      Iterator<ThrottleBlockingQueue<E>> iterator = childQueueList.iterator();
      List<ResourceHolder<E>> buffer = new ArrayList<>();
      while (drainedCount < maxElements && iterator.hasNext()) {
        ThrottleBlockingQueue<E> childQueue = iterator.next();
        drainedCount += childQueue.drainTo(buffer, maxElements - drainedCount);
        dst.addAll(wrapResourceHolderStream(childQueue.getGroup(), buffer).collect(Collectors.toList()));
        buffer.clear();
      }
      return drainedCount;
    }
    finally {
      lock.unlock();
    }
  }

  private static class ThrottleBlockingQueue<E> extends ForwardingBlockingQueue<ResourceHolder<E>>
  {
    private final BlockingQueue<ResourceHolder<E>> delegate;
    private final String group;
    private final ResourceGroupScheduler laneLimiter;
    private final Supplier<Boolean> useMaxCapacityPredicate;
    private final int capacity;

    public static final Comparator<ThrottleBlockingQueue<?>> PRIORITY_COMPARATOR = new Comparator<ThrottleBlockingQueue<?>>()
    {
      @Override
      public int compare(ThrottleBlockingQueue<?> o1, ThrottleBlockingQueue<?> o2)
      {
        return Integer.compare(o2.capacity, o1.capacity);
      }
    };

    public ThrottleBlockingQueue(
        BlockingQueue<ResourceHolder<E>> delegate,
        String group,
        ResourceGroupScheduler laneLimiter,
        Supplier<Boolean> useMaxCapacityPredicate,
        int capacity
    )
    {
      this.delegate = delegate;
      this.group = group;
      this.laneLimiter = laneLimiter;
      this.useMaxCapacityPredicate = useMaxCapacityPredicate;
      this.capacity = capacity;
    }

    public String getGroup()
    {
      return group;
    }

    public int getCapacity()
    {
      return capacity;
    }

    public int getGroupAvailableCapacity()
    {
      return laneLimiter.getGroupAvailableCapacity(group);
    }

    @Override
    protected BlockingQueue<ResourceHolder<E>> delegate()
    {
      return delegate;
    }

    @Nullable
    @Override
    public ResourceHolder<E> poll()
    {
      boolean useMaxCapacity = useMaxCapacityPredicate.get();
      if (size() > 0 && tryAccquire(useMaxCapacity)) {
        ResourceHolder<E> r = super.poll();
        if (r == null) {
          release(useMaxCapacity);
        } else {
          r = wrap(useMaxCapacity, r);
        }
        return r;
      }
      return null;
    }

    @Nullable
    @Override
    public ResourceHolder<E> poll(long timeout, TimeUnit unit) throws InterruptedException
    {
      boolean useMaxCapacity = useMaxCapacityPredicate.get();
      long startNano = System.nanoTime();
      long waitNano = unit.toNanos(timeout);
      if (size() > 0 && tryAccquire(useMaxCapacity, waitNano, TimeUnit.NANOSECONDS)) {
        waitNano = startNano + waitNano - System.nanoTime();
        ResourceHolder<E> r = super.poll(waitNano, TimeUnit.NANOSECONDS);
        if (r == null) {
          release(useMaxCapacity);
        } else {
          r = wrap(useMaxCapacity, r);
        }
        return r;
      }
      return null;
    }

    @Override
    public ResourceHolder<E> take() throws InterruptedException
    {
      boolean useMaxCapacity = useMaxCapacityPredicate.get();
      if (!useMaxCapacity) {
        laneLimiter.accquire(group);
      }
      return super.take();
    }

    private boolean tryAccquire(boolean useMaxCapacity)
    {
      if (useMaxCapacity) {
        return true;
      }
      return laneLimiter.tryAccquire(group);
    }

    private boolean tryAccquire(boolean useMaxCapacity, long timeout, TimeUnit unit) throws InterruptedException
    {
      if (useMaxCapacity) {
        return true;
      }
      return laneLimiter.tryAccquire(group, timeout, unit);
    }

    private void release(boolean maxCapacity)
    {
      if (!maxCapacity) {
        laneLimiter.release(group);
      }
    }

    private ResourceHolder<E> wrap(boolean useMaxCapacity, @Nonnull ResourceHolder<E> r)
    {
      return new ResourceHolder<E>()
      {

        @Override
        public E get()
        {
          return r.get();
        }

        @Override
        public void close()
        {
          r.close();
          if (!useMaxCapacity) {
            laneLimiter.release(group);
          }
        }
      };
    }
  }

  private static void checkNonNull(ResourceHolder<?> resource)
  {
    if (resource == null) {
      throw new IAE("resource holder cannot be null");
    }
  }
}


