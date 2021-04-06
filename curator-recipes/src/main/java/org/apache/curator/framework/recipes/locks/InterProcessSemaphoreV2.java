/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.curator.framework.recipes.locks;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closeables;
import org.apache.curator.RetryLoop;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.PathAndBytesable;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.shared.SharedCountListener;
import org.apache.curator.framework.recipes.shared.SharedCountReader;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * A counting semaphore that works across JVMs. All processes
 * in all JVMs that use the same lock path will achieve an inter-process limited set of leases.
 * Further, this semaphore is mostly "fair" - each user will get a lease in the order requested
 * (from ZK's point of view).
 * </p>
 * <p/>
 * <p>
 * There are two modes for determining the max leases for the semaphore. In the first mode the
 * max leases is a convention maintained by the users of a given path. In the second mode a
 * {@link SharedCountReader} is used as the method for semaphores of a given path to determine
 * the max leases.
 * </p>
 * <p/>
 * <p>
 * If a {@link SharedCountReader} is <b>not</b> used, no internal checks are done to prevent
 * Process A acting as if there are 10 leases and Process B acting as if there are 20. Therefore,
 * make sure that all instances in all processes use the same numberOfLeases value.
 * </p>
 * <p/>
 * <p>
 * The various acquire methods return {@link Lease} objects that represent acquired leases. Clients
 * must take care to close lease objects  (ideally in a <code>finally</code>
 * block) else the lease will be lost. However, if the client session drops (crash, etc.),
 * any leases held by the client are automatically closed and made available to other clients.
 * </p>
 * <p/>
 * <p>
 * Thanks to Ben Bangert (ben@groovie.org) for the algorithm used.
 * </p>
 */
public class InterProcessSemaphoreV2 {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final InterProcessMutex lock;
    private final CuratorFramework client;
    private final String leasesPath;
    private final Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            notifyFromWatcher();
        }
    };

    private volatile byte[] nodeData;
    private volatile int maxLeases;

    private static final String LOCK_PARENT = "locks";
    private static final String LEASE_PARENT = "leases";
    private static final String LEASE_BASE_NAME = "lease-";

    /**
     * @param client    the client
     * @param path      path for the semaphore
     * @param maxLeases the max number of leases to allow for this instance
     */
    public InterProcessSemaphoreV2(CuratorFramework client, String path, int maxLeases) {
        this(client, path, maxLeases, null);
    }

    /**
     * @param client the client
     * @param path   path for the semaphore
     * @param count  the shared count to use for the max leases
     */
    public InterProcessSemaphoreV2(CuratorFramework client, String path, SharedCountReader count) {
        this(client, path, 0, count);
    }

    private InterProcessSemaphoreV2(CuratorFramework client, String path, int maxLeases, SharedCountReader count) {
        this.client = client;
        // lock的path是 path/locks
        lock = new InterProcessMutex(client, ZKPaths.makePath(path, LOCK_PARENT));
        this.maxLeases = (count != null) ? count.getCount() : maxLeases;
        // leasesPath = path/leases
        leasesPath = ZKPaths.makePath(path, LEASE_PARENT);

        if (count != null) {
            count.addListener
                    (
                            new SharedCountListener() {
                                @Override
                                public void countHasChanged(SharedCountReader sharedCount, int newCount) throws Exception {
                                    InterProcessSemaphoreV2.this.maxLeases = newCount;
                                }

                                @Override
                                public void stateChanged(CuratorFramework client, ConnectionState newState) {
                                    // no need to handle this here - clients should set their own connection state listener
                                }
                            }
                    );
        }
    }

    /**
     * Set the data to put for the node created by this semaphore. This must be called prior to calling one
     * of the acquire() methods.
     *
     * @param nodeData node data
     */
    public void setNodeData(byte[] nodeData) {
        this.nodeData = (nodeData != null) ? Arrays.copyOf(nodeData, nodeData.length) : null;
    }

    /**
     * Return a list of all current nodes participating in the semaphore
     *
     * @return list of nodes
     * @throws Exception ZK errors, interruptions, etc.
     */
    public Collection<String> getParticipantNodes() throws Exception {
        return client.getChildren().forPath(leasesPath);
    }

    /**
     * Convenience method. Closes all leases in the given collection of leases
     *
     * @param leases leases to close
     */
    public void returnAll(Collection<Lease> leases) {
        for (Lease l : leases) {
            Closeables.closeQuietly(l);
        }
    }

    /**
     * Convenience method. Closes the lease
     *
     * @param lease lease to close
     */
    public void returnLease(Lease lease) {
        Closeables.closeQuietly(lease);
    }

    /**
     * <p>Acquire a lease. If no leases are available, this method blocks until either the maximum
     * number of leases is increased or another client/process closes a lease.</p>
     * <p/>
     * <p>The client must close the lease when it is done with it. You should do this in a
     * <code>finally</code> block.</p>
     *
     * @return the new lease
     * @throws Exception ZK errors, interruptions, etc.
     */
    public Lease acquire() throws Exception {
        Collection<Lease> leases = acquire(1, 0, null);
        return leases.iterator().next();
    }

    /**
     * <p>Acquire <code>qty</code> leases. If there are not enough leases available, this method
     * blocks until either the maximum number of leases is increased enough or other clients/processes
     * close enough leases.</p>
     * <p/>
     * <p>The client must close the leases when it is done with them. You should do this in a
     * <code>finally</code> block. NOTE: You can use {@link #returnAll(Collection)} for this.</p>
     *
     * @param qty number of leases to acquire
     * @return the new leases
     * @throws Exception ZK errors, interruptions, etc.
     */
    public Collection<Lease> acquire(int qty) throws Exception {
        return acquire(qty, 0, null);
    }

    /**
     * <p>Acquire a lease. If no leases are available, this method blocks until either the maximum
     * number of leases is increased or another client/process closes a lease. However, this method
     * will only block to a maximum of the time parameters given.</p>
     * <p/>
     * <p>The client must close the lease when it is done with it. You should do this in a
     * <code>finally</code> block.</p>
     *
     * @param time time to wait
     * @param unit time unit
     * @return the new lease or null if time ran out
     * @throws Exception ZK errors, interruptions, etc.
     */
    public Lease acquire(long time, TimeUnit unit) throws Exception {
        Collection<Lease> leases = acquire(1, time, unit);
        return (leases != null) ? leases.iterator().next() : null;
    }

    /**
     * <p>Acquire <code>qty</code> leases. If there are not enough leases available, this method
     * blocks until either the maximum number of leases is increased enough or other clients/processes
     * close enough leases. However, this method will only block to a maximum of the time
     * parameters given. If time expires before all leases are acquired, the subset of acquired
     * leases are automatically closed.</p>
     * <p/>
     * <p>The client must close the leases when it is done with them. You should do this in a
     * <code>finally</code> block. NOTE: You can use {@link #returnAll(Collection)} for this.</p>
     *
     * @param qty  number of leases to acquire
     * @param time time to wait
     * @param unit time unit
     * @return the new leases or null if time ran out
     * @throws Exception ZK errors, interruptions, etc.
     */
    public Collection<Lease> acquire(int qty, long time, TimeUnit unit) throws Exception {
        long startMs = System.currentTimeMillis();
        boolean hasWait = (unit != null);
        long waitMs = hasWait ? TimeUnit.MILLISECONDS.convert(time, unit) : 0;

        Preconditions.checkArgument(qty > 0, "qty cannot be 0");

        ImmutableList.Builder<Lease> builder = ImmutableList.builder();
        boolean success = false;
        try {
            while (qty-- > 0) {
                int retryCount = 0;
                long startMillis = System.currentTimeMillis();
                boolean isDone = false;
                while (!isDone) {
                    switch (internalAcquire1Lease(builder, startMs, hasWait, waitMs)) {
                        case CONTINUE: {
                            isDone = true;
                            break;
                        }

                        case RETURN_NULL: {
                            return null;
                        }

                        case RETRY_DUE_TO_MISSING_NODE: {
                            // gets thrown by internalAcquire1Lease when it can't find the lock node
                            // this can happen when the session expires, etc. So, if the retry allows, just try it all again
                            if (!client.getZookeeperClient().getRetryPolicy().allowRetry(retryCount++, System.currentTimeMillis() - startMillis, RetryLoop.getDefaultRetrySleeper())) {
                                throw new KeeperException.NoNodeException("Sequential path not found - possible session loss");
                            }
                            // try again
                            break;
                        }
                    }
                }
            }
            success = true;
        } finally {
            if (!success) {
                returnAll(builder.build());
            }
        }

        return builder.build();
    }

    private enum InternalAcquireResult {
        CONTINUE,
        RETURN_NULL,
        RETRY_DUE_TO_MISSING_NODE
    }

    private InternalAcquireResult internalAcquire1Lease(ImmutableList.Builder<Lease> builder, long startMs, boolean hasWait, long waitMs) throws Exception {
        if (client.getState() != CuratorFrameworkState.STARTED) {
            return InternalAcquireResult.RETURN_NULL;
        }

        if (hasWait) {
            long thisWaitMs = getThisWaitMs(startMs, waitMs);
            if (!lock.acquire(thisWaitMs, TimeUnit.MILLISECONDS)) {
                return InternalAcquireResult.RETURN_NULL;
            }
        } else {
            // 释放分布式锁来防止并发操作
            lock.acquire();
        }
        try {
            // 在 leasesPath 路径下创建了一个临时顺序节点
            PathAndBytesable<String> createBuilder = client.create().creatingParentsIfNeeded().withProtection().withMode(CreateMode.EPHEMERAL_SEQUENTIAL);
            String path = (nodeData != null) ? createBuilder.forPath(ZKPaths.makePath(leasesPath, LEASE_BASE_NAME), nodeData) : createBuilder.forPath(ZKPaths.makePath(leasesPath, LEASE_BASE_NAME));
            String nodeName = ZKPaths.getNodeFromPath(path);
            builder.add(makeLease(path));

            // 因为用了 wait()，所以需要用 synchronized
            synchronized (this) {
                for (; ; ) {
                    // 获取leasesPath目录下的所有子节点
                    List<String> children = client.getChildren().usingWatcher(watcher).forPath(leasesPath);
                    if (!children.contains(nodeName)) {
                        log.error("Sequential path not found: " + path);
                        return InternalAcquireResult.RETRY_DUE_TO_MISSING_NODE;
                    }
                    // 如果子节点的数量不超过 maxLeases（也就是Semaphore最大运行的量）
                    if (children.size() <= maxLeases) {
                        break;
                    }
                    if (hasWait) {
                        long thisWaitMs = getThisWaitMs(startMs, waitMs);
                        if (thisWaitMs <= 0) {
                            return InternalAcquireResult.RETURN_NULL;
                        }
                        wait(thisWaitMs);
                    } else {
                        wait();
                    }
                }
            }
        } finally {
            lock.release();
        }
        return InternalAcquireResult.CONTINUE;
    }

    private long getThisWaitMs(long startMs, long waitMs) {
        long elapsedMs = System.currentTimeMillis() - startMs;
        return waitMs - elapsedMs;
    }

    private Lease makeLease(final String path) {
        return new Lease() {
            @Override
            public void close() throws IOException {
                try {
                    client.delete().guaranteed().forPath(path);
                } catch (KeeperException.NoNodeException e) {
                    log.warn("Lease already released", e);
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }

            @Override
            public byte[] getData() throws Exception {
                return client.getData().forPath(path);
            }
        };
    }

    private synchronized void notifyFromWatcher() {
        notifyAll();
    }
}
