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

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class StandardLockInternalsDriver implements LockInternalsDriver {
    static private final Logger log = LoggerFactory.getLogger(StandardLockInternalsDriver.class);

    /**
     *
     * @param maxLeases 每次允许几个客户端同时获取一个锁，默认最多只允许1个客户端获取一把锁
     * @return 是否或得锁
     */
    @Override
    public PredicateResults getsTheLock(CuratorFramework client, List<String> children, String sequenceNodeName, int maxLeases) throws Exception {
        // 获取本次创建的顺序节点的名字在子节点列表中的位置
        int ourIndex = children.indexOf(sequenceNodeName);
        // 确认 ourIndex >= 0
        validateOurIndex(sequenceNodeName, ourIndex);

        boolean getsTheLock = ourIndex < maxLeases;
        // 如果 getsTheLock 为false，则 pathToWatch 是本次创建的子节点的上一个节点
        // 比如本次创建的子节点在列表中的index是3（即ourIndex=3），那么就返回index=2的路径
        // 因为它要在它前一个子节点上注册一个Watcher
        String pathToWatch = getsTheLock ? null : children.get(ourIndex - maxLeases);

        return new PredicateResults(pathToWatch, getsTheLock);
    }

    @Override
    public String fixForSorting(String str, String lockName) {
        return standardFixForSorting(str, lockName);
    }

    public static String standardFixForSorting(String str, String lockName) {
        int index = str.lastIndexOf(lockName);
        if (index >= 0) {
            index += lockName.length();
            return index <= str.length() ? str.substring(index) : "";
        }
        return str;
    }

    static void validateOurIndex(String sequenceNodeName, int ourIndex) throws KeeperException {
        if (ourIndex < 0) {
            log.error("Sequential path not found: " + sequenceNodeName);
            throw new KeeperException.NoNodeException("Sequential path not found: " + sequenceNodeName);
        }
    }
}
