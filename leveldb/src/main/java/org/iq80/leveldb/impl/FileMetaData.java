/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.iq80.leveldb.impl;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * level层级里有多个.sst文件，对应的一个.sst文件实体
 */
public class FileMetaData {
    /**
     * number是文件的编号，全局递增，通过number就可以找到一个sst文件
     */
    private final long number;

    /**
     * File size in bytes
     * 文件大小(以字节为单位)
     */
    private final long fileSize;

    /**
     * Smallest internal key served by table
     * 表所服务的最小内部键
     */
    private final InternalKey smallest;

    /**
     * Largest internal key served by table
     * 表服务的最大内部键
     */
    private final InternalKey largest;

    /**
     * Seeks allowed until compaction
     * compact之前允许的 seek 次数 （参见 Version）
     */
    // todo this mutable state should be moved elsewhere
    private final AtomicInteger allowedSeeks = new AtomicInteger(1 << 30);

    public FileMetaData(long number, long fileSize, InternalKey smallest, InternalKey largest) {
        this.number = number;
        this.fileSize = fileSize;
        this.smallest = smallest;
        this.largest = largest;
    }

    public long getFileSize() {
        return fileSize;
    }

    public long getNumber() {
        return number;
    }

    public InternalKey getSmallest() {
        return smallest;
    }

    public InternalKey getLargest() {
        return largest;
    }

    public int getAllowedSeeks() {
        return allowedSeeks.get();
    }

    public void setAllowedSeeks(int allowedSeeks) {
        this.allowedSeeks.set(allowedSeeks);
    }

    public void decrementAllowedSeeks() {
        //获取原子Inter值后减1
        allowedSeeks.getAndDecrement();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("FileMetaData");
        sb.append("{number=").append(number);
        sb.append(", fileSize=").append(fileSize);
        sb.append(", smallest=").append(smallest);
        sb.append(", largest=").append(largest);
        sb.append(", allowedSeeks=").append(allowedSeeks);
        sb.append('}');
        return sb.toString();
    }
}
