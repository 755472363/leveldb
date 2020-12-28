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

public final class DbConstants {
    public static final int MAJOR_VERSION = 0;
    public static final int MINOR_VERSION = 1;

    // todo this should be part of the configuration

    /**
     * Max number of levels
     * 最大级别数
     */
    public static final int NUM_LEVELS = 7;

    /**
     * Level-0 compaction is started when we hit this many files.
     * 当我们碰到这么多文件时，0级压缩就开始了。
     */
    public static final int L0_COMPACTION_TRIGGER = 4;

    /**
     * Soft limit on number of level-0 files.  We slow down writes at this point.
     * 软限制0级文件的数量。我们放慢了写的速度。
     */
    public static final int L0_SLOWDOWN_WRITES_TRIGGER = 8;

    /**
     * Maximum number of level-0 files.  We stop writes at this point.
     * level-0文件的最大数目。我们在这里停止写。
     */
    public static final int L0_STOP_WRITES_TRIGGER = 12;

    /**
     * Maximum level to which a new compacted memtable is pushed if it
     * does not create overlap.  We try to push to level 2 to avoid the
     * relatively expensive level 0=>1 compactions and to avoid some
     * expensive manifest file operations.  We do not push all the way to
     * the largest level since that can generate a lot of wasted disk
     * space if the same key space is being repeatedly overwritten.
     * 如果一个新的紧凑memtable没有产生重叠，那么它被压缩到最大水平。
     * 我们尝试推到level 2，以避免相对昂贵的level 0=>1压缩，并避免一些昂贵的清单文件操作。
     * 我们没有一直推到最大的级别，因为如果重复覆盖相同的键空间，这会产生大量浪费的磁盘空间。
     */
    public static final int MAX_MEM_COMPACT_LEVEL = 2;// max_mem_compact_level

    private DbConstants() {
    }
}
