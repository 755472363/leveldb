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

import org.iq80.leveldb.util.Slice;

/**
 * db 内部在为查找 memtable/sstable 方便，包装使用的 key 结构，保存有 userkey 与
 * SequnceNumber/ValueType dump 在内存的数据。
 * LookupKey:
 * start                           kstart                    end
 * userkey_len(varint32)   userkey_data(userkey_len)    SequnceNumber/ValueType(uint64)
 * <p>
 * 对 memtable 进行 lookup 时使用 [start,end],
 * 对 sstable lookup 时使用[kstart, end]
 */
public class LookupKey {
    private final InternalKey key;

    public LookupKey(Slice userKey, long sequenceNumber) {
        key = new InternalKey(userKey, sequenceNumber, ValueType.VALUE);
    }

    public InternalKey getInternalKey() {
        return key;
    }

    public Slice getUserKey() {
        return key.getUserKey();
    }

    @Override
    public String toString() {
        return key.toString();
    }
}
