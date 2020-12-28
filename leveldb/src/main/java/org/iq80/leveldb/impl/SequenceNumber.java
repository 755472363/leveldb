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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * leveldb 中的每次更新（put/delete)操作都拥有一个版本，由 SequnceNumber 来标识，整个 db 有一个
 * 全局值保存着当前使用到的 SequnceNumber。SequnceNumber 在 leveldb 有重要的地位，key 的排序，
 * compact 以及 snapshot 都依赖于它。
 * typedef uint64_t SequenceNumber;
 * 存储时，SequnceNumber 只占用 56 bits, ValueType 占用 8 bits，二者共同占用 64bits（uint64_t).
 */
public final class SequenceNumber {
    // We leave eight bits empty at the bottom so a type and sequence#
    // can be packed together into 64-bits.
    public static final long MAX_SEQUENCE_NUMBER = ((0x1L << 56) - 1);

    private SequenceNumber() {
    }

    //打包序列和值类型，
    public static long packSequenceAndValueType(long sequence, ValueType valueType) {
        checkArgument(sequence <= MAX_SEQUENCE_NUMBER, "Sequence number is greater than MAX_SEQUENCE_NUMBER");
        requireNonNull(valueType, "valueType is null");
        //存储时，SequnceNumber 只占用 56 bits, ValueType 占用 8 bits，二者共同占用 64bits（uint64_t).
        return (sequence << 8) | valueType.getPersistentId();
    }

    public static ValueType unpackValueType(long num) {
        return ValueType.getValueTypeByPersistentId((byte) num);
    }

    //解包序列号，右移8个字节，就是序列号
    public static long unpackSequenceNumber(long num) {
        return num >>> 8;
    }

    public static void main(String[] args) {
        System.out.println(MAX_SEQUENCE_NUMBER);//72057594037927935
        long unpackSequenceNumber = SequenceNumber.unpackSequenceNumber(1024L);
        System.out.println(unpackSequenceNumber);//4
        ValueType valueType = ValueType.VALUE;
        long packSequenceAndValueType = SequenceNumber.packSequenceAndValueType(1L, valueType);
        //存储时，SequnceNumber 只占用 56 bits, ValueType 占用 8 bits，二者共同占用 64bits（uint64_t).
        System.out.println(packSequenceAndValueType);//262145
    }
}
