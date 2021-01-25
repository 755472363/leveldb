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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.iq80.leveldb.util.DynamicSliceOutput;
import org.iq80.leveldb.util.Slice;
import org.iq80.leveldb.util.SliceInput;
import org.iq80.leveldb.util.VariableLengthQuantity;

import java.util.Map;
import java.util.TreeMap;

/**
 * compact 过程中会有一系列改变当前 Version 的操作（FileNumber 增加，删除 input 的 sstable，增加
 * 输出的 sstable……），为了缩小 Version 切换的时间点，将这些操作封装成 VersionEdit，compact
 * 完成时，将 VersionEdit 中的操作一次应用到当前 Version 即可得到最新状态的 Version。
 */
public class VersionEdit {
    private final Map<Integer, InternalKey> compactPointers = new TreeMap<>();
    //要新增的FileMetaData，键是level,  值是.sst文件实体
    private final Multimap<Integer, FileMetaData> newFiles = ArrayListMultimap.create();
    //要删除的FileMetaData，键是level,  值是文件编号
    private final Multimap<Integer, Long> deletedFiles = ArrayListMultimap.create();
    private String comparatorName;
    private Long logNumber;
    private Long nextFileNumber;
    private Long previousLogNumber;
    private Long lastSequenceNumber;

    public VersionEdit() {
    }

    public VersionEdit(Slice slice) {
        SliceInput sliceInput = slice.input();
        while (sliceInput.isReadable()) {
            int i = VariableLengthQuantity.readVariableLengthInt(sliceInput);
            VersionEditTag tag = VersionEditTag.getValueTypeByPersistentId(i);
            tag.readValue(sliceInput, this);
        }
    }

    public String getComparatorName() {
        return comparatorName;
    }

    public void setComparatorName(String comparatorName) {
        this.comparatorName = comparatorName;
    }

    public Long getLogNumber() {
        return logNumber;
    }

    public void setLogNumber(long logNumber) {
        this.logNumber = logNumber;
    }

    public Long getNextFileNumber() {
        return nextFileNumber;
    }

    public void setNextFileNumber(long nextFileNumber) {
        this.nextFileNumber = nextFileNumber;
    }

    public Long getPreviousLogNumber() {
        return previousLogNumber;
    }

    public void setPreviousLogNumber(long previousLogNumber) {
        this.previousLogNumber = previousLogNumber;
    }

    public Long getLastSequenceNumber() {
        return lastSequenceNumber;
    }

    public void setLastSequenceNumber(long lastSequenceNumber) {
        this.lastSequenceNumber = lastSequenceNumber;
    }

    public Map<Integer, InternalKey> getCompactPointers() {
        return ImmutableMap.copyOf(compactPointers);
    }

    public void setCompactPointers(Map<Integer, InternalKey> compactPointers) {
        this.compactPointers.putAll(compactPointers);
    }

    public void setCompactPointer(int level, InternalKey key) {
        compactPointers.put(level, key);
    }

    public Multimap<Integer, FileMetaData> getNewFiles() {
        return ImmutableMultimap.copyOf(newFiles);
    }

    // Add the specified file at the specified level.
    // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
    // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
    //在指定的级别添加指定的文件。
    //要求:这个版本没有被保存(参见VersionSet::SaveTo)
    //要求:“最小”和“最大”是文件中最小和最大的键
    public void addFile(int level, long fileNumber,
                        long fileSize,
                        InternalKey smallest,
                        InternalKey largest) {
        FileMetaData fileMetaData = new FileMetaData(fileNumber, fileSize, smallest, largest);
        addFile(level, fileMetaData);
    }

    public void addFile(int level, FileMetaData fileMetaData) {
        newFiles.put(level, fileMetaData);
    }

    public void addFiles(Multimap<Integer, FileMetaData> files) {
        newFiles.putAll(files);
    }

    public Multimap<Integer, Long> getDeletedFiles() {
        return ImmutableMultimap.copyOf(deletedFiles);
    }

    // Delete the specified "file" from the specified "level".
    public void deleteFile(int level, long fileNumber) {
        deletedFiles.put(level, fileNumber);
    }

    public Slice encode() {
        DynamicSliceOutput dynamicSliceOutput = new DynamicSliceOutput(4096);
        for (VersionEditTag versionEditTag : VersionEditTag.values()) {
            versionEditTag.writeValue(dynamicSliceOutput, this);
        }
        return dynamicSliceOutput.slice();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("VersionEdit");
        sb.append("{comparatorName='").append(comparatorName).append('\'');
        sb.append(", logNumber=").append(logNumber);
        sb.append(", previousLogNumber=").append(previousLogNumber);
        sb.append(", lastSequenceNumber=").append(lastSequenceNumber);
        sb.append(", compactPointers=").append(compactPointers);
        sb.append(", newFiles=").append(newFiles);
        sb.append(", deletedFiles=").append(deletedFiles);
        sb.append('}');
        return sb.toString();
    }
}
