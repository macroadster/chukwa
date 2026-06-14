/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.chukwa;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;


public class ChukwaArchiveKey implements WritableComparable<ChukwaArchiveKey> {

  private long timePartition;
  private String dataType;
  private String streamName;
  private long seqId;

  public ChukwaArchiveKey() {
  }

  public ChukwaArchiveKey(final long timePartition, final String dataType,
                          final String streamName, final long seqId) {
    this.timePartition = timePartition;
    this.dataType = dataType;
    this.streamName = streamName;
    this.seqId = seqId;
  }

  public long getTimePartition() {
    return timePartition;
  }

  public void setTimePartition(final long timePartition) {
    this.timePartition = timePartition;
  }

  public String getDataType() {
    return dataType;
  }

  public void setDataType(final String dataType) {
    this.dataType = dataType;
  }

  public String getStreamName() {
    return streamName;
  }

  public void setStreamName(final String streamName) {
    this.streamName = streamName;
  }

  public long getSeqId() {
    return seqId;
  }

  public void setSeqId(final long seqId) {
    this.seqId = seqId;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(timePartition);
    Text.writeString(out, dataType);
    Text.writeString(out, streamName);
    out.writeLong(seqId);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    timePartition = in.readLong();
    dataType = Text.readString(in);
    streamName = Text.readString(in);
    seqId = in.readLong();
  }

  @Override
  public int compareTo(final ChukwaArchiveKey _rio_peer) {
    int _rio_ret = 0;
    _rio_ret = (timePartition == _rio_peer.timePartition) ? 0
        : ((timePartition < _rio_peer.timePartition) ? -1 : 1);
    if (_rio_ret != 0)
      return _rio_ret;
    _rio_ret = dataType.compareTo(_rio_peer.dataType);
    if (_rio_ret != 0)
      return _rio_ret;
    _rio_ret = streamName.compareTo(_rio_peer.streamName);
    if (_rio_ret != 0)
      return _rio_ret;
    _rio_ret = (seqId == _rio_peer.seqId) ? 0 : ((seqId < _rio_peer.seqId) ? -1
        : 1);
    if (_rio_ret != 0)
      return _rio_ret;
    return _rio_ret;
  }

  public boolean equals(final Object _rio_peer_) {
    if (!(_rio_peer_ instanceof ChukwaArchiveKey)) {
      return false;
    }
    if (_rio_peer_ == this) {
      return true;
    }
    ChukwaArchiveKey _rio_peer = (ChukwaArchiveKey) _rio_peer_;
    boolean _rio_ret = false;
    _rio_ret = (timePartition == _rio_peer.timePartition);
    if (!_rio_ret)
      return _rio_ret;
    _rio_ret = dataType.equals(_rio_peer.dataType);
    if (!_rio_ret)
      return _rio_ret;
    _rio_ret = streamName.equals(_rio_peer.streamName);
    if (!_rio_ret)
      return _rio_ret;
    _rio_ret = (seqId == _rio_peer.seqId);
    if (!_rio_ret)
      return _rio_ret;
    return _rio_ret;
  }

  public Object clone() throws CloneNotSupportedException {
    super.clone();
    ChukwaArchiveKey _rio_other = new ChukwaArchiveKey();
    _rio_other.timePartition = this.timePartition;
    _rio_other.dataType = this.dataType;
    _rio_other.streamName = this.streamName;
    _rio_other.seqId = this.seqId;
    return _rio_other;
  }

  public int hashCode() {
    int _rio_result = 17;
    int _rio_ret;
    _rio_ret = (int) (timePartition ^ (timePartition >>> 32));
    _rio_result = 37 * _rio_result + _rio_ret;
    _rio_ret = dataType.hashCode();
    _rio_result = 37 * _rio_result + _rio_ret;
    _rio_ret = streamName.hashCode();
    _rio_result = 37 * _rio_result + _rio_ret;
    _rio_ret = (int) (seqId ^ (seqId >>> 32));
    _rio_result = 37 * _rio_result + _rio_ret;
    return _rio_result;
  }

  public static String signature() {
    return "LChukwaArchiveKey(lssl)";
  }

  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(ChukwaArchiveKey.class);
    }

    static public int slurpRaw(byte[] b, int s, int l) {
      try {
        int os = s;
        {
          long i = readVLong(b, s);
          int z = WritableUtils.getVIntSize(i);
          s += z;
          l -= z;
        }
        {
          int i = readVInt(b, s);
          int z = WritableUtils.getVIntSize(i);
          s += (z + i);
          l -= (z + i);
        }
        {
          int i = readVInt(b, s);
          int z = WritableUtils.getVIntSize(i);
          s += (z + i);
          l -= (z + i);
        }
        {
          long i = readVLong(b, s);
          int z = WritableUtils.getVIntSize(i);
          s += z;
          l -= z;
        }
        return (os - s);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    static public int compareRaw(byte[] b1, int s1, int l1, byte[] b2, int s2,
        int l2) {
      try {
        int os1 = s1;
        {
          long i1 = readVLong(b1, s1);
          long i2 = readVLong(b2, s2);
          if (i1 != i2) {
            return ((i1 - i2) < 0) ? -1 : 0;
          }
          int z1 = WritableUtils.getVIntSize(i1);
          int z2 = WritableUtils.getVIntSize(i2);
          s1 += z1;
          s2 += z2;
          l1 -= z1;
          l2 -= z2;
        }
        {
          int i1 = readVInt(b1, s1);
          int i2 = readVInt(b2, s2);
          int z1 = WritableUtils.getVIntSize(i1);
          int z2 = WritableUtils.getVIntSize(i2);
          s1 += z1;
          s2 += z2;
          l1 -= z1;
          l2 -= z2;
          int r1 = WritableComparator.compareBytes(b1, s1, i1, b2,
              s2, i2);
          if (r1 != 0) {
            return (r1 < 0) ? -1 : 0;
          }
          s1 += i1;
          s2 += i2;
          l1 -= i1;
          l1 -= i2;
        }
        {
          int i1 = readVInt(b1, s1);
          int i2 = readVInt(b2, s2);
          int z1 = WritableUtils.getVIntSize(i1);
          int z2 = WritableUtils.getVIntSize(i2);
          s1 += z1;
          s2 += z2;
          l1 -= z1;
          l2 -= z2;
          int r1 = WritableComparator.compareBytes(b1, s1, i1, b2,
              s2, i2);
          if (r1 != 0) {
            return (r1 < 0) ? -1 : 0;
          }
          s1 += i1;
          s2 += i2;
          l1 -= i1;
          l1 -= i2;
        }
        {
          long i1 = readVLong(b1, s1);
          long i2 = readVLong(b2, s2);
          if (i1 != i2) {
            return ((i1 - i2) < 0) ? -1 : 0;
          }
          int z1 = WritableUtils.getVIntSize(i1);
          int z2 = WritableUtils.getVIntSize(i2);
          s1 += z1;
          s2 += z2;
          l1 -= z1;
          l2 -= z2;
        }
        return (os1 - s1);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      int ret = compareRaw(b1, s1, l1, b2, s2, l2);
      return (ret == -1) ? -1 : ((ret == 0) ? 1 : 0);
    }
  }

  static {
    WritableComparator.define(ChukwaArchiveKey.class,
        new Comparator());
  }
}
