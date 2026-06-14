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
package org.apache.hadoop.chukwa.extraction.engine;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;


public class ChukwaRecordKey implements WritableComparable<ChukwaRecordKey> {

  private String reduceType;
  private String key;

  public ChukwaRecordKey() {
  }

  public ChukwaRecordKey(final String reduceType, final String key) {
    this.reduceType = reduceType;
    this.key = key;
  }

  public String getReduceType() {
    return reduceType;
  }

  public void setReduceType(final String reduceType) {
    this.reduceType = reduceType;
  }

  public String getKey() {
    return key;
  }

  public void setKey(final String key) {
    this.key = key;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, reduceType);
    Text.writeString(out, key);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    reduceType = Text.readString(in);
    key = Text.readString(in);
  }

  @Override
  public int compareTo(final ChukwaRecordKey _rio_peer) {
    int _rio_ret = 0;
    _rio_ret = reduceType.compareTo(_rio_peer.reduceType);
    if (_rio_ret != 0)
      return _rio_ret;
    _rio_ret = key.compareTo(_rio_peer.key);
    if (_rio_ret != 0)
      return _rio_ret;
    return _rio_ret;
  }

  public boolean equals(final Object _rio_peer_) {
    if (!(_rio_peer_ instanceof ChukwaRecordKey)) {
      return false;
    }
    if (_rio_peer_ == this) {
      return true;
    }
    ChukwaRecordKey _rio_peer = (ChukwaRecordKey) _rio_peer_;
    boolean _rio_ret = false;
    _rio_ret = reduceType.equals(_rio_peer.reduceType);
    if (!_rio_ret)
      return _rio_ret;
    _rio_ret = key.equals(_rio_peer.key);
    if (!_rio_ret)
      return _rio_ret;
    return _rio_ret;
  }

  public Object clone() throws CloneNotSupportedException {
    super.clone();
    ChukwaRecordKey _rio_other = new ChukwaRecordKey();
    _rio_other.reduceType = this.reduceType;
    _rio_other.key = this.key;
    return _rio_other;
  }

  public int hashCode() {
    int _rio_result = 17;
    int _rio_ret;
    _rio_ret = reduceType.hashCode();
    _rio_result = 37 * _rio_result + _rio_ret;
    _rio_ret = key.hashCode();
    _rio_result = 37 * _rio_result + _rio_ret;
    return _rio_result;
  }

  public static String signature() {
    return "LChukwaRecordKey(ss)";
  }

  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(ChukwaRecordKey.class);
    }

    static public int slurpRaw(byte[] b, int s, int l) {
      try {
        int os = s;
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
    WritableComparator.define(ChukwaRecordKey.class,
        new Comparator());
  }
}
