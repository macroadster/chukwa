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
import java.io.Serializable;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;


public class ChukwaRecordJT implements WritableComparable<ChukwaRecordJT>, Serializable {
  private static final long serialVersionUID = 15015L;

  protected long time;
  protected java.util.TreeMap<String, ByteBuffer> mapFields;

  public ChukwaRecordJT() {
  }

  public ChukwaRecordJT(
                        final long time,
                        final java.util.TreeMap<String, ByteBuffer> mapFields) {
    this.time = time;
    this.mapFields = mapFields;
  }

  public long getTime() {
    return time;
  }

  public void setTime(final long time) {
    this.time = time;
  }

  public java.util.TreeMap<String, ByteBuffer> getMapFields() {
    return mapFields;
  }

  public void setMapFields(
      final java.util.TreeMap<String, ByteBuffer> mapFields) {
    this.mapFields = mapFields;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(time);
    WritableUtils.writeVInt(out, mapFields.size());
    for (java.util.Map.Entry<String, ByteBuffer> _rio_me1 : mapFields.entrySet()) {
      Text.writeString(out, _rio_me1.getKey());
      byte[] _rio_v1 = _rio_me1.getValue().array();
      WritableUtils.writeVInt(out, _rio_v1.length);
      out.write(_rio_v1);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    time = in.readLong();
    int _rio_size = WritableUtils.readVInt(in);
    mapFields = new java.util.TreeMap<String, ByteBuffer>();
    for (int _rio_i = 0; _rio_i < _rio_size; _rio_i++) {
      String _rio_k1 = Text.readString(in);
      int _rio_len = WritableUtils.readVInt(in);
      byte[] _rio_b = new byte[_rio_len];
      in.readFully(_rio_b);
      mapFields.put(_rio_k1, ByteBuffer.wrap(_rio_b));
    }
  }

  @Override
  public int compareTo(final ChukwaRecordJT _rio_peer) {
    int _rio_ret = 0;
    _rio_ret = (time == _rio_peer.time) ? 0
        : ((time < _rio_peer.time) ? -1 : 1);
    if (_rio_ret != 0)
      return _rio_ret;
    {
      java.util.Set<String> _rio_set10 = mapFields.keySet();
      java.util.Set<String> _rio_set20 = _rio_peer.mapFields.keySet();
      java.util.Iterator<String> _rio_miter10 = _rio_set10.iterator();
      java.util.Iterator<String> _rio_miter20 = _rio_set20.iterator();
      while(_rio_miter10.hasNext() && _rio_miter20.hasNext()) {
        String _rio_k10 = _rio_miter10.next();
        String _rio_k20 = _rio_miter20.next();
        _rio_ret = _rio_k10.compareTo(_rio_k20);
        if (_rio_ret != 0) {
          return _rio_ret;
        }
      }
      _rio_ret = (_rio_set10.size() - _rio_set20.size());
    }
    if (_rio_ret != 0)
      return _rio_ret;
    return _rio_ret;
  }

  public boolean equals(final Object _rio_peer_) {
    if (!(_rio_peer_ instanceof ChukwaRecordJT)) {
      return false;
    }
    if (_rio_peer_ == this) {
      return true;
    }
    ChukwaRecordJT _rio_peer = (ChukwaRecordJT) _rio_peer_;
    boolean _rio_ret = false;
    _rio_ret = (time == _rio_peer.time);
    if (!_rio_ret)
      return _rio_ret;
    _rio_ret = mapFields.equals(_rio_peer.mapFields);
    if (!_rio_ret)
      return _rio_ret;
    return _rio_ret;
  }

  @SuppressWarnings("unchecked")
  public Object clone() throws CloneNotSupportedException {
    super.clone();
    ChukwaRecordJT _rio_other = new ChukwaRecordJT();
    _rio_other.time = this.time;
    _rio_other.mapFields = (java.util.TreeMap<String, ByteBuffer>) this.mapFields
        .clone();
    return _rio_other;
  }

  public int hashCode() {
    int _rio_result = 17;
    int _rio_ret;
    _rio_ret = (int) (time ^ (time >>> 32));
    _rio_result = 37 * _rio_result + _rio_ret;
    _rio_ret = mapFields.hashCode();
    _rio_result = 37 * _rio_result + _rio_ret;
    return _rio_result;
  }

  public static String signature() {
    return "LChukwaRecordJT(l{sB})";
  }

  public static class Comparator extends WritableComparator implements Serializable {
    public Comparator() {
      super(ChukwaRecordJT.class);
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
          int mi1 = readVInt(b, s);
          int mz1 = WritableUtils.getVIntSize(mi1);
          s += mz1;
          l -= mz1;
          for (int midx1 = 0; midx1 < mi1; midx1++) {
            {
              int i = readVInt(b, s);
              int z = WritableUtils.getVIntSize(i);
              s += (z + i);
              l -= (z + i);
            }
            {
              int i = readVInt(b, s);
              int z = WritableUtils.getVIntSize(i);
              s += z + i;
              l -= (z + i);
            }
          }
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
          int mi11 = readVInt(b1, s1);
          int mi21 = readVInt(b2, s2);
          int mz11 = WritableUtils.getVIntSize(mi11);
          int mz21 = WritableUtils.getVIntSize(mi21);
          s1 += mz11;
          s2 += mz21;
          l1 -= mz11;
          l2 -= mz21;
          for (int midx1 = 0; midx1 < mi11 && midx1 < mi21; midx1++) {
            {
              int i1 = readVInt(b1, s1);
              int i2 = readVInt(b2, s2);
              int z1 = WritableUtils.getVIntSize(i1);
              int z2 = WritableUtils.getVIntSize(i2);
              s1 += z1;
              s2 += z2;
              l1 -= z1;
              l2 -= z2;
              int r1 = WritableComparator.compareBytes(b1, s1, i1,
                  b2, s2, i2);
              if (r1 != 0) {
                return (r1 < 0) ? -1 : 0;
              }
              s1 += i1;
              s2 += i2;
              l1 -= i1;
              l1 -= i2;
            }
            {
              int i = readVInt(b1, s1);
              int z = WritableUtils.getVIntSize(i);
              s1 += z + i;
              l1 -= (z + i);
            }
            {
              int i = readVInt(b2, s2);
              int z = WritableUtils.getVIntSize(i);
              s2 += z + i;
              l2 -= (z + i);
            }
          }
          if (mi11 != mi21) {
            return (mi11 < mi21) ? -1 : 0;
          }
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
    WritableComparator.define(ChukwaRecordJT.class,
        new Comparator());
  }
}
