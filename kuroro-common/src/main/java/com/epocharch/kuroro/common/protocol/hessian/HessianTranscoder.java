/*
 * Copyright 2017 EpochArch.com
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

package com.epocharch.kuroro.common.protocol.hessian;

import com.epocharch.kuroro.common.hessian.io.Deflation;
import com.epocharch.kuroro.common.hessian.io.Hessian2Input;
import com.epocharch.kuroro.common.hessian.io.Hessian2Output;
import com.epocharch.kuroro.common.hessian.io.HessianEnvelope;
import com.epocharch.kuroro.common.hessian.io.HessianInput;
import com.epocharch.kuroro.common.hessian.io.HessianOutput;
import com.epocharch.kuroro.common.hessian.io.SerializerFactory;
import com.epocharch.kuroro.common.inner.util.TranscoderUtils;
import com.epocharch.kuroro.common.protocol.CachedData;
import com.epocharch.kuroro.common.protocol.Transcoder;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HessianTranscoder implements Transcoder {

  public static final int SERIALIZED = 1;
  public static final int COMPRESSED = 2;
  public static final int SPECIAL_MASK = 0xff00;
  public static final int SPECIAL_BOOLEAN = (1 << 8);
  public static final int SPECIAL_INT = (2 << 8);
  public static final int SPECIAL_LONG = (3 << 8);
  public static final int SPECIAL_DATE = (4 << 8);
  public static final int SPECIAL_BYTE = (5 << 8);
  public static final int SPECIAL_FLOAT = (6 << 8);
  public static final int SPECIAL_DOUBLE = (7 << 8);
  public static final int SPECIAL_BYTEARRAY = (8 << 8);
  public static final int COMPRESSIONTHRESHOLD = 1024 * 1024;
  private static final Logger LOG = LoggerFactory.getLogger(HessianTranscoder.class);
  private static Transcoder transcoder = null;
  private SerializerFactory factory;
  private int compressionThreshold;

  private HessianTranscoder() {

  }

  public static Transcoder getTranscoder() {
    if (transcoder == null) {
      transcoder = new HessianTranscoder();
    }
    return transcoder;

  }

  public void setCompressionThreshold(int compressionThreshold) {
    if (compressionThreshold <= 0) {
      this.compressionThreshold = COMPRESSIONTHRESHOLD;
      LOG.warn(
          "invalid compressionThreshold, use default value: " + this.compressionThreshold + ".");
      return;
    }
    this.compressionThreshold = compressionThreshold * 1024;
  }

  @Override
  public CachedData encode(Object o) {
    byte[] b = null;
    int flags = 0;
    if (o instanceof String) {
      b = TranscoderUtils.encodeString(o.toString());
    } else if (o instanceof Long) {
      b = TranscoderUtils.encodeString(o.toString());
      flags |= SPECIAL_LONG;
    } else if (o instanceof Integer) {
      b = TranscoderUtils.encodeString(o.toString());
      flags |= SPECIAL_INT;
    } else if (o instanceof Boolean) {
      b = TranscoderUtils.encodeString(o.toString());
      flags |= SPECIAL_BOOLEAN;
    } else if (o instanceof Date) {
      Long d = ((Date) o).getTime();
      b = TranscoderUtils.encodeString(d.toString());
      flags |= SPECIAL_DATE;
    } else if (o instanceof Byte) {
      b = TranscoderUtils.encodeString(o.toString());
      flags |= SPECIAL_BYTE;
    } else if (o instanceof Float) {
      b = TranscoderUtils.encodeString(o.toString());
      flags |= SPECIAL_FLOAT;
    } else if (o instanceof Double) {
      b = TranscoderUtils.encodeString(o.toString());
      flags |= SPECIAL_DOUBLE;
    } else if (o instanceof byte[]) {
      b = (byte[]) o;
      flags |= SPECIAL_BYTEARRAY;
    } else {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      HessianOutput out = new HessianOutput(bos);
      try {
        out.writeObject(o);
        out.flush();
        out.close();
      } catch (IOException e) {
        if (LOG.isErrorEnabled()) {
          LOG.error(
              "{msg:'Hessian Encode " + o.getClass().getName() + " Error:" + e.getMessage() + "'}");
        }
      }
      b = bos.toByteArray();
      flags |= SERIALIZED;
    }
    assert b != null;
    if ((flags & SERIALIZED) == 0) {
      flags = 0;
    }
    if (b.length > this.compressionThreshold) {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      Hessian2Output out = new Hessian2Output(bos);
      HessianEnvelope envelope = new Deflation();
      try {
        out = envelope.wrap(out);
        out.writeObject(o);
        out.flush();
        out.close();
      } catch (IOException e) {
        if (LOG.isErrorEnabled()) {
          LOG.error("{msg:'Hessian Encode Compressed " + o.getClass().getName() + " Error:" + e
              .getMessage() + "'}");
        }
      }
      // 包装为压缩
      byte[] compressed = bos.toByteArray();
      if (compressed.length < b.length) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("{msg:'Compressed " + o.getClass().getName() + " from " + b.length + " to "
              + compressed.length + "'}");
        }
        b = compressed;
        flags |= COMPRESSED;
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("{msg:'Compression increased the size of " + o.getClass().getName() + " from "
              + b.length + " to "
              + compressed.length + "'}");
        }
      }
    }
    return new CachedData(b, flags);
  }

  @Override
  public Object decode(CachedData d) {
    byte[] data = d.getData();
    Object result = null;
    int flags = d.getFlag();
    flags = flags & SPECIAL_MASK;
    if ((d.getFlag() & COMPRESSED) != 0) {
      ByteArrayInputStream bin = new ByteArrayInputStream(data);
      Hessian2Input input = new Hessian2Input(bin);
      HessianEnvelope envelope = new Deflation();
      try {
        input = envelope.unwrap(input);
        result = input.readObject();
      } catch (IOException e) {
        if (LOG.isErrorEnabled()) {
          LOG.error("{msg:'Hessian Decode Compressed Error:" + e.getMessage() + "'}");
        }
      } finally {
        try {
          input.close();
          bin.close();
        } catch (IOException e) {
          LOG.error(e.getMessage(), e);
        }

      }// 解压缩
    } else if ((d.getFlag() & SERIALIZED) != 0 && data != null) {
      HessianInput input = new HessianInput(new ByteArrayInputStream(data));
      try {
        result = input.readObject();

      } catch (IOException e) {
        if (LOG.isErrorEnabled()) {
          LOG.error("{msg:'Hessian Decode Error:" + e.getMessage() + "'}");
        }
      } finally {
        input.close();
      }
    } else {

      if (flags == 0) {
        return TranscoderUtils.decodeString(data);
      }

      if (flags != 0 && data != null) {
        switch (flags) {
          case SPECIAL_BOOLEAN:
            result = Boolean.valueOf(TranscoderUtils.decodeString(data));
            break;
          case SPECIAL_INT:
            result = Integer.valueOf(TranscoderUtils.decodeString(data));
            break;
          case SPECIAL_LONG:
            result = Long.valueOf(TranscoderUtils.decodeString(data));
            break;
          case SPECIAL_BYTE:
            result = Byte.valueOf(TranscoderUtils.decodeString(data));
            break;
          case SPECIAL_FLOAT:
            result = Float.valueOf(TranscoderUtils.decodeString(data));
            break;
          case SPECIAL_DOUBLE:
            result = Double.valueOf(TranscoderUtils.decodeString(data));
            break;
          case SPECIAL_DATE:
            result = new Date(Long.valueOf(TranscoderUtils.decodeString(data)));
            break;
          case SPECIAL_BYTEARRAY:
            result = data;
            break;
          default:
            LOG.warn(String.format("Undecodeable with flags %x", flags));
        }
      } else {
        result = TranscoderUtils.decodeString(data);
      }
    }
    return result;
  }

  public SerializerFactory getSerializerFactory() {
    if (null == factory) {
      factory = new SerializerFactory();
    }

    return factory;

  }

}
