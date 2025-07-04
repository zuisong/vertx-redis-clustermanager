package com.retailsvc.vertx.spi.cluster.redis.impl.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.vertx.core.buffer.impl.BufferImpl;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;

import io.vertx.core.shareddata.ClusterSerializable;
import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;

/**
 * A Redisson codec for {@link io.vertx.core.shareddata.ClusterSerializable} shared data.
 *
 * <p><b>Note:</b> The codec need to support the deprecated {@link
 * io.vertx.core.shareddata.impl.ClusterSerializable} until it is removed from Vertx.
 *
 * @author sasjo
 */
@SuppressWarnings("deprecation")
public class ClusterSerializableCodec extends ClassLoaderCodec {

  /** Codec singleton. */
  public static final Codec INSTANCE = new ClusterSerializableCodec();

  private final Decoder<Object> decoder =
      (buf, state) -> {
        int classNameLength = buf.readInt();
        String className = buf.readCharSequence(classNameLength, StandardCharsets.UTF_8).toString();
        try {
          Object object =
              getClassLoader().loadClass(className).getDeclaredConstructor().newInstance();
          if (!(object instanceof ClusterSerializable)) {
            throw new IOException(
                className + " does not implement " + ClusterSerializable.class.getName());
          }
          ((ClusterSerializable) object).readFromBuffer(buf.readerIndex(), new BufferImpl(buf));
          return object;
        } catch (InstantiationException
            | IllegalAccessException
            | ClassNotFoundException
            | NoSuchMethodException
            | InvocationTargetException e) {
          throw new IOException("Failed to decode class " + className, e);
        }
      };

  private final Encoder encoder =
      in -> {
        if (!(in instanceof ClusterSerializable)) {
          throw new IOException("Unsupported type: " + in.getClass());
        }
        ByteBuf out = ByteBufAllocator.DEFAULT.buffer();
        String className = in.getClass().getName();
        out.writeInt(className.length());
        out.writeCharSequence(className, StandardCharsets.UTF_8);
        ((ClusterSerializable) in).writeToBuffer(new BufferImpl(out));
        return out;
      };

  /** Create a ClusterSerializableCodec. */
  public ClusterSerializableCodec() {}

  /**
   * Create a ClusterSerializableCodec.
   *
   * @param classLoader required by Codec contract
   */
  public ClusterSerializableCodec(ClassLoader classLoader) {
    super(classLoader);
  }

  /**
   * Create a ClusterSerializableCodec.
   *
   * @param classLoader required by Codec contract
   * @param codec required by Codec contract
   */
  public ClusterSerializableCodec(ClassLoader classLoader, ClusterSerializableCodec codec) {
    this(classLoader);
  }

  @Override
  public Decoder<Object> getValueDecoder() {
    return decoder;
  }

  @Override
  public Encoder getValueEncoder() {
    return encoder;
  }
}
