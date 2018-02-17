package tech.debs17gc.system.serialization;

import java.io.IOException;

import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

public class ByteSchema implements DeserializationSchema<byte[]>, SerializationSchema<byte[]> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4948992370685731123L;

	@Override
	public TypeInformation<byte[]> getProducedType() {
		return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
	}

	@Override
	public byte[] deserialize(byte[] message) throws IOException {
		return message;
	}

	@Override
	public boolean isEndOfStream(byte[] nextElement) {
		return false;
	}

	@Override
	public byte[] serialize(byte[] element) {
		return element;
	}
}