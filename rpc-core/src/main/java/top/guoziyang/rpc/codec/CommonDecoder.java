package top.guoziyang.rpc.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.guoziyang.rpc.entity.RpcRequest;
import top.guoziyang.rpc.entity.RpcResponse;
import top.guoziyang.rpc.enumeration.PackageType;
import top.guoziyang.rpc.enumeration.RpcError;
import top.guoziyang.rpc.exception.RpcException;
import top.guoziyang.rpc.serializer.CommonSerializer;

import java.util.List;

/**
 * 通用的解码拦截器
 *
 * @author ziyang
 */
//父类是ReplayingDecoder，所以不需要考虑ByteBuf中的数据是否准备好了，而可以直接read
public class CommonDecoder extends ReplayingDecoder {

    private static final Logger logger = LoggerFactory.getLogger(CommonDecoder.class);
    private static final int MAGIC_NUMBER = 0xCAFEBABE;

    //byteBuf中数据可能未准备好，所以其不是一个固定大小的数据，而是将netty缓冲区引用传入了
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        //获得魔数
        //注意：如果不是ReplayingDecoder的子类，不能直接读数据，而是要先判断ByteBuf中剩余大小是否满足读的字节数量
        int magic = in.readInt();
        if (magic != MAGIC_NUMBER) {
            logger.error("不识别的协议包: {}", magic);
            throw new RpcException(RpcError.UNKNOWN_PROTOCOL);
        }
        //获得数据包类型码
        int packageCode = in.readInt();
        Class<?> packageClass;
        if (packageCode == PackageType.REQUEST_PACK.getCode()) {
            packageClass = RpcRequest.class;
        } else if (packageCode == PackageType.RESPONSE_PACK.getCode()) {
            packageClass = RpcResponse.class;
        } else {
            logger.error("不识别的数据包: {}", packageCode);
            throw new RpcException(RpcError.UNKNOWN_PACKAGE_TYPE);
        }
        //获得序列器码
        int serializerCode = in.readInt();
        CommonSerializer serializer = CommonSerializer.getByCode(serializerCode);
        if (serializer == null) {
            logger.error("不识别的反序列化器: {}", serializerCode);
            throw new RpcException(RpcError.UNKNOWN_SERIALIZER);
        }
        //获得data大小
        int length = in.readInt();
        //bytes存储字节数组
        byte[] bytes = new byte[length];
        in.readBytes(bytes);
        //将字节数组反序列化为object
        //根据包类型获得实际对象类型，进行辅助反序列化
        Object obj = serializer.deserialize(bytes, packageClass);
        out.add(obj);
    }

}
