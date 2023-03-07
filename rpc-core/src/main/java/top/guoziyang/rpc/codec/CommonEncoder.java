package top.guoziyang.rpc.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import top.guoziyang.rpc.entity.RpcRequest;
import top.guoziyang.rpc.enumeration.PackageType;
import top.guoziyang.rpc.serializer.CommonSerializer;

/**
 * 通用的编码拦截器
 *
 * @author ziyang
 */

public class CommonEncoder extends MessageToByteEncoder {

    private static final int MAGIC_NUMBER = 0xCAFEBABE;

    private final CommonSerializer serializer;

    public CommonEncoder(CommonSerializer serializer) {
        this.serializer = serializer;
    }

    //自定义的应用层协议
    //ByteBuf字节缓冲区可能包含多个数据包，所有数据包都是头尾相连，所以需要自定义协议进行解析。
    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
        //添加魔数
        out.writeInt(MAGIC_NUMBER);
        //添加包类型
        if (msg instanceof RpcRequest) {
            out.writeInt(PackageType.REQUEST_PACK.getCode());
        } else {
            out.writeInt(PackageType.RESPONSE_PACK.getCode());
        }
        //添加序列化号码
        out.writeInt(serializer.getCode());

        byte[] bytes = serializer.serialize(msg);
        //添加包总大小
        out.writeInt(bytes.length);
        //添加序列化后的msg
        out.writeBytes(bytes);
    }

}
