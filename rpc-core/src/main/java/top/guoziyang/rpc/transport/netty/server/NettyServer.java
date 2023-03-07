package top.guoziyang.rpc.transport.netty.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;
import top.guoziyang.rpc.codec.CommonDecoder;
import top.guoziyang.rpc.codec.CommonEncoder;
import top.guoziyang.rpc.hook.ShutdownHook;
import top.guoziyang.rpc.provider.ServiceProviderImpl;
import top.guoziyang.rpc.registry.NacosServiceRegistry;
import top.guoziyang.rpc.serializer.CommonSerializer;
import top.guoziyang.rpc.transport.AbstractRpcServer;

import java.util.concurrent.TimeUnit;

/**
 * NIO方式服务提供侧
 *
 * @author ziyang
 */
public class NettyServer extends AbstractRpcServer {

    private final CommonSerializer serializer;

    public NettyServer(String host, int port) {
        this(host, port, DEFAULT_SERIALIZER);
    }

    public NettyServer(String host, int port, Integer serializer) {
        this.host = host;
        this.port = port;
        serviceRegistry = new NacosServiceRegistry();
        serviceProvider = new ServiceProviderImpl();
        this.serializer = CommonSerializer.getByCode(serializer);
        scanServices();
    }

    //开启netty服务器
    @Override
    public void start() {
        //执行钩子函数
        ShutdownHook.getShutdownHook().addClearAllHook();
        //处理连接的线程池
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        //处理IO操作的线程池
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {

            //开启netty，等待客户端请求
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workerGroup)
                    //NIO socket(TCP)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .option(ChannelOption.SO_BACKLOG, 256)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline pipeline = ch.pipeline();
                            //心跳
                            pipeline.addLast(new IdleStateHandler(30, 0, 0, TimeUnit.SECONDS))
                                    //经过加密object变为字节数组ByteBuf
                                    .addLast(new CommonEncoder(serializer))
                                    //解密，返回Object数据
                                    .addLast(new CommonDecoder())
                                    .addLast(new NettyServerHandler());
                        }
                    });
            //服务器绑定ip + port，并异步启动netty服务器（非main线程执行netty服务器，而是bossGroup、workerGroup中线程负责）
            ChannelFuture future = serverBootstrap.bind(host, port).sync();
            //使main线程阻塞，防止执行finally中语句（如果没有这一步，子线程开启netty服务器，main线程执行finally中语句关闭资源，导致netty也被关闭）
            //目的是为了优雅关闭netty服务器，否则可以不需要该语句，同时吧finally中语句删了也能实现。
            future.channel().closeFuture().sync();

        } catch (InterruptedException e) {
            logger.error("启动服务器时有错误发生: ", e);
        } finally {
            //关闭netty服务器
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

}
