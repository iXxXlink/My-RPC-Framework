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
        //处理连接的线程池，一般线程数为1就行。如果并发量高，可以不设置线程数，默认为2*CPU数
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        //处理IO操作的线程池
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {

            //开启netty，等待客户端请求
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workerGroup)
                    //设置为NIO模型 socket(TCP)
                    .channel(NioServerSocketChannel.class)
                    //这里的handler不负责具体业务，是负责响应事件
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .option(ChannelOption.SO_BACKLOG, 256)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    //childHandler指的是workerEventLoop中线程执行时使用的handle
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        //每建立一个连接都执行一次initChannel。因为每个连接对应一个channel
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            //每一个channel都有自己的pipeline（即handler链）
                            ChannelPipeline pipeline = ch.pipeline();
                                    //心跳
                            pipeline.addLast(new IdleStateHandler(30, 0, 0, TimeUnit.SECONDS))
                                    //经过加密object变为字节数组ByteBuf
                                    .addLast(new CommonEncoder(serializer))
                                    //解密，返回Object数据
                                    .addLast(new CommonDecoder())
                                    //每个客户端连接都是一个新的NettyServerHandler
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
