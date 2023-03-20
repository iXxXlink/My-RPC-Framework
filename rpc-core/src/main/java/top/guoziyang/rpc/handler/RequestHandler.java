package top.guoziyang.rpc.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.guoziyang.rpc.entity.RpcRequest;
import top.guoziyang.rpc.entity.RpcResponse;
import top.guoziyang.rpc.enumeration.ResponseCode;
import top.guoziyang.rpc.provider.ServiceProvider;
import top.guoziyang.rpc.provider.ServiceProviderImpl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.*;

/**
 * 进行过程调用的处理器
 *
 * @author ziyang
 */
public class RequestHandler {

    private static final Logger logger = LoggerFactory.getLogger(RequestHandler.class);
    private static final ServiceProvider serviceProvider;

    private static final ThreadPoolExecutor threadPoolExecutor;
    static {
        serviceProvider = new ServiceProviderImpl();
        threadPoolExecutor = new ThreadPoolExecutor(1,Integer.MAX_VALUE,60, TimeUnit.SECONDS,new LinkedBlockingQueue<>());
    }

    public Object handle(RpcRequest rpcRequest) throws ExecutionException, InterruptedException {
        Object service = serviceProvider.getServiceProvider(rpcRequest.getInterfaceName());
        FutureTask<Object> futureTask = new FutureTask<Object>(new Callable<Object>(){
            @Override
            public Object call() throws Exception {
                return invokeTargetMethod(rpcRequest, service);
            }
        });

        threadPoolExecutor.submit(futureTask);
        return futureTask.get();
    }

    //通过反射的范式调用服务
    //这里我的优化思路是:线程池+异步非阻塞。即使用线程池去执行该业务方法，然后该netty线程（一个EventLoop负责一个任务队列）可以去处理其他新请求，等到该业务方法执行完毕，netty线程在继续执行。
    private Object invokeTargetMethod(RpcRequest rpcRequest, Object service) {
        Object result;
        try {
            Method method = service.getClass().getMethod(rpcRequest.getMethodName(), rpcRequest.getParamTypes());
            result = method.invoke(service, rpcRequest.getParameters());
            logger.info("服务:{} 成功调用方法:{}", rpcRequest.getInterfaceName(), rpcRequest.getMethodName());
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            return RpcResponse.fail(ResponseCode.METHOD_NOT_FOUND, rpcRequest.getRequestId());
        }
        return result;
    }

}
