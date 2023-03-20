package top.guoziyang.rpc.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

public class RedissonConfig {
    private static RedissonClient redisson;
    private static void config(){

         Config config = new Config();
         config.useClusterServers()
                .setScanInterval(2000)
                .addNodeAddress("redis://10.0.29.205:6379");

        redisson = Redisson.create(config);
    }

    public static RedissonClient getRedisson(){
        if(redisson == null){
            config();
        }
        return redisson;
    }


}
