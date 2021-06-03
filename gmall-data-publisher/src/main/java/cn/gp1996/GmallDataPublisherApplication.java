package cn.gp1996;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "cn.gp1996.gmall.publisher.mapper")
public class GmallDataPublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(GmallDataPublisherApplication.class, args);
    }

}
