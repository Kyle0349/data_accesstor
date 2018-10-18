package com.qhcs.accesser;

import org.apache.oozie.client.OozieClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@SpringBootApplication
@Configuration
public class Application {

    public static ConfigurableApplicationContext applicationContext;


    public static void main(String[] args) {
        applicationContext =
                new SpringApplicationBuilder(Application.class).web(true).run(args);


    }

    @Value("${BASE_OOZIE_URL}")
    private String BASE_OOZIE_URL;

    @Bean
    public OozieClient getOozieClient(){
        return new OozieClient(BASE_OOZIE_URL);
    }



}
