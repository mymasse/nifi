package com.mymasse.nifi;

import org.apache.nifi.spring.SpringNiFiConstants;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.channel.MessageChannels;
import org.springframework.messaging.MessageChannel;

@SpringBootApplication
@EnableIntegration
public class NifiBootApplication {

  public static void main(String[] args) {
    SpringApplication.run(NifiBootApplication.class, args);
  }

  @Bean(name = SpringNiFiConstants.FROM_NIFI)
  public MessageChannel fromNiFi() {
    return MessageChannels.direct().get();
  }

  @Bean(name = SpringNiFiConstants.TO_NIFI)
  public MessageChannel toNiFi() {
    return MessageChannels.queue().get();
  }

  @Bean
  public IntegrationFlow mainFlow() {
    return IntegrationFlows.from(fromNiFi()).enrichHeaders(h -> h.header("NiFiBoot", "Yes it works")).channel(toNiFi())
        .get();
  }
}
