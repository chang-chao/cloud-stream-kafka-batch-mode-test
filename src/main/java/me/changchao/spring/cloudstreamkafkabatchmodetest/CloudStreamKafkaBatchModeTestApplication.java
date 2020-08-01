package me.changchao.spring.cloudstreamkafkabatchmodetest;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

import javax.annotation.PreDestroy;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

// https://github.com/spring-cloud/spring-cloud-stream-binder-kafka/issues/892
@SpringBootApplication
@Slf4j
public class CloudStreamKafkaBatchModeTestApplication {
  @Autowired
  @Qualifier("consumerExecutorService")
  ExecutorService consumerExecutorService;

  @Bean
  ExecutorService consumerExecutorService(
      @Value(
              "${spring.cloud.stream.kafka.bindings.input-in-0.consumer.configuration.max.poll.records}")
          int concurrency) {
    ExecutorService executorService = Executors.newFixedThreadPool(concurrency);
    return executorService;
  }

  @PreDestroy
  public void destroyConsumerExecutorService() {
    log.info("destroyConsumerExecutorService starts");
    try {
      consumerExecutorService.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      log.error("interrupted in consumerExecutorService.awaitTermination", e);
    }
  }

  @Bean
  Consumer<List<String>> input() {

    return list -> {
      log.info("input list {}", list);
      List<Callable<Void>> tasks =
          list.stream().map(this::convertStringToTask).collect(Collectors.toList());

      try {
        log.info("start parallel processing");
        consumerExecutorService.invokeAll(tasks);
        log.info("end parallel processing");
      } catch (InterruptedException e) {
        log.error("error in processing message", e);
      }
    };
  }

  Callable<Void> convertStringToTask(String s) {
    return () -> {
      log.info("processing {}", s);
      Thread.sleep((long) (Math.random() * TimeUnit.MILLISECONDS.toMillis(500)));
      return null;
    };
  }

  @Bean
  public ApplicationRunner kafkaMsgSender(KafkaTemplate<byte[], byte[]> template) {
    return args -> {
      for (int i = 0; i < 1000; i++) {
        template.send("test-topic", ("test-msg-" + i).getBytes());
      }
    };
  }

  public static void main(String[] args) {
    SpringApplication.run(CloudStreamKafkaBatchModeTestApplication.class, args);
  }
}
