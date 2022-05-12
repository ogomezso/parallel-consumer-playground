package org.github.ogomezso.playground.parallelconsumer.basic;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;

@Slf4j
public class BasicParallelConsumerByKey {

    ParallelStreamProcessor<String, String> parallelConsumer;

    @SuppressWarnings("UnqualifiedFieldAccess")
    void run(Consumer<String, String> kafkaConsumer, Producer<String, String> kafkaProducer, List<String> inputTopics) {
        this.parallelConsumer = setupParallelConsumer(kafkaConsumer, kafkaProducer, inputTopics);
        parallelConsumer.poll(record -> log.info("Concurrently processing a record: {}", record));
    }

    ParallelStreamProcessor<String, String> setupParallelConsumer(Consumer<String, String> kafkaConsumer,
            Producer<String, String> kafkaProducer, List<String> inputTopics) {
        ParallelConsumerOptions<String, String> options = ParallelConsumerOptions.<String, String>builder()
                .ordering(KEY)
                .maxConcurrency(1000)
                .consumer(kafkaConsumer)
                .producer(kafkaProducer)
                .build();
        ParallelStreamProcessor<String, String> eosStreamProcessor = ParallelStreamProcessor
                .createEosStreamProcessor(options);
        eosStreamProcessor.subscribe(inputTopics);

        return eosStreamProcessor;
    }

    void close() {
        this.parallelConsumer.close();
    }

    void runPollAndProduce(Consumer<String, String> kafkaConsumer, Producer<String, String> kafkaProducer,
            List<String> inputTopics, String outputTopic) {
        this.parallelConsumer = setupParallelConsumer(kafkaConsumer, kafkaProducer, inputTopics);
        parallelConsumer.pollAndProduce(context -> {
            var record = context.getSingleRecord().getConsumerRecord();
            Result result = processBrokerRecord(record);
            return new ProducerRecord<>(outputTopic, record.key(), result.payload);
        }, consumeProduceResult -> log.debug("Message {} saved to broker at offset {}",
                consumeProduceResult.getOut(),
                consumeProduceResult.getMeta().offset()));
    }

    private Result processBrokerRecord(ConsumerRecord<String, String> record) {
        return new Result("Some payload from " + record.value());
    }

    @Value
    static class Result {
        String payload;
    }
}
