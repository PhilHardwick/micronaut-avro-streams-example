package technology.wick.bank.transfers.producer;

import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.Topic;
import technology.wick.pots.CreatePot;
import technology.wick.pots.transfers.MakePotTransfer;

import java.util.UUID;

@KafkaClient
public interface TestCommandSender {

    @Topic("pot-commands")
    void sendMakePotTransfer(@KafkaKey UUID sourceAccountId, MakePotTransfer makePotTransfer);

    @Topic("pot-commands")
    void sendCreatePot(@KafkaKey UUID accountId, CreatePot createPot);

}
