package technology.wick.bank.transfers;

import io.micronaut.test.annotation.MicronautTest;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import technology.wick.bank.transfers.listener.EventsListener;
import technology.wick.bank.transfers.producer.TestCommandSender;
import technology.wick.pots.CreatePot;
import technology.wick.pots.PotEvent;
import technology.wick.pots.transfers.MakePotTransfer;
import technology.wick.pots.transfers.PotTransferEvent;

import javax.inject.Inject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@MicronautTest(environments = "kafka")
public class TestTransferListener {

    @Inject
    private TestCommandSender testCommandSender;
    @Inject
    private EventsListener eventsListener;
    @Inject
    private KafkaStreams stream;

    @BeforeEach
    void setUp() {
        await().atMost(10, TimeUnit.SECONDS).until(() -> stream.state().equals(KafkaStreams.State.RUNNING));
    }

    @Test
    void sendsATransferEvent() throws InterruptedException {
        UUID accountId = UUID.randomUUID();
        testCommandSender.sendCreatePot(accountId, CreatePot.newBuilder()
                .setAccountId(accountId)
                .setName("Pot 1")
                .build());
        PotEvent potEvent = eventsListener.getPotEvents().poll(5, TimeUnit.SECONDS);
        assertThat(potEvent).isNotNull();
        assertThat(potEvent.getAccountId()).isEqualTo(accountId);
        assertThat(potEvent.getName()).isEqualTo("Pot 1");
        assertThat(potEvent.getBalance()).isEqualTo(0L);
        assertThat(potEvent.getCreatedAt()).isNotNull();
        UUID srcPotId = potEvent.getPotId();
        Instant srcPotCreatedAt = potEvent.getCreatedAt();

        testCommandSender.sendCreatePot(accountId, CreatePot.newBuilder()
                .setAccountId(accountId)
                .setName("Pot 2")
                .build());
        PotEvent potEvent2 = eventsListener.getPotEvents().poll(5, TimeUnit.SECONDS);
        assertThat(potEvent2).isNotNull();
        assertThat(potEvent2.getAccountId()).isEqualTo(accountId);
        assertThat(potEvent2.getName()).isEqualTo("Pot 2");
        assertThat(potEvent2.getBalance()).isEqualTo(0L);
        assertThat(potEvent2.getCreatedAt()).isNotNull();
        UUID destPotId = potEvent2.getPotId();
        Instant destPotCreatedAt = potEvent2.getCreatedAt();

        testCommandSender.sendMakePotTransfer(srcPotId, MakePotTransfer.newBuilder()
                .setAccountId(accountId)
                .setDestPotId(destPotId)
                .setSrcPotId(srcPotId)
                .setAmount(100)
                .build());


        await().atMost(5, TimeUnit.SECONDS).until(() -> eventsListener.getPotEvents().size() == 2);
        List<PotEvent> potEvents = new ArrayList<>();
        eventsListener.getPotEvents().drainTo(potEvents);

        assertThat(potEvents).containsExactlyInAnyOrder(
                PotEvent.newBuilder().setPotId(srcPotId).setAccountId(accountId).setName("Pot 1")
                .setBalance(-100).setCreatedAt(srcPotCreatedAt).build(),
                PotEvent.newBuilder().setPotId(destPotId).setAccountId(accountId).setName("Pot 2")
                .setBalance(100).setCreatedAt(destPotCreatedAt).build());

        PotTransferEvent transferEvent = eventsListener.getTransferEvents().poll(5, TimeUnit.SECONDS);
        assertThat(transferEvent).isNotNull();
        assertThat(transferEvent.getAmount()).isEqualTo(100);
        assertThat(transferEvent.getSrcPotId()).isEqualTo(srcPotId);
        assertThat(transferEvent.getDestPotId()).isEqualTo(destPotId);
        assertThat(transferEvent.getTransferId()).isNotNull();
    }
}
