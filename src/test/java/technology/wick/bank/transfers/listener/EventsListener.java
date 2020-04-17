package technology.wick.bank.transfers.listener;

import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.OffsetReset;
import io.micronaut.configuration.kafka.annotation.Topic;
import technology.wick.pots.PotEvent;
import technology.wick.pots.transfers.PotTransferEvent;

import javax.inject.Singleton;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

@Singleton
@KafkaListener(groupId = "all-event-listener", offsetReset = OffsetReset.EARLIEST, clientId = "all-event-test-listener")
public class EventsListener {

    private BlockingQueue<PotEvent> potEvents = new LinkedBlockingDeque<>();
    private BlockingQueue<PotTransferEvent> transferEvents = new LinkedBlockingDeque<>();

    @Topic("pot-events")
    public void potEventReceived(PotEvent accountEvent) {
        potEvents.add(accountEvent);
    }
    @Topic("transfer-events")
    public void transferEventReceived(PotTransferEvent transferEvent) {
        transferEvents.add(transferEvent);
    }

    public BlockingQueue<PotEvent> getPotEvents() {
        return potEvents;
    }

    public BlockingQueue<PotTransferEvent> getTransferEvents() {
        return transferEvents;
    }

}
