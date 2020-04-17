package technology.wick.bank.transfers.streams.transformer;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import technology.wick.bank.transfers.streams.TransferStreamFactory;
import technology.wick.pots.PotEvent;

import java.util.UUID;

public class OverwriteByKeyProcessor implements Processor<UUID, PotEvent> {

    private KeyValueStore<UUID, PotEvent> stateStore;

    @Override
    public void init(ProcessorContext context) {
        stateStore = (KeyValueStore<UUID, PotEvent>) context.getStateStore(TransferStreamFactory.POT_STORE_NAME);
    }

    @Override
    public void process(UUID key, PotEvent value) {
        stateStore.put(key, value);
    }

    @Override
    public void close() {
    }
}
