package technology.wick.bank.transfers.streams.transformer;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import technology.wick.bank.transfers.streams.TransferStreamFactory;
import technology.wick.pots.CreatePot;
import technology.wick.pots.PotEvent;
import technology.wick.pots.balance.PotBalance;
import technology.wick.pots.balance.PotsBalanceState;

import java.time.Instant;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;

public class CreatePotTransformer implements Transformer<UUID, CreatePot, KeyValue<UUID, PotEvent>> {

    private KeyValueStore<UUID, PotsBalanceState> stateStore;

    @Override
    public void init(ProcessorContext context) {
        stateStore = ((KeyValueStore) context.getStateStore(TransferStreamFactory.POT_BALANCE_STORE_NAME));
    }

    @Override
    public KeyValue<UUID, PotEvent> transform(UUID accountId, CreatePot value) {
        UUID potId = UUID.randomUUID();
        PotsBalanceState potsBalanceState = Optional.ofNullable(stateStore.get(value.getAccountId())).orElse(
                PotsBalanceState.newBuilder()
                        .setAccountId(value.getAccountId())
                        .setPots(new HashMap<>())
                .build());
        potsBalanceState.getPots().put(potId.toString(), PotBalance.newBuilder().setPotId(potId).setBalance(0).build());
        stateStore.put(value.getAccountId(), potsBalanceState);
        return KeyValue.pair(potId, PotEvent.newBuilder()
                .setAccountId(value.getAccountId())
                .setPotId(potId)
                .setName(value.getName())
                .setCreatedAt(Instant.now())
                .setBalance(0L)
                .build());
    }

    @Override
    public void close() {
    }

}
