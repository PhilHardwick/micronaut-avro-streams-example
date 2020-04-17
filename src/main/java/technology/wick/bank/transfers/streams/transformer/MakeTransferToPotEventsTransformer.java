package technology.wick.bank.transfers.streams.transformer;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import technology.wick.bank.transfers.streams.TransferStreamFactory;
import technology.wick.pots.PotEvent;
import technology.wick.pots.balance.PotBalance;
import technology.wick.pots.balance.PotsBalanceState;
import technology.wick.pots.transfers.MakePotTransfer;
import technology.wick.pots.transfers.PotTransferEvent;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class MakeTransferToPotEventsTransformer implements Transformer<UUID, MakePotTransfer, Iterable<KeyValue<UUID, SpecificRecord>>> {

    private KeyValueStore<UUID, PotsBalanceState> stateStore;
    private KeyValueStore<UUID, PotEvent> potStore;

    @Override
    public void init(ProcessorContext context) {
        stateStore = ((KeyValueStore) context.getStateStore(TransferStreamFactory.POT_BALANCE_STORE_NAME));
        potStore = ((KeyValueStore) context.getStateStore(TransferStreamFactory.POT_STORE_NAME));
    }

    @Override
    public Iterable<KeyValue<UUID, SpecificRecord>> transform(UUID key, MakePotTransfer value) {
        UUID accountId = value.getAccountId();
        PotsBalanceState balanceState = stateStore.get(accountId);
        UUID srcPotId = value.getSrcPotId();
        PotBalance srcPotBalance = balanceState.getPots().get(srcPotId.toString());
        PotBalance srcPotBalanceAfterTransfer = PotBalance.newBuilder(srcPotBalance).setBalance(srcPotBalance.getBalance() - value.getAmount()).build();
        UUID destPotId = value.getDestPotId();
        PotBalance destPotBalance = balanceState.getPots().get(destPotId.toString());
        PotBalance destPotBalanceAfterTransfer = PotBalance.newBuilder(destPotBalance).setBalance(destPotBalance.getBalance() + value.getAmount()).build();

        HashMap<String, PotBalance> potBalances = new HashMap<>(balanceState.getPots());
        potBalances.put(srcPotId.toString(), srcPotBalanceAfterTransfer);
        potBalances.put(destPotId.toString(), destPotBalanceAfterTransfer);
        stateStore.put(accountId, PotsBalanceState.newBuilder(balanceState).setPots(potBalances).build());

        return List.of(
                KeyValue.pair(srcPotId, PotEvent.newBuilder(potStore.get(srcPotId))
                .setBalance(srcPotBalanceAfterTransfer.getBalance())
                .build()),
                KeyValue.pair(destPotId, PotEvent.newBuilder(potStore.get(destPotId))
                .setBalance(destPotBalanceAfterTransfer.getBalance())
                .build()),
                KeyValue.pair(accountId, PotTransferEvent.newBuilder()
                .setTransferId(UUID.randomUUID())
                .setSrcPotId(srcPotId)
                .setDestPotId(destPotId)
                .setAmount(value.getAmount())
                .build()));
    }

    @Override
    public void close() {
    }

}
