package technology.wick.bank.transfers.streams;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import io.micronaut.context.annotation.Factory;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.state.Stores;
import technology.wick.bank.transfers.streams.transformer.CreatePotTransformer;
import technology.wick.bank.transfers.streams.transformer.MakeTransferToPotEventsTransformer;
import technology.wick.bank.transfers.streams.transformer.OverwriteByKeyProcessor;
import technology.wick.pots.CreatePot;
import technology.wick.pots.PotEvent;
import technology.wick.pots.transfers.MakePotTransfer;
import technology.wick.pots.transfers.PotTransferEvent;

import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Map;
import java.util.UUID;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

@Factory
public class TransferStreamFactory {
    public static final String POTS = "pots";
    public static final String INPUT = "pot-commands";
    public static final String POT_BALANCE_STORE_NAME = "pot-balance-state-store";
    public static final String POT_STORE_NAME = "pots";
    private static final String POT_EVENTS = "pot-events";

    @Singleton
    @Named(POTS)
    KStream<UUID, SpecificRecord> bankTransferStream(ConfiguredStreamBuilder builder) {
        setUpStateStores(builder);

        KStream<UUID, SpecificRecord> source = builder.stream(INPUT);
        KStream<UUID, SpecificRecord>[] branches = source
                .branch((key, value) -> value instanceof MakePotTransfer,
                        ((key, value) -> value instanceof CreatePot));
        branches[0].mapValues(value -> (MakePotTransfer) value)
                .flatTransform(MakeTransferToPotEventsTransformer::new, POT_BALANCE_STORE_NAME)
                .to(this::topicByValueType);
        branches[1].mapValues(value -> (CreatePot) value)
                .transform(CreatePotTransformer::new, POT_BALANCE_STORE_NAME)
                .to(POT_EVENTS);
        return source;
    }

    private String topicByValueType(UUID key, SpecificRecord value, RecordContext recordContext) {
        if (value instanceof PotEvent) {
            return POT_EVENTS;
        }
        if (value instanceof PotTransferEvent) {
            return "transfer-events";
        }
        throw new IllegalStateException("Unknown message type");

    }

    private void setUpStateStores(ConfiguredStreamBuilder builder) {
        SpecificAvroSerde<SpecificRecord> valueSerde = new SpecificAvroSerde<>();
        valueSerde.configure(Map.of(SCHEMA_REGISTRY_URL_CONFIG, builder.getConfiguration().get(SCHEMA_REGISTRY_URL_CONFIG)), false);
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(POT_BALANCE_STORE_NAME), Serdes.UUID(), valueSerde));
        builder.addGlobalStore(Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(POT_STORE_NAME), Serdes.UUID(), valueSerde), POT_EVENTS,
                Consumed.with(Serdes.UUID(), valueSerde), OverwriteByKeyProcessor::new);
    }

}
