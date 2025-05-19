// src/main/java/org/example/FraudDetection.java
package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
//import org.apache.flink.streaming.api.functions.timestamps.WatermarkStrategy;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class FraudDetection {
    public static void main(String[] args) throws Exception {
        // 1. Set up execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(5000);

        // 2. Source: read transaction strings from socket (host:port)
        DataStream<String> raw = env.socketTextStream("host.docker.internal", 6666);

        // 3. Parse and assign timestamps & watermarks
        DataStream<Transaction> txStream = raw
                .flatMap(new ParseTransaction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((tx, ts) -> tx.getTimestamp())
                );

        // 4. Key by accountId and apply fraud detection logic
        txStream
                .keyBy(Transaction::getAccountId)
                .process(new FraudDetector())
                .print();  // Sink: print alerts to console

        // 5. Execute the Flink pipeline
        env.execute("Flink Fraud Detection");
    }

    // Helper to parse raw CSV lines into Transaction objects
    public static class ParseTransaction implements FlatMapFunction<String, Transaction> {
        @Override
        public void flatMap(String line, Collector<Transaction> out) {
            try {
                String[] parts = line.split(",");
                // Expected format: id,account,amount,timestamp
                Transaction tx = new Transaction(
                        parts[0],
                        parts[1],
                        Double.parseDouble(parts[2]),
                        Long.parseLong(parts[3])
                );
                out.collect(tx);
            } catch (Exception e) {
                // Ignore bad records or log for inspection
            }
        }
    }

    // POJO representing a financial transaction
    public static class Transaction {
        private String transactionId;
        private String accountId;
        private double amount;
        private long timestamp;

        public Transaction() {}
        public Transaction(String transactionId, String accountId, double amount, long timestamp) {
            this.transactionId = transactionId;
            this.accountId = accountId;
            this.amount = amount;
            this.timestamp = timestamp;
        }
        public String getTransactionId() { return transactionId; }
        public String getAccountId() { return accountId; }
        public double getAmount() { return amount; }
        public long getTimestamp() { return timestamp; }
    }

    // Alert type emitted when suspicious activity is detected
    public static class FraudAlert {
        private String accountId;
        private String message;
        public FraudAlert() {}
        public FraudAlert(String accountId, String message) {
            this.accountId = accountId;
            this.message = message;
        }
        @Override
        public String toString() {
            return "FraudAlert{" +
                    "accountId='" + accountId + '\'' +
                    ", message='" + message + '\'' +
                    '}';
        }
    }

    // Core fraud detection logic: checks large or rapid transactions
    public static class FraudDetector extends KeyedProcessFunction<String, Transaction, FraudAlert> {
        private static final double LARGE_AMOUNT = 10000.0;
        private static final long RAPID_WINDOW_MS = 10_000L;

        // State to keep last transaction timestamp per account
        private transient ValueState<Long> lastTxTime;

//        @Override
        public void open(org.apache.flink.configuration.Configuration config) {
            ValueStateDescriptor<Long> desc = new ValueStateDescriptor<>(
                    "lastTxTime", Long.class
            );
            lastTxTime = getRuntimeContext().getState(desc);
        }

        @Override
        public void processElement(Transaction tx,
                                   Context ctx,
                                   Collector<FraudAlert> out) throws Exception {
            Long prev = lastTxTime.value();
            long now = tx.getTimestamp();

            // 1) Alert if transaction exceeds LARGE_AMOUNT
            if (tx.getAmount() >= LARGE_AMOUNT) {
                out.collect(new FraudAlert(
                        tx.getAccountId(),
                        "Large transaction: $" + tx.getAmount()
                ));
            }

            // 2) Alert if two transactions arrive within RAPID_WINDOW_MS
            if (prev != null && (now - prev) <= RAPID_WINDOW_MS) {
                out.collect(new FraudAlert(
                        tx.getAccountId(),
                        "Rapid transactions within " + (RAPID_WINDOW_MS/1000) + "s"
                ));
            }

            // Update state for this account
            lastTxTime.update(now);
        }
    }
}
