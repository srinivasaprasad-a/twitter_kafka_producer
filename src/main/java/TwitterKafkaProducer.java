import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import kafka.utils.ZKStringSerializer$;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import com.google.common.collect.Lists;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;


public class TwitterKafkaProducer {

    private static final String topic = "twitter-topic";
    private static Client client = null;
    private static Producer<String, String> producer = null;
    private static String zookeeperConnect = "zookeeper:2181";
    private static int sessionTimeoutMs = 10 * 1000;
    private static int connectionTimeoutMs = 8 * 1000;


    public static void run(String consumerKey, String consumerSecret,
                           String accesstoken, String accesssecret) throws InterruptedException {

        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka:9092, kafka_1:9094");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, String>(props);
        /*for(int i = 0; i < 100; i++)
            producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i), Integer.toString(i)));
        producer.close();*/

        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(Lists.newArrayList("disney", "magickingdom"));

        Authentication auth = new OAuth1(consumerKey, consumerSecret, accesstoken, accesssecret);
        System.out.println("Authenticated!");
        // Authentication auth = new BasicAuth(username, password);

        // Create a new BasicClient. By default gzip is enabled.
        client = new ClientBuilder().hosts(Constants.STREAM_HOST)
                .endpoint(endpoint).authentication(auth)
                .processor(new StringDelimitedProcessor(queue)).build();

        // Establish a connection
        client.connect();
        System.out.println("Connected!");

        // Check if topic exists
        ZkClient zkClient = new ZkClient(
                zookeeperConnect,
                sessionTimeoutMs,
                connectionTimeoutMs,
                ZKStringSerializer$.MODULE$);
        ZkConnection zkConnection = new ZkConnection(zookeeperConnect);

        boolean isSecureKafkaCluster = false;
        ZkUtils zkUtils = null;
        int no_of_brokers = 0;
        do {
            System.out.println("Looking for available brokers, of at least 2!");
            Thread.sleep(30000);
            zkUtils = new ZkUtils(zkClient, zkConnection, isSecureKafkaCluster);
            no_of_brokers = zkUtils.getAllBrokersInCluster().size();
            System.out.println("Number of available brokers: " + no_of_brokers);
        } while (no_of_brokers < 2);

        int partitions = 1;
        int replications = 1;
        Properties topicConfig = new Properties();
        if (!AdminUtils.topicExists(zkUtils, topic)) {
            AdminUtils.createTopic(zkUtils, topic, partitions, replications, topicConfig, RackAwareMode.Disabled$.MODULE$);
            System.out.println(topic + " - Topic created!");
        } else {
            System.out.println(topic + " - Topic already exists!");
        }

        // Push messages to topic
        for (int msgRead = 0; msgRead < 5000000; msgRead++) {
            ProducerRecord<String, String> message = null;
            try {
                message = new ProducerRecord<String, String>(topic, queue.take());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            producer.send(message);
            System.out.println("Sent messages");
        }
        producer.close();
        client.stop();
        System.out.println("End of Process");
    }

    public static void main(String[] args) {
        try {
            run(
                    "7dPJ0GG9GTC3QdohGHkzIJqLR",
                    "Ez4YDNVZtQqsdZUmZXM2KBqeeguSKeh74ZhWHPI6q62UP47cIt",
                    "932604322235301888-QEiAiYj5vpib8YBUkW2RuRa02c4amjT",
                    "IDV9S7mPiNyIHqCoZ74QzJreoZorJotgnpkFP25CxwoDZ");
        } catch (InterruptedException e) {
            System.out.println(e);
        }
    }
}
