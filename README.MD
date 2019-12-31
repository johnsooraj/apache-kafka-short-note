Required Tools

Apache Kafka Download from the official site (https://kafka.apache.org/downloads)
Download Zookeeper (https://www.apache.org/dyn/closer.cgi/zookeeper)
Steps To Start Kafka Server and Zookeeper (below steps are based on the video attached)

Set $JAVA_HOME
Set $KAFKA_HOME (path to the downloaded Kafka directory)
Start Zookeeper (port: 2181) (cmd: ./bin/zookeeper-server.start.sh config/zookeeper.peoperties)
Start Kafka Server (port: 9092) (cmd: ./bin/kafka-server.start.sh config/server.peoperties)
IMP -> these are default port for the servers, we can change it in config/XXXX.properties file

Please see the video : below

Apache Kafka Ecosystem

Producers -> Kafka Cluster (managed by zookeeper) -> Consumer (see the pic attached)

img

Once the kafka-server and Zookeeper start and running, lets create the topic from command prompt.

Open terminal -> change directory to Kafka home

./bin/kafka-topics.sh --create --zookeeper localhost:2181 -replication-factor 1 --partitions 1 --topic myCustomTopic1

and list all available topic by,

./bin/kafka-topics.sh --list --zookeeper localhost:2181

Now move to the console output of the Messages, Creating a consumer for the topic.

open new terminal,

./bin/kafka-console-producer.sh --broker-list localhost:9002 -opic myCustomTopic1 (create a console for write message)

open new terminal,

./bin/kafka-console-consumer.sh --bootstrap-server localhost:9002 --topic myCustomTopic1 (can view the message writing in previous terminal)

For this presentation i have created two micro-services. One for produce the message & another for View the message (listener).

let me add the configuration needed for the application.properties (for both micro-services, same properties needed)



Producer (Service Two) will trigger an event/function on a time of interval (10 seconds)



application.properties

spring.kafka.consumer.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=my-custom-topic
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.producer.bootstrap-servers=localhost:9092

Service One (Listener)

@Slf4j
@Service
public class CustomerServiceImpl implements CustomerService {

    @Value("${kafka.sample.topic}")
    String kafkaSampleTopic;

    @KafkaListener(topics = {"myCustomTopic1"})
    public void kafkaCustomerListner(String currentTime) {
        log.info("kafka listner into timestamp : {}", currentTime);
    }
}

Service Two (Producer)

@Slf4j
@Async
@Component
public class AsynTaksUtils {

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.sample.topic}")
    private String kafkaSampleTopic;

    @Scheduled(fixedRate = 10000)
    public void sendSomeMessageTokafka() {
        kafkaTemplate.send(kafkaSampleTopic, LocalDateTime.now().toString());
        log.info("kafka timestamp send successfully");
    }
}