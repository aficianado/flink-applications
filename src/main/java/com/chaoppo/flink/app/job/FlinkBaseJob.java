package com.chaoppo.flink.app.job;

import com.chaoppo.flink.app.common.Constants;
import com.chaoppo.flink.app.config.FlinkEnvironmentConfiguration;
import com.chaoppo.flink.app.connector.FlinkConnector;
import com.chaoppo.flink.app.executor.FlinkExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class FlinkBaseJob {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkBaseJob.class);

    @Autowired
    private FlinkEnvironmentConfiguration flinkEnvironmentConfiguration;

    @Autowired
    private FlinkExecutor flinkExecutor;

    @Autowired
    private FlinkConnector flinkConnector;

    @Value("${bootstrap.servers}")
    private String bootStrapServer;

    @Value("${group.id}")
    private String groupId;

    @Value("${request.timeout.ms}")
    private String requestTimeout;

    @Value("${sasl.mechanism}")
    private String saslMechanism;

    @Value("${security.protocol}")
    private String securityProtocol;

    @Value("${sasl.jaas.config}")
    private String saslJaasConfig;

    @Value("${zookeeper.connect}")
    private String zookeeperConnect;

    @Value("${ddec.ingest.kafka.topic}")
    private String ddecIngestKafkaTopic;

    @Value("${ddec.connectionString}")
    private String connectionString;

    public void executeBaseJob() {
        LOG.info("Started execution jobs");
        //        ingestJob.ingestJob(flinkEnvironmentConfiguration.createEnvironmentWithWebUI(), flinkConnector.configKafkaConsumer(ddecIngestKafkaTopic, loadProperties()), flinkExecutor);
        //        ingestJob.hbaseSFJob(flinkEnvironmentConfiguration.createEnvironment(), new HBaseSourceFunction(new FlinkEventHubConsumerClient(connectionString,"$DEFAULT"), "DdecIngestTopology", "8" , true),flinkExecutor);
        //        ingestJob.consumer(flinkEnvironmentConfiguration.createEnvironmentWithWebUI(),flinkExecutor);
    }

    private Properties loadProperties() {
        Properties properties = new Properties();
        properties.setProperty(Constants.BOOT_STRAP_SERVER, bootStrapServer);
        properties.setProperty(Constants.GROUP_ID, groupId);
        properties.setProperty(Constants.REQUEST_TIMEOUT, requestTimeout);
        properties.setProperty(Constants.SASL_MECHANISM, saslMechanism);
        properties.setProperty(Constants.SECURITY_PROTOCOL, securityProtocol);
        properties.setProperty(Constants.SASL_JAAS_CONFIG, saslJaasConfig);
        properties.setProperty(Constants.ZOOKEEPER_CONNECT, zookeeperConnect);
        return properties;
    }

}
