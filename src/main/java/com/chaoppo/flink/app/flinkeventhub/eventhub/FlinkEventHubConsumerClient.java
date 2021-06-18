package com.chaoppo.flink.app.flinkeventhub.eventhub;

import com.azure.core.amqp.AmqpTransportType;
import com.azure.core.amqp.ProxyAuthenticationType;
import com.azure.core.amqp.ProxyOptions;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubConsumerAsyncClient;
import com.azure.messaging.eventhubs.EventHubConsumerClient;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Proxy;

@Service
public class FlinkEventHubConsumerClient implements Serializable {

    private String connectionString;
    private String consumerGroup;

    public FlinkEventHubConsumerClient() {
    }

    public FlinkEventHubConsumerClient(String connectionString) {
        this(connectionString, "$DEFAULT");
    }

    public FlinkEventHubConsumerClient(String connectionString, String consumerGroup) {
        this.connectionString = connectionString;
        this.consumerGroup = consumerGroup;
    }

    public EventHubConsumerClient createEventHubConsumerClient() {
        ProxyOptions proxyOptions = new ProxyOptions(ProxyAuthenticationType.NONE,
                new Proxy(Proxy.Type.HTTP, new InetSocketAddress("sgscaiu0388.inedc.corpintra.net", 3128)), null, null);
        return new EventHubClientBuilder().connectionString(connectionString).consumerGroup(consumerGroup)
                .proxyOptions(proxyOptions).transportType(AmqpTransportType.AMQP_WEB_SOCKETS).buildConsumerClient();
    }

    public EventHubConsumerAsyncClient createEventHubConsumerAsyncClient() {
        return new EventHubClientBuilder().connectionString(connectionString).consumerGroup(consumerGroup)
                .buildAsyncConsumerClient();
    }
}
