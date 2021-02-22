using Azure.Identity;
using Confluent.Kafka;
using Microsoft.Azure.Kafka.SchemaRegistry.Avro;
using System;
using System.Configuration;
using System.Threading;
using zohan.schemaregistry.events;

namespace Zohan.SchemaRegistry.Consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Press any key to begin consuming new events");
            Console.ReadKey();

            ConsumeEvents();
        }

        static void ConsumeEvents()
        {
            // Initialize the producer configuration properties           
            var config = InitializeConsumerConfig();

            // Create an instance of the serializer that will
            // use the schema for the messages.
            var valueDeserializer = InitializeValueDeserializer();

            using (var consumer = new ConsumerBuilder<Null, CustomerLoyalty>(config)
                .SetValueDeserializer(valueDeserializer)
                .Build())
            {
                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

                // Retrieve the topic name from the configuration settings
                // and set the subscription to the topic.
                var topic = ConfigurationManager.AppSettings["EH_NAME"];                
                consumer.Subscribe(topic);
                Console.WriteLine($"Consuming messages from topic: {topic}");

                // Start consuming events
                try
                {
                    while (true)
                    {
                        try
                        {
                            var msg = consumer.Consume(cts.Token);
                            var loyalty = msg.Message.Value;
                            Console.WriteLine($"Customer {loyalty.CustomerId} received {loyalty.PointsAdded} points");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Consume error: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    consumer.Close();
                }
            }
        }

        private static KafkaAvroDeserializer<CustomerLoyalty> InitializeValueDeserializer()
        {
            // Retrieve the necessary settings for the schema url and
            // credentials needed communicate with the registry in Azure. 
            var schemaRegistryUrl = ConfigurationManager.AppSettings["SCHEMA_REGISTRY_URL"];
            ClientSecretCredential credential = new ClientSecretCredential(
                            ConfigurationManager.AppSettings["SCHEMA_REGISTRY_TENANT_ID"],
                            ConfigurationManager.AppSettings["SCHEMA_REGISTRY_CLIENT_ID"],
                            ConfigurationManager.AppSettings["SCHEMA_REGISTRY_CLIENT_SECRET"]);

            return new KafkaAvroDeserializer<CustomerLoyalty>(schemaRegistryUrl, credential);
        }

        private static ConsumerConfig InitializeConsumerConfig()
        {
            var brokerList = ConfigurationManager.AppSettings["EH_FQDN"];
            var connectionString = ConfigurationManager.AppSettings["EH_CONNECTION_STRING"];
            var caCertLocation = ConfigurationManager.AppSettings["CA_CERT_LOCATION"];
            var consumerGroup = ConfigurationManager.AppSettings["CONSUMER_GROUP"];

            return new ConsumerConfig
            {
                BootstrapServers = brokerList,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SocketTimeoutMs = 60000, 
                SessionTimeoutMs = 30000,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "$ConnectionString",
                SaslPassword = connectionString,
                SslCaLocation = caCertLocation,
                GroupId = consumerGroup,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                BrokerVersionFallback = "1.0.0"
            };
        }
    }
}
