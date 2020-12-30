using Azure.Identity;
using Confluent.Kafka;
using Microsoft.Azure.Kafka.SchemaRegistry.Avro;
using System;
using System.Configuration;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using zohan.schemaregistry.events;

namespace Zohan.SchemaRegistry.Producer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Press any key to begin sending events.");
            Console.ReadKey();

            await SendEvents();            
        }

        public static async Task SendEvents()
        {
            // Initialize the producer configuration properties           
            var config = InitializeProducerConfig();

            // Create an instance of the serializer that will
            // use the schema for the messages.
            var valueSerializer = InitializeValueSerializer();

            try
            {
                // Create an instance of the producer using the serializer
                // for the message value. 
                using (var producer = new ProducerBuilder<Null, CustomerLoyalty>(config)
                    .SetValueSerializer(valueSerializer)
                    .Build())
                {
                    // Retrieve the topic name from the configuration settings
                    var topic = ConfigurationManager.AppSettings["EH_NAME"];

                    // Send some messages
                    for (int i = 0; i < 4; i++)
                    {
                        var loyaltyEvent = new CustomerLoyalty()
                        {
                            CustomerId = 1,
                            PointsAdded = i,
                            Description = $"Points added: {i}"
                        };
                        
                        var message = new Message<Null, CustomerLoyalty> { Key = null, Value = loyaltyEvent };
                        await producer.ProduceAsync(topic, message);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(string.Format("Exception Occurred - {0}", e.Message));
            }
        }

        private static KafkaAvroAsyncSerializer<CustomerLoyalty> InitializeValueSerializer()
        {
            // Retrieve the necessary settings for the schema url, group and
            // credentials needed communicate with the registry in Azure. 
            var schemaRegistryUrl = ConfigurationManager.AppSettings["SCHEMA_REGISTRY_URL"];
            var schemaGroup = ConfigurationManager.AppSettings["SCHEMA_GROUP"];
            ClientSecretCredential credential = new ClientSecretCredential(
                            ConfigurationManager.AppSettings["SCHEMA_REGISTRY_TENANT_ID"],
                            ConfigurationManager.AppSettings["SCHEMA_REGISTRY_CLIENT_ID"],
                            ConfigurationManager.AppSettings["SCHEMA_REGISTRY_CLIENT_SECRET"]);
            
            // Set the autoRegisterSchema flag to true so that the schema will be 
            // registered if it does not already exist.
            return new KafkaAvroAsyncSerializer<CustomerLoyalty>(
                schemaRegistryUrl,
                credential,
                schemaGroup,
                autoRegisterSchemas: true);
        }

        private static ProducerConfig InitializeProducerConfig()
        {
            var brokerList = ConfigurationManager.AppSettings["EH_FQDN"];
            var connectionString = ConfigurationManager.AppSettings["EH_CONNECTION_STRING"];
            var caCertLocation = ConfigurationManager.AppSettings["CA_CERT_LOCATION"];
            
            return new ProducerConfig
            {
                BootstrapServers = brokerList,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "$ConnectionString",
                SaslPassword = connectionString,
                SslCaLocation = caCertLocation
            };
        }
    }
}
