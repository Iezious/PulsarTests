using System;
using System.IO;
using Newtonsoft.Json;
using Pulsar.Client.Api;
using Pulsar.Client.Common;

namespace MessageProducer
{
    class Program
    {
        static Config LoadConfig()
        {
            return JsonConvert.DeserializeObject<Config>(File.ReadAllText("config.json"));
        }
        
        static void Main(string[] args)
        {
            var config = LoadConfig();
            var conn = new PulsarClientBuilder()
                .ServiceUrl(config.URL)
                .Build();

            var producer = new ProducerBuilder(conn)
                .Topic(config.TopicToWrite)
                .MessageRoutingMode(MessageRoutingMode.SinglePartition)
                .CompressionType(CompressionType.LZ4)
                .EnableBatching(false)
                .CreateAsync()
                .Result;
            
        }
    }
}