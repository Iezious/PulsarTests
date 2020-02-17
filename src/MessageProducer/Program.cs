using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Pulsar.Client.Api;
using Pulsar.Client.Common;
using Serilog;

namespace MessageProducer
{
    class Program
    {
        static void Main()
        {
            var cancel = new CancellationTokenSource();
            var logger = new LoggerConfiguration()
                .MinimumLevel.Verbose()
                .WriteTo.Console()
                .CreateLogger();
            
            Execute(cancel.Token, logger);

            Console.WriteLine("Press enter to finish");
            Console.ReadLine();
        }

        static Config LoadConfig()
        {
            return JsonConvert.DeserializeObject<Config>(File.ReadAllText("config.json"));
        }

        static async void Execute(CancellationToken cancelToken, ILogger logger)
        {
            await Task.Yield();
            
            var producer = await CreateProducer(logger);

            if(producer == null) return;
            
            var cnt = 0;
            
            while (!cancelToken.IsCancellationRequested)
            {
                
                try
                {
                    var msg = System.Text.Encoding.UTF8.GetBytes($"[{cnt} - msg at {DateTime.UtcNow}]");
                    var sent = await producer.SendAsync(msg);
                    logger.Verbose($"Message {cnt} sent - {sent.EntryId}");
                }
                catch (Exception e)
                {
                    logger.Error($"Send error for {cnt}" ,e);
                }
                cnt++;
                await Task.Delay(500, cancelToken);
            }
        }

        private static async Task<IProducer> CreateProducer(ILogger logger)
        {
            var config = LoadConfig();
            var conn = new PulsarClientBuilder()
                .ServiceUrl(config.URL)
                .Build();

            try
            {
                var producer = await new ProducerBuilder(conn)
                    .Topic(config.TopicToWrite)
                    .MessageRoutingMode(MessageRoutingMode.SinglePartition)
                    .CompressionType(CompressionType.LZ4)
                    .EnableBatching(false)
                    .CreateAsync();
                
                return producer;
            }
            catch (Exception e)
            {
                logger.Fatal($"Connection exception {e.Message}", e);
            }

            return null;
        }
    }
}