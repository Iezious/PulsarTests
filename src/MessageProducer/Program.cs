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
            var ra = new Random();
            
            while (!cancelToken.IsCancellationRequested)
            {
                
                try
                {
                    var msg = System.Text.Encoding.UTF8.GetBytes($"[{cnt} - msg at {DateTime.UtcNow}]");
                    var message = producer.NewMessage(msg, (cnt % 100).ToString());
                    var sent = await producer.SendAsync(message);
                    logger.Verbose($"Message {cnt} sent - {sent.Partition}");
                }
                catch (Exception e)
                {
                    logger.Error($"Send error for {cnt}" ,e);
             //       logger.Information($"Producer status: {producer.}");
                }
                cnt++;
                await Task.Delay(500, cancelToken);
            }
        }

        private static async Task<IProducer<byte[]>> CreateProducer(ILogger logger)
        {
            var config = LoadConfig();
            var conn = await new PulsarClientBuilder()
                .ServiceUrl(config.URL)
                .BuildAsync();

            try
            {
                var producer = await conn.NewProducer<byte[]>(Schema.BYTES())
                    .Topic(config.TopicToWrite)
                    .MessageRoutingMode(MessageRoutingMode.RoundRobinPartition)
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