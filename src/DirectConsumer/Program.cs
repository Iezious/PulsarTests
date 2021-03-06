﻿using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Pulsar.Client.Api;
using Pulsar.Client.Common;
using Serilog;
using Serilog.Core;

namespace DirectConsumer
{
    class Program
    {
        private static bool _doAck = true; 
        static void Main()
        {
            var cancel = new CancellationTokenSource();
            var logger = new LoggerConfiguration()
                .MinimumLevel.Verbose()
                .WriteTo.Console()
                .CreateLogger();
            
            Execute(cancel.Token, logger);

            while (!cancel.IsCancellationRequested)
            {
                Console.Write("#>");
                var cmd = Console.ReadLine();

                switch (cmd?.Trim())
                {
                    case "exit":
                        cancel.Cancel();
                        break;
                
                    case "a":
                        _doAck = true;
                        break;
                    
                    case "n":
                        _doAck = false;
                        break;
                }
            }
        }

        private static async void Execute(CancellationToken cancelToken, Logger logger)
        {
            var subscription = await CreateSubscription(logger);
            
            if(subscription == null) return;
            
            while (!cancelToken.IsCancellationRequested)
            {
                try
                {
                    var message = await subscription.ReceiveAsync();
                    if (_doAck)
                    {
                        await subscription.AcknowledgeAsync(message.MessageId);
                        var msg = Encoding.UTF8.GetString(message.Data);
                        logger.Verbose($"Message: {msg} @ {message.MessageId.EntryId}");
                    }
                    else
                    {
                        var msg = Encoding.UTF8.GetString(message.Data);
                        logger.Verbose($"NO ACK Message: {msg} @ {message.MessageId.EntryId}");
                    }
                }
                catch (Exception e)

                {
                    logger.Error($"Message recisive error {e.Message}", e);
                }
            }
        }
        
        static Config LoadConfig()
        {
            return JsonConvert.DeserializeObject<Config>(File.ReadAllText("config.json"));
        }       
        
        private static async Task<IConsumer> CreateSubscription(ILogger logger)
        {
            var config = LoadConfig();
            var conn = new PulsarClientBuilder()
                .ServiceUrl(config.URL)
                .Build();

            try
            {
                return await new ConsumerBuilder(conn)
                    .Topic(config.TopicToRead)
                    .SubscriptionType(SubscriptionType.Failover)
                    .SubscriptionName(config.ConsumerID)
                    .AckTimeout(TimeSpan.FromSeconds(30))
                    .NegativeAckRedeliveryDelay(TimeSpan.FromSeconds(45))
                    .DeadLettersPolicy(new DeadLettersPolicy(5, null))
                    .SubscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .SubscribeAsync();
                
            }
            catch (Exception e)
            {
                logger.Fatal($"Connection exception {e.Message}", e);
            }

            return null;

        }        
    }
}