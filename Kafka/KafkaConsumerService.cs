using Confluent.Kafka;
using KafkaWebApi.Controllers;
using System;
using System.Threading;

namespace KafkaWebApi
{
    public class KafkaConsumerService
    {
        private readonly IConsumer<string, string> _consumer;

        public KafkaConsumerService(ConsumerConfig config)
        {
            _consumer = new ConsumerBuilder<string, string>(config).Build();
        }

        public void Start()
        {
            _consumer.Subscribe("my-topic");

            ThreadPool.QueueUserWorkItem(_ =>
            {
                try
                {
                    while (true)
                    {
                        var consumeResult = _consumer.Consume(CancellationToken.None);
                        Console.WriteLine($"Received message: {consumeResult.Message.Value}");
                    }
                }
                catch (OperationCanceledException)
                {
                    _consumer.Close();
                }
            });
        }
    
        public IEnumerable<string> ReadMessages(string topic)
        {
            _consumer.Subscribe(topic);

            var messages = new List<string>();

            try
            {
                while (true)
                {
                    var consumeResult = _consumer.Consume(TimeSpan.FromMilliseconds(100)); // Adjust the timeout as needed

                    if (consumeResult != null && !consumeResult.IsPartitionEOF)
                    {
                        //Actually only read 1 single message as this will continue to poll
                        messages.Add(consumeResult.Message.Value);
                        break;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Consumer.Close() will be called automatically upon disposal
            }

            return messages;
        }
    
        public static void StartConsuming(IConsumer<string, string> consumer, WebApplication app, CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var consumeResult = consumer.Consume(cancellationToken);

                    if (consumeResult != null && !consumeResult.IsPartitionEOF)
                    {
                        var message = consumeResult.Message.Value;

                        // Process the received Kafka message
                        using var scope = app.Services.CreateScope();
                        var kafkaController = scope.ServiceProvider.GetRequiredService<KafkaController>();
                        kafkaController.ProcessKafkaMessage(message);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Consumer.Close() will be called automatically upon disposal
            }
        }
    }
}
