using Confluent.Kafka;
using System;

namespace KafkaWebApi
{
    public class KafkaProducerService
    {
        private readonly IProducer<string, string> _producer;

        public KafkaProducerService(ProducerConfig config)
        {
            _producer = new ProducerBuilder<string, string>(config).Build();
        }

        public void PushMessage(string topic, string message)
        {
            _producer.Produce(topic, new Message<string, string> { Key = Guid.NewGuid().ToString(), Value = message });
        }
    }
}
