using Microsoft.AspNetCore.Mvc;
using KafkaWebApi;
using KafkaWebApi.Models;

namespace KafkaWebApi.Controllers;

[ApiController]
[Route("[controller]")]
public class KafkaController : ControllerBase {
    private readonly ILogger<KafkaController> _logger;
    private readonly KafkaProducerService _producerService;
    private readonly KafkaConsumerService _consumerService;

    public KafkaController(ILogger<KafkaController> logger, 
    KafkaProducerService producerService, KafkaConsumerService consumerService)
    {
        _logger = logger;
        _producerService = producerService;
        _consumerService = consumerService;
    }

    [HttpGet("test")]
    public IActionResult Test()
    {
        return Ok("Great Success!");
    }

    [HttpPost("push")]
    public IActionResult PushToKafka([FromBody] VmKafkaMessage jsonMessage)
    {
        if (jsonMessage == null)
        {
            return BadRequest("Invalid JSON message.");
        }

        _producerService.PushMessage("my-topic", jsonMessage.Message);
        return Ok("JSON message pushed to Kafka.");
    }

    [HttpGet("read")]
    public IActionResult ReadFromKafka()
    {
        var messages = _consumerService.ReadMessages("my-topic"); // Replace with your Kafka topic name
        return Ok(messages);
    }

    [HttpPost("process")]
    public IActionResult ProcessKafkaMessage([FromBody] string message)
    {
        // Process the received Kafka message
        // You can perform any desired operations here
        Console.WriteLine($"Service received a message to be sent out {message}");
        return Ok("Kafka message processed successfully.");
    }
}