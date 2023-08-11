using Confluent.Kafka;
using KafkaWebApi;
using KafkaWebApi.Controllers;

var builder = WebApplication.CreateBuilder(args);

// Kafka Producer Configuration
var producerConfig = new ProducerConfig
{
    BootstrapServers = "localhost:9092"
};

// Kafka Consumer Configuration
var consumerConfig = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "my-group",
    AutoOffsetReset = AutoOffsetReset.Earliest
};

// Register services to the container.
builder.Services.AddSingleton(producerConfig);
builder.Services.AddSingleton(consumerConfig);
builder.Services.AddSingleton<KafkaProducerService>();
builder.Services.AddSingleton<KafkaConsumerService>();

// Register controllers to container
builder.Services.AddTransient<KafkaController>();

using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
consumer.Subscribe("my-topic"); // Replace with your Kafka topic

var cancellationTokenSource = new CancellationTokenSource();
var cancellationToken = cancellationTokenSource.Token;

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

//Comment this if don't want to listen
var consumerThread = new Thread(() => KafkaConsumerService.StartConsuming(consumer, app, cancellationToken));
consumerThread.Start();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

// app.UseAuthorization();
// app.MapControllers();

app.Map("/api", app =>
{
    app.UseRouting();
    app.UseAuthorization();
    app.UseEndpoints(endpoints =>
    {
        endpoints.MapControllers(); // Map controllers with the /api prefix
    });
});

app.Run();
