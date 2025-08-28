using Contracts;
using MassTransit;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddMassTransit(x =>
{
    x.AddConsumer<StartRunConsumer>();
    x.UsingRabbitMq((context, cfg) =>
    {
        cfg.Host("rabbitmq", "/", h =>
        {
            h.Username("guest");
            h.Password("guest");
        });
        cfg.ConfigureEndpoints(context);
    });
});

var app = builder.Build();
app.MapGet("/health", () => "ok");
app.Run("http://0.0.0.0:8080");

public class StartRunConsumer : IConsumer<StartRunCommand>
{
    private readonly IBus _bus;
    public StartRunConsumer(IBus bus) => _bus = bus;

    public async Task Consume(ConsumeContext<StartRunCommand> context)
    {
        // Forward the command to Calculation.Service
        await _bus.Publish(context.Message);
    }
}
