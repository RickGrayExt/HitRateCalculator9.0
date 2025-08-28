using Contracts;
using MassTransit;
using System.Collections.Concurrent;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddMassTransit(x =>
{
    x.AddConsumer<ResultConsumer>();
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

builder.Services.AddSingleton<ResultWaiter>();

var app = builder.Build();

app.MapGet("/health", () => "ok");

app.MapPost("/run", async (StartRunCommand req, IBus bus, ResultWaiter waiter) =>
{
    var runId = req.RunId == Guid.Empty ? Guid.NewGuid() : req.RunId;
    var ds = string.IsNullOrWhiteSpace(req.DatasetPath) ? "/app/data/DataSetClean.csv" : req.DatasetPath;
    var cmd = req with { RunId = runId, DatasetPath = ds };

    var tcs = waiter.Create(runId);
    await bus.Publish(cmd);

    var completed = await Task.WhenAny(tcs.Task, Task.Delay(TimeSpan.FromSeconds(25)));
    if (completed == tcs.Task)
    {
        var res = await tcs.Task;
        return Results.Json(new { runId, result = res });
    }
    else
    {
        waiter.Remove(runId);
        return Results.Json(new { runId, status = "timeout" });
    }
});

app.Run("http://0.0.0.0:8080");

public class ResultWaiter
{
    private readonly ConcurrentDictionary<Guid, TaskCompletionSource<HitRateResult>> _map = new();

    public TaskCompletionSource<HitRateResult> Create(Guid runId)
        => _map.GetOrAdd(runId, _ => new(TaskCreationOptions.RunContinuationsAsynchronously));

    public bool TrySet(Guid runId, HitRateResult res)
        => _map.TryRemove(runId, out var tcs) && tcs.TrySetResult(res);

    public void Remove(Guid runId) => _map.TryRemove(runId, out _);
}

public class ResultConsumer : IConsumer<HitRateCalculated>
{
    private readonly ResultWaiter _waiter;
    public ResultConsumer(ResultWaiter waiter) => _waiter = waiter;
    public Task Consume(ConsumeContext<HitRateCalculated> ctx)
    {
        _waiter.TrySet(ctx.Message.RunId, ctx.Message.Result);
        return Task.CompletedTask;
    }
}
