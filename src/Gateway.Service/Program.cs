using Contracts;
using MassTransit;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
    {
        policy.AllowAnyOrigin()
              .AllowAnyMethod()
              .AllowAnyHeader();
    });
});

builder.Services.AddMassTransit(x =>
{
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
app.UseCors();

app.MapPost("/api/run", async (StartRunCommand req, IBus bus, ResultWaiter waiter) =>
{
    var runId = req.RunId == Guid.Empty ? Guid.NewGuid() : req.RunId;
    var dataset = string.IsNullOrWhiteSpace(req.DatasetPath) ? "/app/data/DataSetClean.csv" : req.DatasetPath;
    var cmd = req with { RunId = runId, DatasetPath = dataset };

    var tcs = waiter.Create(runId);
    await bus.Publish(cmd);

    if (await Task.WhenAny(tcs.Task, Task.Delay(TimeSpan.FromSeconds(20))) == tcs.Task)
    {
        var result = await tcs.Task;
        return Results.Json(new { runId, result });
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
