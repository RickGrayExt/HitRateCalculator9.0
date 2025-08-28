
using System.Globalization;
using Contracts;
using CsvHelper;
using MassTransit;

var builder = WebApplication.CreateBuilder(args);

// MassTransit with RabbitMQ
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

// ---------- Consumers & Logic ----------
public class StartRunConsumer : IConsumer<StartRunCommand>
{
    public async Task Consume(ConsumeContext<StartRunCommand> context)
    {
        var cmd = context.Message;
        var dsPath = string.IsNullOrWhiteSpace(cmd.DatasetPath) ? "/app/data/DataSetClean.csv" : cmd.DatasetPath;
        var records = LoadSales(dsPath);

        var result = Calculator.Run(records, cmd.Params, cmd.Mode ?? "default");

        await context.Publish(new HitRateCalculated(cmd.RunId, result));
    }

    private static List<SalesRecord> LoadSales(string path)
    {
        using var reader = new StreamReader(path);
        using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
        var rows = new List<SalesRecord>();
        csv.Read();
        csv.ReadHeader();
        while (csv.Read())
        {
            // Expect field headings EXACTLY as in the provided dataset
            var orderDate = DateOnly.FromDateTime(csv.GetField<DateTime>("Order Date"));
            var orderId = csv.GetField<string>("Order ID");
            var customerId = csv.GetField<string>("Customer ID");
            var category = csv.GetField<string>("Category");
            var product = csv.GetField<string>("Product Name");
            var sales = csv.GetField<decimal>("Sales");
            var qty = csv.GetField<int>("Quantity");
            var priority = csv.GetField<string>("Order Priority");

            rows.Add(new SalesRecord(orderDate, orderId, customerId, category, product, sales, qty, priority));
        }
        return rows;
    }
}

public static class Calculator
{
    public static HitRateResult Run(List<SalesRecord> sales, RunParams p, string mode)
    {
        // Demand per SKU
        var skuDemand = sales
            .GroupBy(r => (SkuId: r.Product, Category: r.ProductCategory))
            .Select(g => new {
                g.Key.SkuId,
                g.Key.Category,
                TotalUnits = g.Sum(x => x.Qty),
                OrderCount = g.Select(x => x.OrderId).Distinct().Count()
            })
            .OrderByDescending(x => x.TotalUnits)
            .ToList();

        // Seasonality boost (simple: add 20% weight to December demand ranking when enabled)
        if (p.UseSeasonalityBoost)
        {
            var decSkus = sales.Where(r => r.OrderDate.Month == 12)
                               .GroupBy(r => r.Product)
                               .ToDictionary(g => g.Key, g => g.Sum(x => x.Qty));
            skuDemand = skuDemand
                .Select(x => new {
                    x.SkuId,
                    x.Category,
                    TotalUnits = x.TotalUnits + (decSkus.TryGetValue(x.SkuId, out var d) ? (int)(d * 0.2) : 0),
                    x.OrderCount
                })
                .OrderByDescending(x => x.TotalUnits)
                .ToList();
        }

        // Assign SKUs to racks
        int maxSkusPerRack = Math.Max(1, p.MaxSkusPerRack);
        var rackMap = new Dictionary<string, List<string>>();
        int rackIndex = 1;
        rackMap["R1"] = new List<string>();
        int inRack = 0;
        foreach (var s in skuDemand)
        {
            if (inRack >= maxSkusPerRack)
            {
                rackIndex++;
                rackMap[$"R{rackIndex}"] = new List<string>();
                inRack = 0;
            }
            rackMap[$"R{rackIndex}"].Add(s.SkuId);
            inRack++;
        }

        // Build batches (very lightweight PTL vs PTO simulation)
        bool usePto = p.UsePto;
        int maxLines = Math.Max(1, p.MaxBatchLines);
        int maxStations = Math.Max(1, p.MaxStationsOpen);

        var orders = sales.GroupBy(r => r.OrderId)
                          .Select(g => new {
                              OrderId = g.Key,
                              Lines = g.GroupBy(x => x.Product)
                                       .Select(gg => (Sku: gg.Key, Qty: gg.Sum(x => x.Qty)))
                                       .ToList()
                          }).ToList();

        var batches = new List<List<(string OrderId, string Sku, int Qty)>>();
        var current = new List<(string,string,int)>();
        var currentSkus = new HashSet<string>();

        foreach (var o in orders)
        {
            foreach (var line in o.Lines)
            {
                if (usePto)
                {
                    if (current.Count >= maxLines)
                    {
                        batches.Add(current);
                        current = new();
                        currentSkus.Clear();
                    }
                    current.Add((o.OrderId, line.Sku, line.Qty));
                    currentSkus.Add(line.Sku);
                }
                else
                {
                    // PTL: try to keep SKU groups together; enforce MaxSkusPerStation as a proxy
                    if (current.Count >= maxLines || (currentSkus.Count >= Math.Max(1, p.MaxSkusPerStation) && !currentSkus.Contains(line.Sku)))
                    {
                        batches.Add(current);
                        current = new();
                        currentSkus.Clear();
                    }
                    current.Add((o.OrderId, line.Sku, line.Qty));
                    currentSkus.Add(line.Sku);
                }
            }
        }
        if (current.Count > 0) batches.Add(current);

        // Station allocation – round-robin across open stations
        var stationCount = Math.Min(maxStations, Math.Max(1, batches.Count));
        var stationBatches = Enumerable.Range(0, stationCount).ToDictionary(i => $"S{i+1}", i => new List<int>());
        for (int i = 0; i < batches.Count; i++)
        {
            var key = $"S{(i % stationCount)+1}";
            stationBatches[key].Add(i);
        }

        // Compute hit rate: items per rack presentation
        int totalItemsPicked = 0;
        int totalRackPresentations = 0;
        var byRack = rackMap.Keys.ToDictionary(r => r, r => 0.0);

        foreach (var idxList in stationBatches.Values)
        {
            foreach (var bi in idxList)
            {
                var lines = batches[bi];
                totalItemsPicked += lines.Sum(l => l.Qty);
                // Which racks were presented in this batch?
                var racksTouched = new HashSet<string>();
                foreach (var l in lines)
                {
                    foreach (var (rackId, skus) in rackMap)
                    {
                        if (skus.Contains(l.Sku))
                        {
                            racksTouched.Add(rackId);
                            break;
                        }
                    }
                }
                totalRackPresentations += racksTouched.Count;
                foreach (var r in racksTouched)
                    byRack[r] += lines.Where(l => rackMap[r].Contains(l.Sku)).Sum(l => (double)l.Qty);
            }
        }

        var avgByRack = byRack.ToDictionary(kv => kv.Key, kv => totalRackPresentations == 0 ? 0.0 : kv.Value / totalRackPresentations);
        double hitRate = totalRackPresentations == 0 ? 0.0 : (double)totalItemsPicked / totalRackPresentations;

        return new HitRateResult(mode, hitRate, totalItemsPicked, totalRackPresentations, avgByRack);
    }
}
