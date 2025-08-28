using Contracts;
using CsvHelper;
using CsvHelper.Configuration;
using MassTransit;
using System.Globalization;

public class StartRunConsumer : IConsumer<StartRunCommand>
{
    public async Task Consume(ConsumeContext<StartRunCommand> context)
    {
        var msg = context.Message;
        var records = LoadSales(msg.DatasetPath);

        // group orders into waves (3 hours default)
        var waves = records
            .GroupBy(r => new DateTime(r.Order_Date.Year, r.Order_Date.Month, r.Order_Date.Day, r.Order_Date.Hour / 3 * 3, 0, 0))
            .OrderBy(g => g.Key)
            .ToList();

        int totalItems = 0;
        int totalPresentations = 0;
        Dictionary<string, double> byRack = new();

        foreach (var wave in waves)
        {
            var uniqueSkus = wave.Select(r => r.Product).Distinct().ToList();
            var racks = AllocateRacks(uniqueSkus, msg.Params.MaxSkusPerRack);

            foreach (var rack in racks)
            {
                var itemsPicked = wave.Count(r => rack.Contains(r.Product));
                if (itemsPicked > 0)
                {
                    totalItems += itemsPicked;
                    totalPresentations++;
                    byRack[string.Join(",", rack)] = itemsPicked;
                }
            }
        }

        var hitRate = totalPresentations > 0 ? (double)totalItems / totalPresentations : 0.0;

        var result = new HitRateResult(msg.Mode, hitRate, totalItems, totalPresentations, byRack);
        await context.Publish(new HitRateCalculated(msg.RunId, result));
    }

    private List<HashSet<string>> AllocateRacks(List<string> skus, int maxSkusPerRack)
    {
        var racks = new List<HashSet<string>>();
        var currentRack = new HashSet<string>();
        foreach (var sku in skus)
        {
            if (currentRack.Count >= maxSkusPerRack)
            {
                racks.Add(currentRack);
                currentRack = new HashSet<string>();
            }
            currentRack.Add(sku);
        }
        if (currentRack.Count > 0)
            racks.Add(currentRack);
        return racks;
    }

    private List<SalesRecord> LoadSales(string path)
    {
        using var reader = new StreamReader(path);
        using var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HeaderValidated = null,
            MissingFieldFound = null
        });
        csv.Context.RegisterClassMap<SalesRecordMap>();
        return csv.GetRecords<SalesRecord>().ToList();
    }
}

public sealed class SalesRecordMap : ClassMap<SalesRecord>
{
    public SalesRecordMap()
    {
        Map(m => m.Order_Date).TypeConverterOption.Format("dd/MM/yyyy");
        Map(m => m.Time);
        Map(m => m.Customer_Id);
        Map(m => m.Product_Category);
        Map(m => m.Product);
        Map(m => m.Sales);
        Map(m => m.Quantity);
        Map(m => m.Order_Priority);
    }
}
