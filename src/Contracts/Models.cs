namespace Contracts;

public record SalesRecord(
    DateTime Order_Date,
    string Time,
    string Customer_Id,
    string Product_Category,
    string Product,
    decimal Sales,
    int Quantity,
    string Order_Priority
);

public record Sku(string Id, string Name, string Category);
public record SkuDemand(string SkuId, int TotalUnits, int OrderCount, double Velocity, bool Seasonal, string Category);

public record SkuGroup(string GroupId, List<string> SkuIds);

public record ShelfLocation(string SkuId, string RackId, string SlotId, int Rank);

public record Rack(string RackId, int LevelCount, int SlotPerLevel, double MaxWeightKg);

public record OrderLine(string OrderId, string SkuId, int Qty, string RackId);

public record Batch(string BatchId, string Mode, List<OrderLine> Lines);

public record Station(string StationId, int Capacity);
public record StationAssignment(string StationId, List<string> BatchIds);

public record HitRateResult(string Mode, double HitRate, int TotalItemsPicked, int TotalRackPresentations,
                            Dictionary<string, double> ByRack);

public record RunParams(
    bool UseSeasonalityBoost,
    int MaxSkusPerRack,
    int MaxSkusPerStation,
    int MaxBatchLines,
    bool UsePto,
    int MaxStationsOpen,
    int RacksLevels,
    int SlotsPerLevel);
