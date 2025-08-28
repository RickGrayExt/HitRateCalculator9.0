namespace Contracts;

public record StartRunCommand(
    Guid RunId,
    string DatasetPath,
    string Mode,
    RunParams Params
);

public record HitRateCalculated(Guid RunId, HitRateResult Result);
