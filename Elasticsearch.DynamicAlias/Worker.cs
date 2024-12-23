using Elastic.Clients.Elasticsearch;
using Elastic.Transport;

namespace Elasticsearch.DynamicAlias;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _Logger;
    private readonly ElasticsearchClient _Client;
    private const string LiveAliasName = "live-ecommerce";
    private const string AllAliasName = "all-ecommerce";

    public Worker(ILogger<Worker> logger)
    {
        _Logger = logger;
        _Client = new ElasticsearchClient(
            new ElasticsearchClientSettings(new SingleNodePool(new Uri("http://localhost:9200/"))));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ExecuteElasticProcessAsync();
            }
            catch (Exception ex)
            {
                _Logger.LogError(ex, "An error occurred while executing the Elasticsearch process");
            }

            await Task.Delay(TimeSpan.FromDays(1), stoppingToken);
        }
    }

    private async Task ExecuteElasticProcessAsync()
    {
        var now = DateTime.Now;
        var newIndicesName = $"ecommerce-{now.Year}-{now.Month:D2}-{now.Day:D2}";

        var aliasList = (await _Client.Indices.GetAliasAsync(c => c.Name(LiveAliasName))).Aliases.ToList();

        var currentWriteIndices = aliasList.OrderByDescending(x => x.Key.ToString())
            .FirstOrDefault(x =>
                x.Value.Aliases.First(a => a.Key == LiveAliasName).Value.IsWriteIndex == true);

        var currentWriteIndicesSettings = (await _Client.Indices.GetAsync(currentWriteIndices.Key)).Indices.First();

        await _Client.Indices.CreateAsync(newIndicesName, s =>
        {
            s.Mappings(currentWriteIndicesSettings.Value.Mappings);
            s.Settings(settings =>
            {
                settings.NumberOfShards(currentWriteIndicesSettings.Value.Settings?.NumberOfShards);
                settings.NumberOfReplicas(currentWriteIndicesSettings.Value.Settings?.NumberOfReplicas);
                settings.Routing(currentWriteIndicesSettings.Value.Settings?.Routing);
            });
        });


        await _Client.Indices.DeleteAliasAsync(currentWriteIndices.Key, LiveAliasName);
        await _Client.Indices.PutAliasAsync(currentWriteIndices.Key, LiveAliasName);
        await _Client.Indices.PutAliasAsync(newIndicesName, LiveAliasName, c => c.IsWriteIndex());
        await _Client.Indices.PutAliasAsync(newIndicesName, AllAliasName);
    }
}