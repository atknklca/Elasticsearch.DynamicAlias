using System.Text.Json.Nodes;
using Elastic.Clients.Elasticsearch;

var settings = new ElasticsearchClientSettings(new Uri("http://localhost:9200/"));
var client = new ElasticsearchClient(settings);

string filePath = "../../../ecommerce.json";
var jsonContent = File.ReadAllText(filePath);
var dataList = JsonNode.Parse(jsonContent)?.AsArray();

if (dataList != null)
    foreach (var data in dataList)
    {
        if (data?["order_date"] != null)
        {
            // "created_on" alanını al ve DateTime'a dönüştür
            var createdOn = DateTime.Parse(data["order_date"]!.ToString()).AddYears(8);
            data["order_date"] = createdOn;
            // Elasticsearch'e gönder
            client.IndexAsync(data, index: $"ecommerce-{createdOn.Year}-{createdOn.Month:D2}-{createdOn.Day:D2}").GetAwaiter()
                .GetResult();
            Console.WriteLine($"Indexing data for: ecommerce-{createdOn.Year}-{createdOn.Month:D2}-{createdOn.Day:D2}");
        }
        else
        {
            Console.WriteLine("created_on is null.");
        }
    }

Console.WriteLine("Import complete.!");