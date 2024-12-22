using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using System;
using Newtonsoft.Json;
using System.Collections.Generic;

namespace LogsTransmitterFunction
{
    /// <summary>
    /// Retry policy: https://vector.dev/docs/reference/configuration/sinks/http/#retry-policy
    /// * Vector will retry failed requests (status in [408, 429], >= 500, and != 501). 
    /// * Other responses will not be retried. You can control the number of retry attempts and 
    /// * backoff rate with the request.retry_attempts and request.retry_backoff_secs options.
    /// </summary>

    // TODO: move to isolated process flow!
    public static class LogsTransmitterFunction
    {
        public record LogBatch
        {
            [JsonProperty("id")]
            public string Id;
            [JsonProperty("logs")]
            public List<dynamic> Logs = [];
            [JsonProperty("created")]
            public DateTime Created = DateTime.UtcNow;
            [JsonProperty("expected_schema")]
            public bool ExpectedSchema;
        }

        private record LogLine
        {
            [JsonProperty("file")]
            public string File;
            [JsonProperty("host")]
            public string Host;
            [JsonProperty("message")]
            public string Message;
            [JsonProperty("source_type")]
            public string SourceType;
            [JsonProperty("timestamp")]
            public DateTime Timestamp;
        }

        [FunctionName("LogsTransmitter")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            [CosmosDB(
                databaseName: "LogDb",
                containerName: "CollectedLogs",
                CreateIfNotExists = true,
                PartitionKey = "/id",
                Connection = "CosmosDbConnectionSetting")]IAsyncCollector<LogBatch> output,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            // NOTE: the default batch size is 10 000 log records
            try
            {
                using var stream = new StreamReader(req.Body);
                string line;
                LogBatch batch = new()
                {
                    Id = Guid.NewGuid().ToString(),
                };

                while ((line = await stream.ReadLineAsync()) != null)
                {
                    try
                    {
                        var lines = JsonConvert.DeserializeObject<LogLine[]>(line);
                        batch.Logs.AddRange(lines);
                        batch.ExpectedSchema = true;
                    }
                    catch
                    {
                        // if not expected format
                        batch.Logs.Add(line);
                        batch.ExpectedSchema = false;
                    }
                }

                await output.AddAsync(batch);

                return new OkObjectResult($"Number of logs: {batch.Logs.Count}");
            }
            catch (Exception ex)
            {
                log.LogError(new EventId(), ex, ex.Message);
                throw;
            }
        }
    }
}
