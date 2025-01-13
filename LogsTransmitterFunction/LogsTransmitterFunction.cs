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
using System.Linq;

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

        public record LogLine
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

        public record ParsedMessage
        {
            [JsonProperty("engagement_id")]
            public string EngagementId;
            [JsonProperty("title")]
            public string Title;
            [JsonProperty("severity")]
            public string Severity;
            [JsonProperty("stack_trace")]
            public string StackTrace;
            [JsonProperty("full_message")]
            public string FullMessage;
            [JsonProperty("file")]
            public string File;
            [JsonProperty("host")]
            public string Host;
            [JsonProperty("source_type")]
            public string SourceType;
            [JsonProperty("timestamp")]
            public DateTime Timestamp;
            [JsonProperty("expected_message_schema")]
            public bool ExpectedMessage => Title != null || EngagementId != null || Severity != null || StackTrace != null;
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
                        var parsedLines = lines.Select(ParseMessage).ToList();

                        batch.Logs.AddRange(parsedLines);
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

        /// <summary>
        /// Let it be public for now
        /// </summary>
        public static ParsedMessage ParseMessage(LogLine logLine)
        {
            const string EngagementIdToken = "Engagement Id:";
            const string TitleToken = "Title :";
            const string SeverityToken = "Severity ::";
            const string StackTraceToken = "StackTrace ::";
            var message = logLine.Message;

            int engagementIndex, titleIndex, severityIndex, stackTraceIndex;
            if (
                ((engagementIndex = message.IndexOf(EngagementIdToken, StringComparison.OrdinalIgnoreCase)) == -1) ||
                ((titleIndex = message.IndexOf(TitleToken, StringComparison.OrdinalIgnoreCase)) == -1) ||
                ((severityIndex = message.IndexOf(SeverityToken, StringComparison.OrdinalIgnoreCase)) == -1) ||
                ((stackTraceIndex = message.IndexOf(StackTraceToken, StringComparison.OrdinalIgnoreCase)) == 1) ||
                (engagementIndex != 0 || titleIndex <= engagementIndex || severityIndex <= titleIndex || stackTraceIndex <= severityIndex)
                )
            {
                // the message structure has not been recognized
                return new ParsedMessage 
                { 
                    FullMessage = message, 
                    File = logLine.File, 
                    Timestamp = logLine.Timestamp, 
                    Host = logLine.Host,
                    SourceType = logLine.SourceType,
                };
            }

            // TODO: regex instead?
            var engagementId = message.Substring(0, titleIndex).Replace(EngagementIdToken, string.Empty).Trim().TrimEnd('-').Trim().Trim('\"').Trim();
            var title = message.Substring(titleIndex, severityIndex - titleIndex).Replace(TitleToken, string.Empty).Trim().TrimEnd('-').Trim().Trim('\"').Trim();
            var severity = message.Substring(severityIndex, stackTraceIndex - severityIndex).Replace(SeverityToken, string.Empty).Trim().TrimEnd('-').Trim().Trim('\"').Trim();
            var stackTrace = message.Substring(stackTraceIndex, message.Length - stackTraceIndex).Replace(StackTraceToken, string.Empty).Trim().TrimEnd('-').Trim().Trim('\"').Trim();

            return new ParsedMessage 
            {
                EngagementId = engagementId, 
                Title = title, 
                StackTrace = stackTrace, 
                Severity = severity,
                FullMessage = message,

                File = logLine.File,
                Timestamp = logLine.Timestamp,
                Host = logLine.Host,
                SourceType = logLine.SourceType,
            };
        }
    }
}
