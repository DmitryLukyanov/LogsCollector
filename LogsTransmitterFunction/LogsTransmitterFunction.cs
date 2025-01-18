using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;
using Microsoft.Azure.Functions.Worker;
using System.Text.Json;
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
    public class LogsTransmitterFunction(ILogger<LogsTransmitterFunction> logger)
    {
        public record LogBatch
        {
            [JsonPropertyName("id")]
            public string Id { get; set; }
            [JsonPropertyName("logs")]
            public List<dynamic> Logs { get; set; } = [];
            [JsonPropertyName("created")]
            public DateTime Created { get; set; } = DateTime.UtcNow;
            [JsonPropertyName("expected_schema")]
            public bool ExpectedSchema { get; set; }
        }

        public record LogLine
        {
            [JsonPropertyName("file")]
            public string File { get; set; }
            [JsonPropertyName("host")]
            public string Host { get; set; }
            [JsonPropertyName("message")]
            public string Message { get; set; }
            [JsonPropertyName("source_type")]
            public string SourceType { get; set; }
            [JsonPropertyName("timestamp")]
            public DateTime Timestamp { get; set; }
        }

        public record ParsedMessage
        {
            [JsonPropertyName("engagement_id")]
            public string EngagementId { get; set; }
            [JsonPropertyName("title")]
            public string Title { get; set; }
            [JsonPropertyName("severity")]
            public string Severity { get; set; }
            [JsonPropertyName("stack_trace")]
            public string StackTrace { get; set; }
            [JsonPropertyName("full_message")]
            public string FullMessage { get; set; }
            [JsonPropertyName("file")]
            public string File { get; set; }
            [JsonPropertyName("host")]
            public string Host { get; set; }
            [JsonPropertyName("source_type")]
            public string SourceType { get; set; }
            [JsonPropertyName("timestamp")]
            public DateTime Timestamp { get; set; }
            [JsonPropertyName("expected_message_schema")]
            public bool ExpectedMessage => Title != null || EngagementId != null || Severity != null || StackTrace != null;
        }

        [Function("LogsTransmitter")]
        [CosmosDBOutput(
            databaseName: "LogDb",
            containerName: "CollectedLogs",
            CreateIfNotExists = true,
            PartitionKey = "/id",
            Connection = "CosmosDbConnectionSetting")]
        public async Task<LogBatch> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req)
        {
            logger.LogInformation("C# HTTP trigger function processed a request.");

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
                        var lines = JsonSerializer.Deserialize<LogLine[]>(line);
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

                return batch;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, ex.Message);
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
            var engagementId = message[..titleIndex].Replace(EngagementIdToken, string.Empty).Trim().TrimEnd('-').Trim().Trim('\"').Trim();
            var title = message[titleIndex..severityIndex].Replace(TitleToken, string.Empty).Trim().TrimEnd('-').Trim().Trim('\"').Trim();
            var severity = message[severityIndex..stackTraceIndex].Replace(SeverityToken, string.Empty).Trim().TrimEnd('-').Trim().Trim('\"').Trim();
            var stackTrace = message[stackTraceIndex..].Replace(StackTraceToken, string.Empty).Trim().TrimEnd('-').Trim().Trim('\"').Trim();

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
