using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Azure.Cosmos;
using System.Linq;
using Microsoft.AspNetCore.Mvc;

namespace LogsSourceFunction
{
    public class LogsSourceFunction(ILogger<LogsSourceFunction> logger, IConfiguration configuration)
    {
        private enum RequestType
        {
            Dummy,
            CosmosDB,
            SQL,
        }

        [Function("LogsSource")]
        public Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req)
        {
            logger.LogInformation("C# HTTP trigger function processed a request.");

            if (!req.Query.TryGetValue("requestType", out var requestType) ||
                !Enum.TryParse<RequestType>(requestType, ignoreCase: true, out var parsedRequestType))
            {
                throw new InvalidOperationException();
            }
            var requestId = Guid.NewGuid().ToString();

            var messages = parsedRequestType switch 
            {
                RequestType.CosmosDB => GetCosmosDbLogs(configuration),
                RequestType.SQL => GetSqlLogs(),
                // TODO: move the below into different test related function
                RequestType.Dummy => GetDummyLogs(),
                _ => throw new NotSupportedException()
            };
            return Task.FromResult((IActionResult)new OkObjectResult(messages));

            static string[] GetDummyLogs()
            {
                string[] responseMessage = [$"HttpSource_value:{Guid.NewGuid()}"];
                return responseMessage;
            }

            static string[] GetSqlLogs() => throw new NotImplementedException();

            static string[] GetCosmosDbLogs(IConfiguration configuration)
            {
                var connectionString = configuration.GetConnectionString("CosmosDb");
                using var client = new CosmosClient(connectionString: connectionString);
                var dbName = configuration.GetValue<string>("CosmosDbLogsDatabase");
                var containerName = configuration.GetValue<string>("CosmosDbLogsContainer");
                var container = client.GetContainer(dbName, containerName);
                var querable = container.GetItemLinqQueryable<dynamic>(allowSynchronousQueryExecution: true).ToList();
                return querable.Select(i => (string)i.message.ToString()).ToArray();
            }
        }
    }
}
