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
        [Function("LogsSource")]
        public Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req)
        {
            logger.LogInformation("C# HTTP trigger function processed a request.");

            if (req.Query.TryGetValue("dummyRequest", out _))
            {
                string[] responseMessage = [$"HttpSource_value:{Guid.NewGuid()}"];
                return Task.FromResult((IActionResult)new OkObjectResult(responseMessage));
            }
            else
            {
                var connectionString = configuration.GetConnectionString("CosmosDb");
                using var client = new CosmosClient(connectionString: connectionString);
                var dbName = configuration.GetValue<string>("CosmosDbLogsDatabase");
                var containerName = configuration.GetValue<string>("CosmosDbLogsContainer");
                var container = client.GetContainer(dbName, containerName);
                var querable = container.GetItemLinqQueryable<dynamic>(allowSynchronousQueryExecution: true).ToList();
                var messages = querable.Select(i => (string)i.message.ToString()).ToArray();

                return Task.FromResult((IActionResult)new OkObjectResult(messages));
            }
        }
    }
}
