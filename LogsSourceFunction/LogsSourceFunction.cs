using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace LogsSourceFunction
{
    // TODO: move to isolated process flow!
    public static class LogsSourceFunction
    {
        [FunctionName("LogsSource")]
        public static Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            // Emulate retriving data, for example SQL result
            string responseMessage = $"HttpSource_value:{Guid.NewGuid().ToString()}";

            return Task.FromResult((IActionResult)new OkObjectResult(responseMessage));
        }
    }
}
