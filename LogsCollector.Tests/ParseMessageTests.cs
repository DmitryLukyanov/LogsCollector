using static LogsTransmitterFunction.LogsTransmitterFunction;

namespace LogsCollector.Tests
{
    public sealed class ParseMessageTests
    {
        [Theory]

        [InlineData(
            // actual
            @"Engagement Id: -1 - Title : ""AppName :: UnhandledException"" - Severity :: Information - StackTrace :: ""'Value' property is not set.     at System(Boolean throwIfInvalid)     at System.Windows.SomeLogic()     (..)""",
            // expected
            "-1", "AppName :: UnhandledException", "Information", "'Value' property is not set.     at System(Boolean throwIfInvalid)     at System.Windows.SomeLogic()     (..)")]

        [InlineData(
            // actual
            @"Engagement Id: 1 - Title : ""AppName :: UnhandledException"" - Severity :: Information - StackTrace :: ""'Value' property is not set.     at System(Boolean throwIfInvalid)     at System.Windows.SomeLogic()     (..)""",
            // expected
            "1", "AppName :: UnhandledException", "Information", "'Value' property is not set.     at System(Boolean throwIfInvalid)     at System.Windows.SomeLogic()     (..)")]

        [InlineData(
            // actual
            @"Engagement Id: 0 - Title : ""AppToApp "" - Severity :: Information - StackTrace :: ""Agent Started On , 7/15/2000 0:00:00 PM""",
            // expected
            "0", "AppToApp", "Information", "Agent Started On , 7/15/2000 0:00:00 PM")]

        [InlineData(
            // actual
            @"Engagement Id: 0 - Title : ""/API/Get (Request)"" - Severity :: Information - StackTrace :: "" Request  ::  AuthToken = value :::  {      \""Id\"":\""0\"",      \""Id2\"":\""2\"",      \""Id3\"":\""3\"",      \""Year\"":\""2025\"",      \""Id4\"":\""4\"",      \""Id5\"":\""5\""  }""",
            // expected
            "0", @"/API/Get (Request)", "Information", @"Request  ::  AuthToken = value :::  {      \""Id\"":\""0\"",      \""Id2\"":\""2\"",      \""Id3\"":\""3\"",      \""Year\"":\""2025\"",      \""Id4\"":\""4\"",      \""Id5\"":\""5\""  }")]

        [InlineData(
            // actual
            @"Engagement Id: 1 - Title : ""Library"" - Severity :: Error - StackTrace :: ""The operation has timed out in steps : """,
            // expected
            "1", @"Library", "Error", @"The operation has timed out in steps :")]
        public void Income_message_must_recognize_the_common_message_schema(
            string incomeMessage, 
            string expectedEngagementId, 
            string expectedTitle, 
            string expectedSeverity, 
            string expectedStackTrace)
        {
            var logLine = new LogLine { Message = incomeMessage };

            var parsedMessage = LogsTransmitterFunction.LogsTransmitterFunction.ParseMessage(logLine);

            Assert.Equal(expectedEngagementId, parsedMessage.EngagementId);
            Assert.Equal(expectedTitle, parsedMessage.Title);
            Assert.Equal(expectedSeverity, parsedMessage.Severity);
            Assert.Equal(expectedStackTrace, parsedMessage.StackTrace);
        }
    }
}
