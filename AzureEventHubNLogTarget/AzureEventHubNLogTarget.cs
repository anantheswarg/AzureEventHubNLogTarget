using System.Text;
using Microsoft.Azure.EventHubs;
using NLog;
using NLog.Config;
using NLog.Targets;

namespace AzureEventHubNLogTarget
{
    [Target("AzureEventHubNLogTarget")]
    public class AzureEventHubNLogTarget : TargetWithLayout
    {
        private IEventHubSender eventHubManager;

        [RequiredParameter]
        public string EventHubConnectionString { get; set; }

        [RequiredParameter]
        public string EventHubPath { get; set; }

        public string PartitionKey { get; set; }

        /// <summary>
        /// Takes the contents of the LogEvent and sends the message to EventHub
        /// </summary>
        /// <param name="logEvent"></param>
        protected override void Write(LogEventInfo logEvent)
        {
            if (eventHubManager == null)
            {
                eventHubManager = new EventHubSender(EventHubConnectionString, EventHubPath, PartitionKey);
            }

            string logMessage = this.Layout.Render(logEvent);

            using (var eventHubData = new EventData(Encoding.UTF8.GetBytes(logMessage)))
            {
                foreach (var key in logEvent.Properties.Keys)
                {
                    eventHubData.Properties.Add(key.ToString(), logEvent.Properties[key]);
                }

                //not await - fire and forget ??
                eventHubManager.SendMessage(eventHubData).GetAwaiter().GetResult();
            }
        }
    }
}
