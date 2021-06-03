using System;
using Microsoft.Azure.EventHubs;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace AzureEventHubNLogTarget
{
    public class EventHubSender: IEventHubSender
    {

        private PartitionSender _partitionSender;
        private readonly string _eventHubConnectionString;
        private string _eventHubName;
        private readonly string _partitionKey;

        private readonly EventHubClient eventHubClient;

        public EventHubSender(string eventHubConnectionString, string eventHubName, string partitionKey = null, TransportType transportType = TransportType.AmqpWebSockets)
        {
            this._eventHubConnectionString = eventHubConnectionString;
            this._eventHubName = eventHubName;
            this._partitionKey = partitionKey;

            var connectionStringBuilder = new EventHubsConnectionStringBuilder(_eventHubConnectionString)
            {
                EntityPath = _eventHubName,
                TransportType = transportType
            };

            if (string.IsNullOrEmpty(_partitionKey))
            {
                // Creates an EventHubsConnectionStringBuilder object from the connection string, and sets the EntityPath.
                // Typically, the connection string should have the entity path in it, but this simple scenario
                // uses the connection string from the namespace.

                this.eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            }
            else
            {
                this.eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

                this._partitionSender = eventHubClient.CreatePartitionSender(_partitionKey);
            }
        }

        public EventHubSender(string eventHubConnectionString, string partitionKey = null, TransportType transportType = TransportType.AmqpWebSockets)
        {
            this._eventHubConnectionString = eventHubConnectionString;
            this._partitionKey = partitionKey;

            var connectionStringBuilder = new EventHubsConnectionStringBuilder(_eventHubConnectionString)
            {
                EntityPath = _eventHubName,
                TransportType = transportType
            };

            if (string.IsNullOrEmpty(_partitionKey))
            {
                // Creates an EventHubsConnectionStringBuilder object from the connection string, and sets the EntityPath.
                // Typically, the connection string should have the entity path in it, but this simple scenario
                // uses the connection string from the namespace.

                this.eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

            }
            else
            {
                this.eventHubClient = EventHubClient.CreateFromConnectionString(connectionStringBuilder.ToString());

                this._partitionSender = eventHubClient.CreatePartitionSender(_partitionKey);
            }
        }

        public async Task SendMessage(string message)
        {
            await SendMessage(new EventData(Encoding.UTF8.GetBytes(message)));
        }

        public async Task SendMessage(string message, string eventHubName)
        {
            this._eventHubName = eventHubName;
            await SendMessage(new EventData(Encoding.UTF8.GetBytes(message)));
        }

        public async Task SendMessage(EventData eventHubData)
        {
            

            if (string.IsNullOrEmpty(_partitionKey))
            {                
                await eventHubClient.SendAsync(eventHubData);
            }
            else
            {
                await _partitionSender.SendAsync(eventHubData);
            }

        }

        public EventData ToEventData(dynamic eventObject, out int payloadSize)
        {
            string json = Convert.ToString(eventObject);
            payloadSize = Encoding.UTF8.GetByteCount(json);
            var payload = Encoding.UTF8.GetBytes(json);
            var eventData = new EventData(payload);
            return eventData;
        }
    }
}
