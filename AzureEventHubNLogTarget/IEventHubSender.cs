using Microsoft.Azure.EventHubs;
using System.Threading.Tasks;

namespace AzureEventHubNLogTarget
{
    public interface IEventHubSender
    {
        Task SendMessage(string message);
        Task SendMessage(string message, string eventHubName);
        Task SendMessage(EventData eventHubData);
        EventData ToEventData(dynamic eventObject, out int payloadSize);
    }
}