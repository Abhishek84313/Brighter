using System;
using Confluent.Kafka;
using Paramore.Brighter.MessagingGateway.Kafka;
using Xunit;

namespace Paramore.Brighter.Kafka.Tests.MessagingGateway.Reactor;

[Trait("Category", "Kafka")]
[Collection("Kafka")] //
public class KafkaTimeStampRoundTripTests
{
    //Deliberately not at UTC: an offset the writer must not drop, so this fails on a UTC host too
    private static readonly DateTimeOffset s_timeStamp = new(2024, 6, 15, 13, 45, 30, TimeSpan.FromHours(5));

    private readonly KafkaDefaultMessageHeaderBuilder _builder = new();
    private readonly Message _message;

    public KafkaTimeStampRoundTripTests()
    {
        //arrange
        _message = new Message(
            new MessageHeader(
                messageId: Guid.NewGuid().ToString(),
                topic: new RoutingKey("test"),
                messageType: MessageType.MT_COMMAND,
                timeStamp: s_timeStamp),
            new MessageBody("test content")
        );
    }

    [Fact]
    public void When_round_tripping_a_timestamp_should_preserve_the_instant_across_hops()
    {
        //act
        Headers firstHopHeaders = _builder.Build(_message);
        Message firstHop = new KafkaMessageCreator().CreateMessage(ConsumeResultFor(firstHopHeaders));

        //assert
        Assert.Equal(s_timeStamp, firstHop.Header.TimeStamp);
        Assert.Equal(s_timeStamp.ToUniversalTime().DateTime, firstHop.Header.TimeStamp.ToUniversalTime().DateTime);
        Assert.Equal(TimeSpan.Zero, firstHop.Header.TimeStamp.Offset);

        //act - a requeue re-publishes what we read
        Headers secondHopHeaders = _builder.Build(firstHop);

        //assert - identical bytes, so drift cannot accumulate over hops
        Assert.Equal(firstHopHeaders.GetLastBytes(HeaderNames.TIMESTAMP),
            secondHopHeaders.GetLastBytes(HeaderNames.TIMESTAMP));
    }

    private static ConsumeResult<string, byte[]> ConsumeResultFor(Headers headers)
        => new()
        {
            Topic = "test",
            Message = new Message<string, byte[]>
            {
                Headers = headers, Key = Guid.NewGuid().ToString(), Value = "test content"u8.ToArray()
            }
        };
}
