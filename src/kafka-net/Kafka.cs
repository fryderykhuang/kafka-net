using KafkaNet.Common;
using KafkaNet.Protocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaNet
{
    // TODO: Consider renaming this
    public static class Kafka
    {
        public const long INFINITY = long.MaxValue;
        public const long ENDOFTOPIC = -1L;
        /// <summary>
        /// Returns the offset just past the end of the topic.
        /// e.g. 0 for new topics.
        /// The number represents the offset for the next message (if/when it arrives)
        /// </summary>
        public const int HEAD = -1;
        public const int TAIL = -2;

        class Cursor
        {
            public long NextOffset;
        }

        public static async Task<OffsetResponse> GetPartitionOffsetAsync(BrokerRouter router, string topic, int partitionId, int maxResults, int time)
        {
            var request = new OffsetRequest
            {
                Offsets = new List<Offset>
                {
                    new Offset
                    {
                        Topic = topic,
                        PartitionId = partitionId,
                        MaxOffsets = maxResults,
                        Time = time
                    }
                },
            };
            var route = await router.SelectBrokerRouteAsync(topic, partitionId);
            var result = await route.Connection.SendAsync(request).ConfigureAwait(false);
            return result.First();
        }

        /// <summary>
        /// Returns the offset just past the end of the topic.
        /// e.g. 0 for new topics.
        /// The number represents the offset for the next message (if/when it arrives)
        /// </summary>
        public static async Task<long> GetPartitionHeadOffsetAsync(BrokerRouter router, string topic, int partitionId)
        {
            var offsets = await GetPartitionOffsetAsync(router, topic, partitionId, 1, HEAD).ConfigureAwait(false);
            return offsets.Offsets.FirstOrDefault();
        }

        public static async Task<long> GetPartitionTailOffsetAsync(BrokerRouter router, string topic, int partitionId)
        {
            var offsets = await GetPartitionOffsetAsync(router, topic, partitionId, 1, TAIL).ConfigureAwait(false);
            return offsets.Offsets.FirstOrDefault();
        }


        /// <summary>
        /// Returns an observable which will connect to the broker for each subscription
        /// 
        /// Subscriber is blocking, so if you want this to run in a background thread, you should use SubscribeOn()
        /// </summary>
        /// <param name="router"></param>
        /// <param name="topicName"></param>
        /// <param name="partitionId"></param>
        /// <param name="fromOffset"></param>
        /// <param name="toOffsetExcl"></param>
        /// <param name="cancel"></param>
        /// <returns></returns>
        public static IObservable<FetchResponse> CreatePartitionObservable(BrokerRouter router, string topicName, int partitionId, long fromOffset, long toOffsetExcl, CancellationToken cancel = default(CancellationToken))
        {
            return Observable.Create<FetchResponse>(async observer =>
            {
                CancellationDisposable disposable = new CancellationDisposable();
                //CancellationTokenSource disposal = new CancellationTokenSource();
                var combined = cancel == default(CancellationToken)
                    ? disposable.Token
                    : CancellationTokenSource.CreateLinkedTokenSource(disposable.Token, cancel).Token;

                await ConsumePartitionAsync(
                    router: router,
                    topicName: topicName,
                    partitionId: partitionId,
                    fromOffset: fromOffset,
                    toOffsetExcl: toOffsetExcl,
                    onNext: observer.OnNext,
                    onComplete: observer.OnCompleted,
                    onError: observer.OnError,
                    cancel: combined);

                return disposable;
            });
        }

        public class PartitionFetchConnection : IDisposable
        {
            private readonly int partitionId;
            private readonly BrokerRouter router;
            private readonly string topicName;
            private IKafkaConnection connection;

            public PartitionFetchConnection(BrokerRouter router, string topicName, int partitionId)
            {
                this.router = router;
                this.topicName = topicName;
                this.partitionId = partitionId;
            }

            private async Task RefreshRoutes()
            {
                await router.RefreshTopicMetadataAsync();
                await Connect();
            }

            private async Task Connect()
            {
                var route = await router.SelectBrokerRouteAsync(topicName, partitionId);
                connection = router.CloneConnectionForFetch(route.Connection);
            }

            public async Task<FetchResponse> FetchAsync(bool wait, long fromOffset, CancellationToken cancel = default(CancellationToken))
            {
                var bufferSizeHighWatermark = FetchRequest.DefaultBufferSize;

                for (;;)
                {
                    //build a fetch request for partition at offset
                    var fetch = new Fetch
                    {
                        Topic = topicName,
                        PartitionId = partitionId,
                        Offset = fromOffset,
                        MaxBytes = bufferSizeHighWatermark,
                    };

                    var fetches = new List<Fetch> { fetch };

                    var fetchRequest = new FetchRequest
                    {
                        MaxWaitTime = int.MaxValue,
                        MinBytes = wait ? 1 : 0,
                        Fetches = fetches
                    };

                    var responses = await connection.SendAsync(fetchRequest, cancel).ConfigureAwait(false);

                    if (responses.Count == 0)
                    {
                        // something went wrong, refresh the route before trying again
                        await RefreshRoutes();
                        continue;
                    }

                    var response = responses.First();

                    switch ((ErrorResponseCode)response.Error)
                    {
                        case ErrorResponseCode.NoError:
                            break;
                        case ErrorResponseCode.OffsetOutOfRange:
                            throw new OffsetOutOfRangeException("FetchResponse indicated we requested an offset that is out of range.  Requested Offset:{0}", fetchRequest.Fetches[0].Offset) { FetchRequest = fetchRequest.Fetches[0] };
                        case ErrorResponseCode.BrokerNotAvailable:
                        case ErrorResponseCode.ConsumerCoordinatorNotAvailableCode:
                        case ErrorResponseCode.LeaderNotAvailable:
                        case ErrorResponseCode.NotLeaderForPartition:
                            await RefreshRoutes();
                            continue;
                        default:
                            throw new KafkaApplicationException("FetchResponse returned error condition.  ErrorCode:{0}", response.Error) { ErrorCode = response.Error };
                    }

                    return response;
                }
            }

            public static async Task<PartitionFetchConnection> Connect(BrokerRouter router, string topicName, int partitionId)
            {
                var cxn = new PartitionFetchConnection(router, topicName, partitionId);
                await cxn.Connect();
                return cxn;
            }

            public void Dispose()
            {
                if (connection != null)
                    connection.Dispose();
            }
        }

        public static async Task ConsumePartitionAsync(BrokerRouter router, string topicName, int partitionId, long fromOffset, long toOffsetExcl, Action<FetchResponse> onNext, Action onComplete, Action<Exception> onError, CancellationToken cancel = default(CancellationToken))
        {
            // This is the loop that continuously gets the broker for the selected topic and partition
            // Under normal conditions it runs only once, unless the broker situation changes
            var cursor = new Cursor { NextOffset = fromOffset };
            for (;;)
            {
                var topics = await router.GetTopicMetadataAsync(topicName);
                if (topics.Count <= 0)
                    throw new ApplicationException(string.Format("Unable to get metadata for topic:{0}.", topicName));

                //make request and post to queue
                var route = await router.SelectBrokerRouteAsync(topicName, partitionId);
                using (var connection = router.CloneConnectionForFetch(route.Connection))
                {
                    bool end = await ConsumePartitionAsync(connection, topicName, partitionId, cursor, toOffsetExcl, onNext, onComplete, onError, cancel).ConfigureAwait(false);
                    if (end)
                        break;
                }

                await router.RefreshTopicMetadataAsync(topicName);
            }
        }

        private static async Task<bool> ConsumePartitionAsync(IKafkaConnection connection, string topicName, int partitionId, Cursor cursor, long toOffsetExcl, Action<FetchResponse> onNext, Action onComplete, Action<Exception> onError, CancellationToken cancel = default(CancellationToken))
        {
            var bufferSizeHighWatermark = FetchRequest.DefaultBufferSize;

            // we don't know the topic high water mark yet, so initialize to -1
            // when we make the first request, we won't set a minimum byte count so that if there are no messages at all, we still get a response
            // the response will tell us the high water mark and we can exit if this is nonblocking, or continue to wait forever otherwise
            long topicHighWaterMark = -1;

            for (;;)
            {

                if ((toOffsetExcl == ENDOFTOPIC && cursor.NextOffset == topicHighWaterMark) || (toOffsetExcl > 0 && cursor.NextOffset >= toOffsetExcl))
                {
                    onComplete();
                    return true; // we're done
                }

                //build a fetch request for partition at offset
                var fetch = new Fetch
                {
                    Topic = topicName,
                    PartitionId = partitionId,
                    Offset = cursor.NextOffset,
                    MaxBytes = bufferSizeHighWatermark,
                };

                var fetches = new List<Fetch> { fetch };

                var fetchRequest = new FetchRequest
                {
                    MaxWaitTime = int.MaxValue,
                    // if we toOffset is ENDOFTOPIC, then we don't want to wait at the end of the topic
                    // but if topicHighWaterMark is -1, then we don't know if there is any data to read yet
                    // So if both of these are true, don't wait for any data
                    MinBytes = (toOffsetExcl == ENDOFTOPIC && topicHighWaterMark == -1) ? 0 : 1,
                    Fetches = fetches
                };

                List<FetchResponse> responses;
                try
                {
                    responses = await connection.SendAsync(fetchRequest, cancel).ConfigureAwait(false);
                }
                catch (OperationCanceledException e)
                {
                    onError(e);
                    return true;
                }
                catch (Exception)
                {
                    return false;
                }

                if (responses.Count == 0) // something went wrong, refresh the route before trying again
                    return false;

                var response = responses.First();

                switch ((ErrorResponseCode)response.Error)
                {
                    case ErrorResponseCode.NoError:
                        break;
                    case ErrorResponseCode.OffsetOutOfRange:
                        onError(new OffsetOutOfRangeException("FetchResponse indicated we requested an offset that is out of range.  Requested Offset:{0}", fetchRequest.Fetches[0].Offset) { FetchRequest = fetchRequest.Fetches[0] });
                        return true;
                    case ErrorResponseCode.BrokerNotAvailable:
                    case ErrorResponseCode.ConsumerCoordinatorNotAvailableCode:
                    case ErrorResponseCode.LeaderNotAvailable:
                    case ErrorResponseCode.NotLeaderForPartition:
                        return false;
                    default:
                        onError(new KafkaApplicationException("FetchResponse returned error condition.  ErrorCode:{0}", response.Error) { ErrorCode = response.Error });
                        return true;
                }

                topicHighWaterMark = response.HighWaterMark;

                onNext(response);

                if (response.Messages.Count > 0)
                    cursor.NextOffset = response.Messages[response.Messages.Count - 1].Meta.Offset + 1;

            }
        }

        public static async Task<IKafkaConnection> ConnectAsync(BrokerRouter router, string topicName, int partitionId, TimeSpan? responseTimeoutMs = null)
        {
            BrokerRoute route = await router.SelectBrokerRouteAsync(topicName, partitionId);
            return router.CloneConnection(route.Connection, responseTimeoutMs ?? TimeSpan.MaxValue);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="topicName"></param>
        /// <param name="partitionId"></param>
        /// <param name="acks">Indicates how many acknowledgements the servers should receive before responding to the request. If it is 0 the server will not send any response (this is the only case where the server will not reply to a request). If it is 1, the server will wait the data is written to the local log before sending a response. If it is -1 the server will block until the message is committed by all in sync replicas before sending a response. For any number > 1 the server will block waiting for this number of acknowledgements to occur (but the server will never wait for more acknowledgements than there are in-sync replicas)</param>
        /// <param name="cancel"></param>
        /// <returns></returns>
        public static async Task<ProduceResponse[]> ProduceAsync(
            IKafkaConnection connection,
            short acks,
            TopicPayload[] payloads,
            TimeSpan timeout = default(TimeSpan),
            CancellationToken cancel = default(CancellationToken)
            )
        {
            if (timeout.Equals(default(TimeSpan)))
                timeout = TimeSpan.FromMinutes(1);
            var request = new ProduceRequest
            {
                Acks = acks,
                TimeoutMS = (int)timeout.TotalMilliseconds,
                TopicPayloads = payloads
            };
            var response = await connection.SendAsync(request, cancel);
            return response.ToArray();
        }
    }
}
