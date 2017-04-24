using Divisions.Logging;
using KafkaNet.Common;
using KafkaNet.Protocol;
using System;
using System.Collections.Generic;
using System.Diagnostics;
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
        private static readonly ILog Log = LogManager.GetLogger(typeof(Kafka));

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

        private enum FetchResultCode
        {
            Ok,
            RefreshAndRetry,
            IncreaseBufferAndRetry,
            End,
        }

        private class FetchResult
        {
            public FetchResultCode Code { get; }
            public FetchResponse Response { get; }
            public Exception Exception { get; }

            public FetchResult(FetchResultCode code, FetchResponse response, Exception exception)
            {
                Code = code;
                Response = response;
                Exception = exception;
            }
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
            var route = await router.SelectBrokerRouteAsync(topic, partitionId).ConfigureAwait(false);
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
            return Observable.Create<FetchResponse>(observer =>
            {
                CancellationDisposable disposable = new CancellationDisposable();
                //CancellationTokenSource disposal = new CancellationTokenSource();
                var combined = cancel == default(CancellationToken)
                    ? disposable.Token
                    : CancellationTokenSource.CreateLinkedTokenSource(disposable.Token, cancel).Token;

                var task = ConsumePartitionAsync(
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
                await router.RefreshTopicMetadataAsync().ConfigureAwait(false);
                await Connect().ConfigureAwait(false);
            }

            private async Task Connect()
            {
                var route = await router.SelectBrokerRouteAsync(topicName, partitionId).ConfigureAwait(false);
                connection = router.CloneConnectionForFetch(route.Connection);
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
            var exceptionBackoff = 5;
            for (;;)
            {
                var topics = await router.GetTopicMetadataAsync(topicName).ConfigureAwait(false);
                if (topics.Count <= 0)
                    throw new ApplicationException(string.Format("Unable to get metadata for topic:{0}.", topicName));

                //make request and post to queue
                var route = await router.SelectBrokerRouteAsync(topicName, partitionId).ConfigureAwait(false);
                using (var connection = router.CloneConnectionForFetch(route.Connection))
                {
                    try
                    {
                        bool end = await ConsumePartitionAsync(connection, topicName, partitionId, cursor, toOffsetExcl, onNext, onComplete, onError, cancel).ConfigureAwait(false);
                        exceptionBackoff = 5;
                        if (end)
                            break;
                    }
                    catch (Exception e)
                    {
                        Log.Error("ConsumePartitionAsync failed unexpectedly, will attempt to recover", e);
                        await Task.Delay(exceptionBackoff);
                        exceptionBackoff = Math.Min(10000, exceptionBackoff * 2);
                    }
                }

                await router.RefreshTopicMetadataAsync(topicName).ConfigureAwait(false);
            }
        }

        const int BufferMax = 1024 * 1024 * 16;

        private static async Task<bool> ConsumePartitionAsync(IKafkaConnection connection, string topicName, int partitionId, Cursor cursor, long toOffsetExcl, Action<FetchResponse> onNext, Action onComplete, Action<Exception> onError, CancellationToken cancel = default(CancellationToken))
        {
            Log.DebugFormat("Consume partition {0}:{1} [{2}..{3}]",
                topicName,
                partitionId,
                cursor.NextOffset,
                toOffsetExcl == ENDOFTOPIC ? "end" : (toOffsetExcl == INFINITY ? "inf" :toOffsetExcl.ToString())
                );
            var bufferSizeHighWatermark = BufferMax;

            // we don't know the topic high water mark yet, so initialize to -1
            // when we make the first request, we won't set a minimum byte count so that if there are no messages at all, we still get a response
            // the response will tell us the high water mark and we can exit if this is nonblocking, or continue to wait forever otherwise
            long topicHighWaterMark = -1;

            // give ourselves the ability to abort a running fetch
            var abort = new CancellationTokenSource();
            cancel = CancellationTokenSource.CreateLinkedTokenSource(abort.Token, cancel).Token;

            var nextOffset = cursor.NextOffset;
            var fetchResultTask = FetchAsync(connection, topicName, partitionId, nextOffset, toOffsetExcl, cancel, bufferSizeHighWatermark, topicHighWaterMark).ConfigureAwait(false);
            try
            {
                for (;;)
                {
                    var fetchResult = await fetchResultTask;
                    switch (fetchResult.Code)
                    {
                        case FetchResultCode.IncreaseBufferAndRetry:
                            bufferSizeHighWatermark = Math.Min(bufferSizeHighWatermark * 2, BufferMax); // 5 MB max
                            Log.InfoFormat("Increasing buffer size to {0}", bufferSizeHighWatermark);
                            fetchResultTask = FetchAsync(connection, topicName, partitionId, nextOffset, toOffsetExcl, cancel, bufferSizeHighWatermark, topicHighWaterMark).ConfigureAwait(false);
                            break;
                        case FetchResultCode.Ok:
                            topicHighWaterMark = fetchResult.Response.HighWaterMark;
                            var response = fetchResult.Response;
                            // go ahead and begin the next fetch
                            var count = response.Messages.Count;
                            if (count > 0)
                                nextOffset = response.Messages[response.Messages.Count - 1].Meta.Offset + 1;
                            fetchResultTask = FetchAsync(connection, topicName, partitionId, nextOffset, toOffsetExcl, cancel, bufferSizeHighWatermark, topicHighWaterMark).ConfigureAwait(false);
                            if (count > 0)
                            {
                                try
                                {
                                    onNext(response);
                                }
                                catch (Exception e)
                                {
                                    Log.Error("Observer did not handle an exception", e);
                                }
                                cursor.NextOffset = nextOffset;
                            }
                            break;
                        case FetchResultCode.RefreshAndRetry:
                            return false;
                        case FetchResultCode.End:
                            if (fetchResult.Exception != null)
                                onError(fetchResult.Exception);
                            else
                                onComplete();
                            return true;
                    }
                }
            }
            finally
            {
                // abort the fetch
                abort.Cancel();
            }
        }

        /// <summary>
        /// If this returns Ok then response contains the response
        /// If this returns FetchAndRetry then exception might contain a reason which you can log, but you should refresh the broker information and call this again
        /// If this returns End then exception might contain a reason why it ended, if not, it's just complete.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="topicName"></param>
        /// <param name="partitionId"></param>
        /// <param name="cursor"></param>
        /// <param name="toOffsetExcl"></param>
        /// <param name="cancel"></param>
        /// <param name="bufferSizeHighWatermark"></param>
        /// <param name="topicHighWaterMark"></param>
        /// <param name="response"></param>
        /// <param name="exception"></param>
        /// <returns></returns>
        private static async Task<FetchResult> FetchAsync(IKafkaConnection connection, string topicName, int partitionId, long startOffset, long toOffsetExcl, CancellationToken cancel, int bufferSizeHighWatermark, long topicHighWaterMark)
        {
            if ((toOffsetExcl == ENDOFTOPIC && startOffset == topicHighWaterMark) || (toOffsetExcl > 0 && startOffset >= toOffsetExcl))
            {
                Log.Debug("Fetch end of stream");
                return new FetchResult(FetchResultCode.End, null, null);
            }

            var sw = new Stopwatch();
            try
            {

                //build a fetch request for partition at offset
                var fetch = new Fetch
                {
                    Topic = topicName,
                    PartitionId = partitionId,
                    Offset = startOffset,
                    MaxBytes = bufferSizeHighWatermark,
                };

                var fetches = new List<Fetch> { fetch };

                var fetchRequest = new FetchRequest
                {
                    MaxWaitTime = 595000, // the default max connection idle time is 600000, so we'll timeout just before that
                    // if we toOffset is ENDOFTOPIC, then we don't want to wait at the end of the topic
                    // but if topicHighWaterMark is -1, then we don't know if there is any data to read yet
                    // So if both of these are true, don't wait for any data
                    MinBytes = (toOffsetExcl == ENDOFTOPIC && topicHighWaterMark == -1) ? 0 : 1,
                    Fetches = fetches
                };

                List<FetchResponse> responses;
                for (;;)
                {
                    try
                    {
                        Log.DebugFormat("Fetch {0}:{1} [{2}..] (min/max: {3}/{4} bytes, wait: {5})",
                            fetch.Topic,
                            fetch.PartitionId,
                            fetch.Offset,
                            fetchRequest.MinBytes,
                            fetch.MaxBytes,
                            fetchRequest.MaxWaitTime
                            );
                        sw.Start();
                        responses = await connection.SendAsync(fetchRequest, cancel).ConfigureAwait(false);
                        break;
                    }
                    catch (BufferUnderRunException)
                    {
                        Log.Debug("Buffer underrun");
                        return new FetchResult(FetchResultCode.IncreaseBufferAndRetry, null, null);
                    }
                    catch (OperationCanceledException e)
                    {
                        Log.Debug("Fetch canceled");
                        return new FetchResult(FetchResultCode.End, null, e);
                    }
                    catch (ResponseTimeoutException e)
                    {
                        sw.Stop();
                        Log.DebugFormat("({0}ms) Request timeout, will re-request", sw.ElapsedMilliseconds);
                    }
                    catch (ServerDisconnectedException e)
                    {
                        Log.Debug("Lost connection to kafka broker", e);
                        return new FetchResult(FetchResultCode.RefreshAndRetry, null, e);
                    }
                    catch (Exception e)
                    {
                        Log.Debug("Fetch error", e);
                        return new FetchResult(FetchResultCode.RefreshAndRetry, null, e);
                    }
                    finally
                    {
                        sw.Stop();
                    }
                }

                if (responses.Count == 0)
                {
                    Log.Warn("Empty response, something went wrong");
                    // something went wrong, refresh the route before trying again
                    return new FetchResult(FetchResultCode.RefreshAndRetry, null, null);
                }

                var response = responses.First();

                switch ((ErrorResponseCode)response.Error)
                {
                    case ErrorResponseCode.NoError:
                        Log.DebugFormat("Fetch: OK {0} msgs", response.Messages?.Count ?? 0);
                        return new FetchResult(FetchResultCode.Ok, response, null);
                    case ErrorResponseCode.OffsetOutOfRange:
                        Log.Debug("Fetch: Out Of Range");
                        return new FetchResult(FetchResultCode.End, null, new OffsetOutOfRangeException("FetchResponse indicated we requested an offset that is out of range.  Requested Offset:{0}", fetchRequest.Fetches[0].Offset) { FetchRequest = fetchRequest.Fetches[0] });
                    case ErrorResponseCode.BrokerNotAvailable:
                    case ErrorResponseCode.ConsumerCoordinatorNotAvailableCode:
                    case ErrorResponseCode.LeaderNotAvailable:
                    case ErrorResponseCode.NotLeaderForPartition:
                        Log.DebugFormat("Fetch: Retry code {0}", response.Error);
                        return new FetchResult(FetchResultCode.RefreshAndRetry, null, null);
                    default:
                        Log.DebugFormat("Fetch: Error code {0}", response.Error);
                        return new FetchResult(FetchResultCode.End, null, new KafkaApplicationException("FetchResponse returned error condition.  ErrorCode:{0}", response.Error) { ErrorCode = response.Error });
                }
            }
            finally
            {
                Log.DebugFormat("({0}ms) Fetch completed", sw.ElapsedMilliseconds);
            }
        }

        public static async Task<IKafkaConnection> ConnectAsync(BrokerRouter router, string topicName, int partitionId, TimeSpan? responseTimeoutMs = null)
        {
            BrokerRoute route = await router.SelectBrokerRouteAsync(topicName, partitionId).ConfigureAwait(false);
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
            var response = await connection.SendAsync(request, cancel).ConfigureAwait(false);
            return response.ToArray();
        }
    }
}
