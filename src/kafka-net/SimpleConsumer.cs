using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KafkaNet.Model;
using KafkaNet.Protocol;

namespace KafkaNet
{
    /// <summary>
    /// Provides a basic consumer of one Topic across all partitions or over a given whitelist of partitions.
    /// 
    /// TODO: provide automatic offset saving when the feature is available in 0.8.2
    /// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-OffsetCommit/FetchAPI
    /// </summary>
    public class SimpleConsumer : IMetadataQueries, IDisposable
    {
        private readonly ConsumerOptions _options;
        private readonly int _partition;
        private readonly BlockingCollection<FetchResponse> _fetchResponseQueue;
        private readonly CancellationTokenSource _disposeToken = new CancellationTokenSource();
        private Task _partitionPolling;
        private long _partitionOffset;
        private readonly IMetadataQueries _metadataQueries;

        private int _disposeCount;
        private Topic _topic;

        private static readonly FetchResponse EndOfTopic = new FetchResponse();

        public SimpleConsumer(ConsumerOptions options, int partition, long position = 0)
        {
            _partition = partition;
            _options = options;
            _fetchResponseQueue = new BlockingCollection<FetchResponse>(_options.ConsumerBufferSize);
            _metadataQueries = new MetadataQueries(_options.Router);
            
            SetOffsetPosition(position);
        }

        /// <summary>
        /// Returns a blocking enumerable of messages received from Kafka.
        /// </summary>
        /// <returns>Blocking enumberable of messages from Kafka.</returns>
        public async Task<IEnumerable<FetchResponse>> ConsumeAsync(CancellationToken? cancellationToken = null)
        {
            _options.Log.DebugFormat("Consumer: Beginning consumption of topic: {0}", _options.Topic);
            await EnsurePartitionPollingThreadsAsync();
            return Consume(cancellationToken);
        }

        private IEnumerable<FetchResponse> Consume(CancellationToken? cancellationToken)
        {
            foreach (var response in _fetchResponseQueue.GetConsumingEnumerable(cancellationToken ?? CancellationToken.None))
            {
                if (Object.ReferenceEquals(response, EndOfTopic))
                {
                    yield break;
                }
                yield return response;
            }
        }

        /// <summary>
        /// Force reset the offset position for a specific partition to a specific offset value.
        /// </summary>
        /// <param name="positions">Collection of positions to reset to.</param>
        public void SetOffsetPosition(long position)
        {
            _partitionOffset = position;
        }

        /// <summary>
        /// Get the current running position (offset) for all consuming partition.
        /// </summary>
        /// <returns>List of positions for each consumed partitions.</returns>
        /// <remarks>Will only return data if the consumer is actively being consumed.</remarks>
        public long GetOffsetPosition()
        {
            return _partitionOffset;
        }

        private async Task EnsurePartitionPollingThreadsAsync()
        {
            try
            {
                _options.Log.DebugFormat("Consumer: Refreshing partitions for topic: {0}", _options.Topic);
                var topic = await _options.Router.GetTopicMetadataAsync(_options.Topic);
                if (topic.Count <= 0) throw new ApplicationException(string.Format("Unable to get metadata for topic:{0}.", _options.Topic));
                _topic = topic.First();

                //create one thread per partition, if they are in the white list.
                foreach (var partition in _topic.Partitions)
                {
                    var partitionId = partition.PartitionId;
                    if (_partition == partitionId)
                    {
                        _partitionPolling = ConsumeTopicPartitionAsync(_topic.Name, partitionId);
                        break;
                    }
                }
            }
            catch (Exception ex)
            {
                _options.Log.ErrorFormat("Exception occured trying to setup consumer for topic:{0}.  Exception={1}", _options.Topic, ex);
            }
        }

        private Task ConsumeTopicPartitionAsync(string topic, int partitionId)
        {
            return Task.Run(async () =>
            {
                try
                {
                    var bufferSizeHighWatermark = FetchRequest.DefaultBufferSize;

                    _options.Log.DebugFormat("Consumer: Creating polling task for topic: {0} on parition: {1}", topic, partitionId);
                    while (_disposeToken.IsCancellationRequested == false)
                    {
                        try
                        {
                            //get the current offset, or default to zero if not there.
                            long offset = _partitionOffset;

                            //build a fetch request for partition at offset
                            var fetch = new Fetch
                            {
                                Topic = topic,
                                PartitionId = partitionId,
                                Offset = offset,
                                MaxBytes = bufferSizeHighWatermark,
                            };

                            var fetches = new List<Fetch> { fetch };

                            var fetchRequest = new FetchRequest
                                {
                                    MaxWaitTime = (int)Math.Min((long)int.MaxValue, _options.MaxWaitTimeForMinimumBytes.TotalMilliseconds),
                                    MinBytes = _options.MinimumBytes,
                                    Fetches = fetches
                                };

                            //make request and post to queue
                            var route = await _options.Router.SelectBrokerRouteAsync(topic, partitionId);

                            var responses = await route.Connection.SendAsync(fetchRequest).ConfigureAwait(false);

                            if (responses.Count > 0)
                            {
                                var response = responses.FirstOrDefault(); //we only asked for one response

                                if (response != null)
                                {
                                    HandleResponseErrors(fetch, response);

                                    if (response.Messages.Count > 0)
                                    {
                                        _fetchResponseQueue.Add(response, _disposeToken.Token);

                                        var nextOffset = response.Messages.Max(x => x.Meta.Offset) + 1;
                                        _partitionOffset = nextOffset;

                                        // sleep is not needed if responses were received
                                        continue;
                                    }
                                    else if (_options.BackoffInterval.TotalMilliseconds == 0 && _options.MaxWaitTimeForMinimumBytes.TotalMilliseconds == 0)
                                    {
                                        // we've reached the end of the partition and this consumer's configuration indicates that
                                        // blocking is not expected.  Send the EndOfTopic message
                                        _fetchResponseQueue.Add(EndOfTopic);
                                        return;
                                    }
                                }
                            }

                            //no message received from server wait a while before we try another long poll
                            Thread.Sleep(_options.BackoffInterval);
                        }
                        catch (BufferUnderRunException ex)
                        {
                            bufferSizeHighWatermark = (int)(ex.RequiredBufferSize * _options.FetchBufferMultiplier) + ex.MessageHeaderSize;
                            _options.Log.InfoFormat("Buffer underrun.  Increasing buffer size to: {0}", bufferSizeHighWatermark);
                        }
                        catch (OffsetOutOfRangeException ex)
                        {
                            //TODO this turned out really ugly.  Need to fix this section.
                            _options.Log.ErrorFormat(ex.Message);
                            //FixOffsetOutOfRangeExceptionAsync(ex.FetchRequest);
                        }
                        catch (InvalidMetadataException ex)
                        {
                            //refresh our metadata and ensure we are polling the correct partitions
                            _options.Log.ErrorFormat(ex.Message);
                            await _options.Router.RefreshTopicMetadataAsync(topic);
                            await EnsurePartitionPollingThreadsAsync();
                        }
                        catch (Exception ex)
                        {
                            _options.Log.ErrorFormat("Exception occured while polling topic:{0} partition:{1}.  Polling will continue.  Exception={2}", topic, partitionId, ex);
                        }
                    }
                }
                finally
                {
                    _options.Log.DebugFormat("Consumer: Disabling polling task for topic: {0} on parition: {1}", topic, partitionId);
                    _partitionPolling = null;
                }
            });
        }

        private void HandleResponseErrors(Fetch request, FetchResponse response)
        {
            switch ((ErrorResponseCode)response.Error)
            {
                case ErrorResponseCode.NoError:
                    return;
                case ErrorResponseCode.OffsetOutOfRange:
                    throw new OffsetOutOfRangeException("FetchResponse indicated we requested an offset that is out of range.  Requested Offset:{0}", request.Offset) { FetchRequest = request };
                case ErrorResponseCode.BrokerNotAvailable:
                case ErrorResponseCode.ConsumerCoordinatorNotAvailableCode:
                case ErrorResponseCode.LeaderNotAvailable:
                case ErrorResponseCode.NotLeaderForPartition:
                    throw new InvalidMetadataException("FetchResponse indicated we may have mismatched metadata.  ErrorCode:{0}", response.Error) { ErrorCode = response.Error };
                default:
                    throw new KafkaApplicationException("FetchResponse returned error condition.  ErrorCode:{0}", response.Error) { ErrorCode = response.Error };
            }
        }

        public Task<Topic> GetTopicAsync(string topic)
        {
            return _metadataQueries.GetTopicAsync(topic);
        }

        public Task<List<OffsetResponse>> GetTopicOffsetAsync(string topic, int maxOffsets = 2, int time = -1)
        {
            return _metadataQueries.GetTopicOffsetAsync(topic, maxOffsets, time);
        }

        public void Dispose()
        {
            if (Interlocked.Increment(ref _disposeCount) != 1) return;

            _options.Log.DebugFormat("Consumer: Disposing...");
            _disposeToken.Cancel();

            //wait for all threads to unwind
            if (_partitionPolling != null)
                _partitionPolling.Wait();

            using (_metadataQueries)
            using (_disposeToken)
            { }
        }
    }
}
