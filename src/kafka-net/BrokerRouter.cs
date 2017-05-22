using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using KafkaNet.Model;
using KafkaNet.Protocol;
using System.Threading.Tasks;
using Nito.AsyncEx;

namespace KafkaNet
{
    /// <summary>
    /// This class provides an abstraction from querying multiple Kafka servers for Metadata details and caching this data.
    /// 
    /// All metadata queries are cached lazily.  If metadata from a topic does not exist in cache it will be queried for using
    /// the default brokers provided in the constructor.  Each Uri will be queried to get metadata information in turn until a
    /// response is received.  It is recommended therefore to provide more than one Kafka Uri as this API will be able to to get
    /// metadata information even if one of the Kafka servers goes down.
    /// 
    /// The metadata will stay in cache until an error condition is received indicating the metadata is out of data.  This error 
    /// can be in the form of a socket disconnect or an error code from a response indicating a broker no longer hosts a partition.
    /// </summary>
    public class BrokerRouter : IBrokerRouter
    {
        private readonly AsyncLock _lock = new AsyncLock();
        private readonly KafkaOptions _kafkaOptions;
        private readonly KafkaMetadataProvider _kafkaMetadataProvider;
        private readonly ConcurrentDictionary<KafkaEndpoint, IKafkaConnection> _defaultConnectionIndex = new ConcurrentDictionary<KafkaEndpoint, IKafkaConnection>();
        private readonly ConcurrentDictionary<int, IKafkaConnection> _brokerConnectionIndex = new ConcurrentDictionary<int, IKafkaConnection>();
        private readonly ConcurrentDictionary<string, Topic> _topicIndex = new ConcurrentDictionary<string, Topic>();

        public BrokerRouter(KafkaOptions kafkaOptions)
        {
            _kafkaOptions = kafkaOptions;
            _kafkaMetadataProvider = new KafkaMetadataProvider(_kafkaOptions.Log);

            foreach (var endpoint in _kafkaOptions.KafkaServerEndpoints)
            {
                var conn = _kafkaOptions.KafkaConnectionFactory.Create(endpoint, _kafkaOptions.ResponseTimeoutMs, _kafkaOptions.Log, _kafkaOptions.MaximumReconnectionTimeout);
                _defaultConnectionIndex.AddOrUpdate(endpoint, e => conn, (e, c) => conn);
            }

            if (_defaultConnectionIndex.Count <= 0)
                throw new ServerUnreachableException("None of the provided Kafka servers are resolvable.");
        }

        /// <summary>
        /// Select a broker for a specific topic and partitionId.
        /// </summary>
        /// <param name="topic">The topic name to select a broker for.</param>
        /// <param name="partitionId">The exact partition to select a broker for.</param>
        /// <returns>A broker route for the given partition of the given topic.</returns>
        /// <remarks>
        /// This function does not use any selector criteria.  If the given partitionId does not exist an exception will be thrown.
        /// </remarks>
        /// <exception cref="InvalidTopicMetadataException">Thrown if the returned metadata for the given topic is invalid or missing.</exception>
        /// <exception cref="InvalidPartitionException">Thrown if the give partitionId does not exist for the given topic.</exception>
        /// <exception cref="ServerUnreachableException">Thrown if none of the Default Brokers can be contacted.</exception>
        public async Task<BrokerRoute> SelectBrokerRouteAsync(string topic, int partitionId)
        {
            var cachedTopic = GetCachedTopic(topic);
            if (cachedTopic == null)
            {
                await RefreshTopicMetadataAsync(topic).ConfigureAwait(false);
                cachedTopic = GetCachedTopic(topic);
                if (cachedTopic == null)
                    throw new InvalidTopicMetadataException(ErrorResponseCode.UnknownTopicOrPartition, "The Metadata is invalid as it returned no data for the given topic: {0}", topic);
            }
            var partition = cachedTopic.Partitions.FirstOrDefault(x => x.PartitionId == partitionId);
            if (partition == null)
                throw new InvalidPartitionException("The topic {0} does not have partition {1}", topic, partitionId);

            return TryGetRouteFromCache(topic, partition);
        }

        // TODO: remove old code
#if OLD_IMPLEMENTATION
        public BrokerRoute SelectBrokerRoute(string topic, int partitionId)
        {
            var cachedTopic = GetTopicMetadata(topic);

            if (cachedTopic.Count <= 0)
                throw new InvalidTopicMetadataException(ErrorResponseCode.NoError, "The Metadata is invalid as it returned no data for the given topic:{0}", topic);

            var topicMetadata = cachedTopic.First();

            var partition = topicMetadata.Partitions.FirstOrDefault(x => x.PartitionId == partitionId);
            if (partition == null) throw new InvalidPartitionException(string.Format("The topic:{0} does not have a partitionId:{1} defined.", topic, partitionId));

            return GetCachedRoute(topicMetadata.Name, partition);
        }

#endif

        public IKafkaConnection CloneConnectionForFetch(IKafkaConnection connection)
        {
            return CloneConnection(connection, TimeSpan.MaxValue, _kafkaOptions.MaximumReconnectionTimeout);
        }

        public IKafkaConnection CloneConnectionForProduce(IKafkaConnection connection)
        {
            return CloneConnection(connection, TimeSpan.MaxValue, _kafkaOptions.MaximumReconnectionTimeout);
        }

        public IKafkaConnection CloneConnection(IKafkaConnection connection, TimeSpan responseTimeoutMs, TimeSpan? maximumReconnectionTimeout = null)
        {
            return _kafkaOptions.KafkaConnectionFactory.Create(connection.Endpoint, responseTimeoutMs, _kafkaOptions.Log, maximumReconnectionTimeout);
        }

        /// <summary>
        /// Select a broker for a given topic using the IPartitionSelector function.
        /// </summary>
        /// <param name="topic">The topic to retreive a broker route for.</param>
        /// <param name="key">The key used by the IPartitionSelector to collate to a consistent partition. Null value means key will be ignored in selection process.</param>
        /// <returns>A broker route for the given topic.</returns>
        /// <exception cref="InvalidTopicMetadataException">Thrown if the returned metadata for the given topic is invalid or missing.</exception>
        /// <exception cref="ServerUnreachableException">Thrown if none of the Default Brokers can be contacted.</exception>
        public async Task<BrokerRoute> SelectBrokerRouteAsync(string topic, byte[] key = null)
        {
            //get topic either from cache or server.
            var cachedTopic = (await GetTopicMetadataAsync(topic)).FirstOrDefault();

            if (cachedTopic == null)
                throw new InvalidTopicMetadataException(ErrorResponseCode.NoError, "The Metadata is invalid as it returned no data for the given topic:{0}", topic);

            var partition = _kafkaOptions.PartitionSelector.Select(cachedTopic, key);

            return await GetCachedRouteAsync(cachedTopic.Name, partition);
        }

        /// <summary>
        /// Returns Topic metadata for each topic requested. 
        /// </summary>
        /// <param name="topics">Collection of topics to request metadata for.</param>
        /// <returns>List of Topics as provided by Kafka.</returns>
        /// <remarks>
        /// The topic metadata will by default check the cache first and then if it does not exist it will then
        /// request metadata from the server.  To force querying the metadata from the server use <see cref="RefreshTopicMetadata"/>
        /// </remarks>
        public async Task<List<Topic>> GetTopicMetadataAsync(params string[] topics)
        {
            var topicSearchResult = SearchCacheForTopics(topics);

            //update metadata for all missing topics
            if (topicSearchResult.Missing.Count > 0)
            {
                //double check for missing topics and query
                await RefreshTopicMetadataAsync(topicSearchResult.Missing.Where(x => _topicIndex.ContainsKey(x) == false).ToArray());

                var refreshedTopics = topicSearchResult.Missing.Select(GetCachedTopic).Where(x => x != null);
                topicSearchResult.Topics.AddRange(refreshedTopics);
            }

            return topicSearchResult.Topics;
        }

        /// <summary>
        /// Force a call to the kafka servers to refresh metadata for the given topics.
        /// </summary>
        /// <param name="topics">List of topics to update metadata for.</param>
        /// <remarks>
        /// This method will ignore the cache and initiate a call to the kafka servers for all given topics, updating the cache with the resulting metadata.
        /// Only call this method to force a metadata update.  For all other queries use <see cref="GetTopicMetadata"/> which uses cached values.
        /// </remarks>
        public async Task RefreshTopicMetadataAsync(params string[] topics)
        {
            using (await _lock.LockAsync())
            {
                _kafkaOptions.Log.DebugFormat("BrokerRouter: Refreshing metadata for topics: {0}", string.Join(",", topics));

                //get the connections to query against and get metadata
                var connections = _defaultConnectionIndex.Values.Union(_brokerConnectionIndex.Values).ToArray();
                var metadataResponse = await _kafkaMetadataProvider.GetAsync(connections, topics).ConfigureAwait(false);

                UpdateInternalMetadataCache(metadataResponse);
            }
        }

        private TopicSearchResult SearchCacheForTopics(IEnumerable<string> topics)
        {
            var result = new TopicSearchResult();

            foreach (var topic in topics)
            {
                var cachedTopic = GetCachedTopic(topic);

                if (cachedTopic == null)
                    result.Missing.Add(topic);
                else
                    result.Topics.Add(cachedTopic);
            }

            return result;
        }

        private Topic GetCachedTopic(string topic)
        {
            return _topicIndex.TryGetValue(topic, out Topic cachedTopic) ? cachedTopic : null;
        }

        private async Task<BrokerRoute> GetCachedRouteAsync(string topic, Partition partition)
        {
            var route = TryGetRouteFromCache(topic, partition);

            //leader could not be found, refresh the broker information and try one more time
            if (route == null)
            {
                await RefreshTopicMetadataAsync(topic);
                route = TryGetRouteFromCache(topic, partition);
            }

            if (route != null) return route;

            throw new LeaderNotFoundException(string.Format("Lead broker cannot be found for parition: {0}, leader: {1}", partition.PartitionId, partition.LeaderId));
        }

        private BrokerRoute TryGetRouteFromCache(string topic, Partition partition)
        {
            if (_brokerConnectionIndex.TryGetValue(partition.LeaderId, out IKafkaConnection conn))
            {
                return new BrokerRoute
                {
                    Topic = topic,
                    PartitionId = partition.PartitionId,
                    Connection = conn
                };
            }

            return null;
        }
        
        private void UpdateInternalMetadataCache(MetadataResponse metadata)
        {

            //resolve each broker
            var brokerEndpoints = metadata.Brokers.Select(broker => new
            {
                Broker = broker,
                Endpoint = _kafkaOptions.KafkaConnectionFactory.Resolve(broker.Address, _kafkaOptions.Log)
            });

            foreach (var broker in brokerEndpoints)
            {
                //if the connection is in our default connection index already, remove it and assign it to the broker index.
                if (_defaultConnectionIndex.TryRemove(broker.Endpoint, out IKafkaConnection connection))
                {
                    UpsertConnectionToBrokerConnectionIndex(broker.Broker.BrokerId, connection);
                }
                else if (!_brokerConnectionIndex.TryGetValue(broker.Broker.BrokerId, out connection) || !connection.Endpoint.Equals(broker.Endpoint))
                {
                    connection = _kafkaOptions.KafkaConnectionFactory.Create(broker.Endpoint, _kafkaOptions.ResponseTimeoutMs, _kafkaOptions.Log);
                    UpsertConnectionToBrokerConnectionIndex(broker.Broker.BrokerId, connection);
                }
            }

            foreach (var topic in metadata.Topics)
            {
                var localTopic = topic;
                _topicIndex.AddOrUpdate(topic.Name, s => localTopic, (s, existing) => localTopic);
            }
        }

        private void UpsertConnectionToBrokerConnectionIndex(int brokerId, IKafkaConnection newConnection)
        {
            //associate the connection with the broker id, and add or update the reference
            _brokerConnectionIndex.AddOrUpdate(brokerId,
                    i => newConnection,
                    (i, existingConnection) =>
                    {
                        //if a connection changes for a broker close old connection and create a new one
                        if (existingConnection.Endpoint.Equals(newConnection.Endpoint)) return existingConnection;
                        _kafkaOptions.Log.WarnFormat("Broker:{0} Uri changed from:{1} to {2}", brokerId, existingConnection.Endpoint, newConnection.Endpoint);
                        using (existingConnection)
                        {
                            return newConnection;
                        }
                    });
        }

        public void Dispose()
        {
            _defaultConnectionIndex.Values.ToList().ForEach(conn => { using (conn) { } });
            _brokerConnectionIndex.Values.ToList().ForEach(conn => { using (conn) { } });
        }
    }

    #region BrokerCache Class...
    public class TopicSearchResult
    {
        public List<Topic> Topics { get; set; }
        public List<string> Missing { get; set; }

        public TopicSearchResult()
        {
            Topics = new List<Topic>();
            Missing = new List<string>();
        }
    }
    #endregion
}
