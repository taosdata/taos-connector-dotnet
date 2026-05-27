using System;
using System.Collections.Generic;

namespace TDengine.Driver
{
    internal static class AdapterClusterRegistry
    {
        private static readonly Dictionary<string, List<FailoverAddress>> KnownClusters =
            new Dictionary<string, List<FailoverAddress>>(StringComparer.OrdinalIgnoreCase);

        private static readonly object SyncLock = new object();

        internal static void RegisterCluster(IReadOnlyList<FailoverAddress> seedAddresses,
            IReadOnlyList<FailoverAddress> fullCluster)
        {
            if (seedAddresses == null || seedAddresses.Count == 0 || fullCluster == null || fullCluster.Count == 0)
            {
                return;
            }

            lock (SyncLock)
            {
                var clusterList = new List<FailoverAddress>(fullCluster);
                for (var i = 0; i < seedAddresses.Count; i++)
                {
                    var seedKey = seedAddresses[i].CacheKey;
                    if (string.IsNullOrWhiteSpace(seedKey))
                    {
                        continue;
                    }

                    KnownClusters[seedKey] = clusterList;
                }

                // Also register all discovered addresses as keys pointing to the same cluster
                for (var i = 0; i < fullCluster.Count; i++)
                {
                    var key = fullCluster[i].CacheKey;
                    if (string.IsNullOrWhiteSpace(key))
                    {
                        continue;
                    }

                    if (!KnownClusters.ContainsKey(key))
                    {
                        KnownClusters[key] = clusterList;
                    }
                }
            }
        }

        internal static IReadOnlyList<FailoverAddress> ExpandIfKnown(IReadOnlyList<FailoverAddress> seeds)
        {
            if (seeds == null || seeds.Count == 0)
            {
                return seeds;
            }

            lock (SyncLock)
            {
                for (var i = 0; i < seeds.Count; i++)
                {
                    var key = seeds[i].CacheKey;
                    if (string.IsNullOrWhiteSpace(key))
                    {
                        continue;
                    }

                    if (KnownClusters.TryGetValue(key, out var cluster) && cluster.Count > seeds.Count)
                    {
                        return cluster;
                    }
                }
            }

            return seeds;
        }

        // For testing purposes
        internal static void Clear()
        {
            lock (SyncLock)
            {
                KnownClusters.Clear();
            }
        }
    }
}
