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
                // Find the old cluster reference (if any) via seed keys
                List<FailoverAddress> oldCluster = null;
                for (var i = 0; i < seedAddresses.Count; i++)
                {
                    var seedKey = seedAddresses[i].CacheKey;
                    if (!string.IsNullOrWhiteSpace(seedKey) &&
                        KnownClusters.TryGetValue(seedKey, out oldCluster))
                    {
                        break;
                    }
                }

                // Build the set of keys that should exist after this update
                var newKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                for (var i = 0; i < seedAddresses.Count; i++)
                {
                    if (!string.IsNullOrWhiteSpace(seedAddresses[i].CacheKey))
                    {
                        newKeys.Add(seedAddresses[i].CacheKey);
                    }
                }

                for (var i = 0; i < fullCluster.Count; i++)
                {
                    if (!string.IsNullOrWhiteSpace(fullCluster[i].CacheKey))
                    {
                        newKeys.Add(fullCluster[i].CacheKey);
                    }
                }

                // Remove stale keys: entries that pointed to the old cluster
                // but are not members of the new cluster
                if (oldCluster != null)
                {
                    var staleKeys = new List<string>();
                    foreach (var kvp in KnownClusters)
                    {
                        if (ReferenceEquals(kvp.Value, oldCluster) && !newKeys.Contains(kvp.Key))
                        {
                            staleKeys.Add(kvp.Key);
                        }
                    }

                    for (var i = 0; i < staleKeys.Count; i++)
                    {
                        KnownClusters.Remove(staleKeys[i]);
                    }
                }

                // Write new mappings — all keys point to the same list instance
                var clusterList = new List<FailoverAddress>(fullCluster);
                foreach (var key in newKeys)
                {
                    KnownClusters[key] = clusterList;
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

                    if (KnownClusters.TryGetValue(key, out var cluster) && HasNewMembers(cluster, seeds))
                    {
                        return cluster;
                    }
                }
            }

            return seeds;
        }

        private static bool HasNewMembers(List<FailoverAddress> cluster, IReadOnlyList<FailoverAddress> seeds)
        {
            // Check if cluster contains any address not already in seeds
            var seedKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < seeds.Count; i++)
            {
                if (!string.IsNullOrWhiteSpace(seeds[i].CacheKey))
                {
                    seedKeys.Add(seeds[i].CacheKey);
                }
            }

            for (var i = 0; i < cluster.Count; i++)
            {
                if (!string.IsNullOrWhiteSpace(cluster[i].CacheKey) && !seedKeys.Contains(cluster[i].CacheKey))
                {
                    return true;
                }
            }

            return false;
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
