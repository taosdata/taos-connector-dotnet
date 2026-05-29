using System;
using System.Collections.Generic;

namespace TDengine.Driver
{
    internal static class AdapterHAHelper
    {
        /// <summary>
        /// Returns only the discovered addresses that are NOT already in existingAddresses.
        /// Used by tests and callers that only need the delta.
        /// </summary>
        internal static List<FailoverAddress> MergeDiscoveredAddresses(
            IReadOnlyList<FailoverAddress> existingAddresses,
            string[] discoveredInstances,
            string protocol,
            bool useSSL)
        {
            if (discoveredInstances == null || discoveredInstances.Length == 0)
            {
                return null;
            }

            var existingKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < existingAddresses.Count; i++)
            {
                existingKeys.Add(existingAddresses[i].CacheKey);
            }

            List<FailoverAddress> newAddresses = null;

            for (var i = 0; i < discoveredInstances.Length; i++)
            {
                var instance = discoveredInstances[i];
                if (string.IsNullOrWhiteSpace(instance))
                {
                    continue;
                }

                string host;
                int port;
                try
                {
                    HostEndpointParser.ParseHostEndpoint(instance, "list_instances", out host, out port,
                        "adapter instance", allowBareIpv6: true);
                }
                catch
                {
                    // Skip invalid instances
                    continue;
                }

                if (port <= 0)
                {
                    // Instance must include a valid port
                    continue;
                }

                var cacheKey = HostEndpointParser.BuildFailoverCacheKey(protocol, useSSL, host, port);
                if (existingKeys.Contains(cacheKey))
                {
                    continue;
                }

                if (!existingKeys.Add(cacheKey))
                {
                    continue;
                }

                if (newAddresses == null)
                {
                    newAddresses = new List<FailoverAddress>();
                }

                newAddresses.Add(new FailoverAddress(host, port, cacheKey));
            }

            return newAddresses;
        }

        /// <summary>
        /// Parses discovered instances into FailoverAddress list without filtering.
        /// Returns null if input is null/empty or contains no valid entries.
        /// </summary>
        internal static List<FailoverAddress> ParseInstances(
            string[] discoveredInstances,
            string protocol,
            bool useSSL)
        {
            if (discoveredInstances == null || discoveredInstances.Length == 0)
            {
                return null;
            }

            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            List<FailoverAddress> result = null;

            for (var i = 0; i < discoveredInstances.Length; i++)
            {
                var instance = discoveredInstances[i];
                if (string.IsNullOrWhiteSpace(instance))
                {
                    continue;
                }

                string host;
                int port;
                try
                {
                    HostEndpointParser.ParseHostEndpoint(instance, "list_instances", out host, out port,
                        "adapter instance", allowBareIpv6: true);
                }
                catch
                {
                    continue;
                }

                if (port <= 0)
                {
                    continue;
                }

                var cacheKey = HostEndpointParser.BuildFailoverCacheKey(protocol, useSSL, host, port);
                if (!seen.Add(cacheKey))
                {
                    continue;
                }

                if (result == null)
                {
                    result = new List<FailoverAddress>();
                }

                result.Add(new FailoverAddress(host, port, cacheKey));
            }

            return result;
        }
    }
}
