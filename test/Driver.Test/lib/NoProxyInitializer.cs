using System;
using System.Collections.Generic;

namespace Driver.Test
{
    internal static class NoProxyInitializer
    {
        private static readonly object SyncRoot = new object();

        internal static void EnsureLoopbackAddressesBypassProxy()
        {
            lock (SyncRoot)
            {
                EnsureLoopbackNoProxy("NO_PROXY");
                EnsureLoopbackNoProxy("no_proxy");
            }
        }

        private static void EnsureLoopbackNoProxy(string variableName)
        {
            Environment.SetEnvironmentVariable(variableName,
                AppendLoopbackAddresses(Environment.GetEnvironmentVariable(variableName)));
        }

        private static string AppendLoopbackAddresses(string value)
        {
            var entries = new List<string>();
            AddEntries(entries, value);
            AddEntry(entries, "::1");
            AddEntry(entries, "[::1]");
            return string.Join(",", entries);
        }

        private static void AddEntries(List<string> entries, string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return;
            }

            foreach (var segment in value.Split(','))
            {
                AddEntry(entries, segment);
            }
        }

        private static void AddEntry(List<string> entries, string value)
        {
            var trimmed = value == null ? null : value.Trim();
            if (string.IsNullOrEmpty(trimmed))
            {
                return;
            }

            foreach (var entry in entries)
            {
                if (string.Equals(entry, trimmed, StringComparison.OrdinalIgnoreCase))
                {
                    return;
                }
            }

            entries.Add(trimmed);
        }
    }
}
