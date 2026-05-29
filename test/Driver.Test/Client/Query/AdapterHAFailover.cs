using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.WebSockets;
using System.Reflection;
using System.Text;
using System.Threading;
using Newtonsoft.Json;
using TDengine.Driver;
using TDengine.Driver.Client;
using TDengine.Driver.Impl.WebSocketMethods.Protocol;
using Xunit;

namespace Driver.Test.Client.Query
{
    public class AdapterHAFailover
    {
        [Fact]
        public void AdapterHAEnabledShouldSendListInstancesInConnReq()
        {
            int port = 0;
            bool? receivedListInstances = null;

            var server = MockWSServer.CreateOnFreePort((webSocket, messageType, message) =>
            {
                var raw = Encoding.UTF8.GetString(message);
                var req = JsonConvert.DeserializeObject<WSActionReq<WSConnReq>>(raw);
                if (req == null) return;

                switch (req.Action)
                {
                    case WSAction.Version:
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0, Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    case WSAction.Conn:
                        receivedListInstances = req.Args?.ListInstances;
                        SendResponse(webSocket, messageType, new WSConnResp
                        {
                            Code = 0, Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            ListInstances = new[] { $"127.0.0.1:{port}" }
                        });
                        break;
                }
            });
            port = server.Port;

            try
            {
                server.Start();
                var connStr = "protocol=WebSocket;" +
                              $"host=127.0.0.1:{port};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "adapterHA=true;" +
                              "connTimeout=00:00:05;";

                using (var client = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                {
                    Assert.True(client.ConnectionAvailable());
                    Assert.NotNull(receivedListInstances);
                    Assert.True(receivedListInstances.Value);
                }
            }
            finally
            {
                server.Dispose();
            }
        }

        [Fact]
        public void AdapterHADisabledShouldNotSendListInstancesInConnReq()
        {
            int port = 0;
            bool? receivedListInstances = null;
            bool connReceived = false;

            var server = MockWSServer.CreateOnFreePort((webSocket, messageType, message) =>
            {
                var raw = Encoding.UTF8.GetString(message);
                var req = JsonConvert.DeserializeObject<WSActionReq<WSConnReq>>(raw);
                if (req == null) return;

                switch (req.Action)
                {
                    case WSAction.Version:
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0, Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    case WSAction.Conn:
                        connReceived = true;
                        receivedListInstances = req.Args?.ListInstances;
                        SendResponse(webSocket, messageType, new WSConnResp
                        {
                            Code = 0, Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId
                        });
                        break;
                }
            });
            port = server.Port;

            try
            {
                server.Start();
                var connStr = "protocol=WebSocket;" +
                              $"host=127.0.0.1:{port};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "adapterHA=false;" +
                              "connTimeout=00:00:05;";

                using (var client = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                {
                    Assert.True(client.ConnectionAvailable());
                    Assert.True(connReceived);
                    Assert.Null(receivedListInstances);
                }
            }
            finally
            {
                server.Dispose();
            }
        }

        [Fact]
        public void AdapterHAShouldExpandFailoverAddressesFromListInstances()
        {
            // Start only one server but have it return a second address in list_instances
            int firstPort = 0;
            int secondPort = 0;

            AdapterClusterRegistry.Clear();

            var server = MockWSServer.CreateOnFreePort((webSocket, messageType, message) =>
            {
                var raw = Encoding.UTF8.GetString(message);
                var req = JsonConvert.DeserializeObject<WSActionReq<WSConnReq>>(raw);
                if (req == null) return;

                switch (req.Action)
                {
                    case WSAction.Version:
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0, Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    case WSAction.Conn:
                        SendResponse(webSocket, messageType, new WSConnResp
                        {
                            Code = 0, Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            ListInstances = new[] { $"127.0.0.1:{firstPort}", $"127.0.0.1:{secondPort}" }
                        });
                        break;
                }
            });
            firstPort = server.Port;
            secondPort = firstPort + 1; // Just a different port number for list_instances response

            try
            {
                server.Start();
                var connStr = "protocol=WebSocket;" +
                              $"host=127.0.0.1:{firstPort};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "adapterHA=true;" +
                              "connTimeout=00:00:05;";

                using (var client = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                {
                    Assert.True(client.ConnectionAvailable());

                    // Verify the cluster was registered in global registry
                    var seeds = new List<FailoverAddress>
                    {
                        new FailoverAddress("127.0.0.1", firstPort, $"ws://127.0.0.1:{firstPort}")
                    };
                    var expanded = AdapterClusterRegistry.ExpandIfKnown(seeds);
                    Assert.True(expanded.Count >= 2,
                        $"Expected at least 2 addresses in cluster, got {expanded.Count}");

                    // Check both ports are present
                    var ports = new HashSet<int>();
                    for (var i = 0; i < expanded.Count; i++)
                    {
                        ports.Add(expanded[i].Port);
                    }

                    Assert.Contains(firstPort, ports);
                    Assert.Contains(secondPort, ports);
                }
            }
            finally
            {
                server.Dispose();
                AdapterClusterRegistry.Clear();
            }
        }

        [Fact]
        public void AdapterHAShouldFailoverToDiscoveredAddress()
        {
            int firstPort = 0;
            int secondPort = 0;

            var firstConnCount = 0;
            var secondConnCount = 0;
            var firstUnavailable = 0;
            ulong stmtId = 0;

            AdapterClusterRegistry.Clear();

            // First server returns list_instances including second port, then goes unavailable on stmt2_init
            Action<WebSocket, WebSocketMessageType, byte[]> firstHandler = (webSocket, messageType, message) =>
            {
                var raw = Encoding.UTF8.GetString(message);
                var req = JsonConvert.DeserializeObject<WSActionReq<TestBaseReq>>(raw);
                if (req == null) return;

                if (Volatile.Read(ref firstUnavailable) == 1)
                {
                    webSocket.CloseAsync(WebSocketCloseStatus.InternalServerError, "unavailable",
                        CancellationToken.None).GetAwaiter().GetResult();
                    return;
                }

                switch (req.Action)
                {
                    case WSAction.Version:
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0, Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    case WSAction.Conn:
                        Interlocked.Increment(ref firstConnCount);
                        SendResponse(webSocket, messageType, new WSConnResp
                        {
                            Code = 0, Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            ListInstances = new[] { $"127.0.0.1:{firstPort}", $"127.0.0.1:{secondPort}" }
                        });
                        break;
                    case "stmt2_init":
                        // Go unavailable on stmt2_init to trigger failover
                        Interlocked.Exchange(ref firstUnavailable, 1);
                        webSocket.CloseAsync(WebSocketCloseStatus.InternalServerError, "going down",
                            CancellationToken.None).GetAwaiter().GetResult();
                        break;
                }
            };

            Action<WebSocket, WebSocketMessageType, byte[]> secondHandler = (webSocket, messageType, message) =>
            {
                var raw = Encoding.UTF8.GetString(message);
                var req = JsonConvert.DeserializeObject<WSActionReq<TestBaseReq>>(raw);
                if (req == null) return;

                switch (req.Action)
                {
                    case WSAction.Version:
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0, Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    case WSAction.Conn:
                        Interlocked.Increment(ref secondConnCount);
                        SendResponse(webSocket, messageType, new WSConnResp
                        {
                            Code = 0, Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId
                        });
                        break;
                    case "stmt2_init":
                        SendResponse(webSocket, messageType, new WSStmt2InitResp
                        {
                            Code = 0, Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            StmtId = ++stmtId
                        });
                        break;
                }
            };

            var firstServer = MockWSServer.CreateOnFreePort(firstHandler);
            var secondServer = MockWSServer.CreateOnFreePort(secondHandler);
            firstPort = firstServer.Port;
            secondPort = secondServer.Port;

            ResetFailoverCacheConnectionCount($"ws://127.0.0.1:{firstPort}");
            ResetFailoverCacheConnectionCount($"ws://127.0.0.1:{secondPort}");

            try
            {
                firstServer.Start();
                secondServer.Start();

                // Connect with adapterHA=true + autoReconnect, only seed is firstPort
                var connStr = "protocol=WebSocket;" +
                              $"host=127.0.0.1:{firstPort};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "adapterHA=true;" +
                              "autoReconnect=true;" +
                              "reconnectRetryCount=5;" +
                              "reconnectIntervalMs=50;" +
                              "connTimeout=00:00:05;";

                using (var client = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                {
                    Assert.True(client.ConnectionAvailable());
                    Assert.Equal(1, Volatile.Read(ref firstConnCount));
                    Assert.Equal(0, Volatile.Read(ref secondConnCount));

                    // StmtInit triggers failover: first server goes down, reconnects to second
                    using (var stmt = client.StmtInit())
                    {
                        Assert.NotNull(stmt);
                    }

                    Assert.True(client.ConnectionAvailable());
                    Assert.True(Volatile.Read(ref secondConnCount) >= 1,
                        "Should have reconnected to second (discovered) server");
                }
            }
            finally
            {
                firstServer.Dispose();
                secondServer.Dispose();
                AdapterClusterRegistry.Clear();
            }
        }

        [Fact]
        public void AdapterHAOldAdapterWithoutListInstancesShouldStillConnect()
        {
            // Simulate old adapter that doesn't return list_instances
            var server = MockWSServer.CreateOnFreePort((webSocket, messageType, message) =>
            {
                var raw = Encoding.UTF8.GetString(message);
                var req = JsonConvert.DeserializeObject<WSActionReq<TestBaseReq>>(raw);
                if (req == null) return;

                switch (req.Action)
                {
                    case WSAction.Version:
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0, Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    case WSAction.Conn:
                        // Old adapter: no ListInstances field in response (null)
                        SendResponse(webSocket, messageType, new WSConnResp
                        {
                            Code = 0, Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            ListInstances = null
                        });
                        break;
                }
            });
            var port = server.Port;

            try
            {
                server.Start();
                var connStr = "protocol=WebSocket;" +
                              $"host=127.0.0.1:{port};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "adapterHA=true;" +
                              "connTimeout=00:00:05;";

                using (var client = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                {
                    Assert.True(client.ConnectionAvailable());
                }
            }
            finally
            {
                server.Dispose();
            }
        }

        [Fact]
        public void AdapterHAEmptyListInstancesShouldStillConnect()
        {
            var server = MockWSServer.CreateOnFreePort((webSocket, messageType, message) =>
            {
                var raw = Encoding.UTF8.GetString(message);
                var req = JsonConvert.DeserializeObject<WSActionReq<TestBaseReq>>(raw);
                if (req == null) return;

                switch (req.Action)
                {
                    case WSAction.Version:
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0, Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    case WSAction.Conn:
                        SendResponse(webSocket, messageType, new WSConnResp
                        {
                            Code = 0, Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            ListInstances = Array.Empty<string>()
                        });
                        break;
                }
            });
            var port = server.Port;

            try
            {
                server.Start();
                var connStr = "protocol=WebSocket;" +
                              $"host=127.0.0.1:{port};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "adapterHA=true;" +
                              "connTimeout=00:00:05;";

                using (var client = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                {
                    Assert.True(client.ConnectionAvailable());
                }
            }
            finally
            {
                server.Dispose();
            }
        }

        [Fact]
        public void AdapterHANewConnectionExpandsFromGlobalRegistry()
        {
            AdapterClusterRegistry.Clear();

            var firstConnCount = 0;
            var secondConnCount = 0;

            // Both servers respond normally
            var firstServer = MockWSServer.CreateOnFreePort(
                CreateHandshakeMessageHandler(() => { Interlocked.Increment(ref firstConnCount); }));
            var secondServer = MockWSServer.CreateOnFreePort(
                CreateHandshakeMessageHandler(() => { Interlocked.Increment(ref secondConnCount); }));

            var firstPort = firstServer.Port;
            var secondPort = secondServer.Port;

            // Pre-register cluster so new connections expand from registry
            var seeds = new List<FailoverAddress>
            {
                new FailoverAddress("127.0.0.1", firstPort, $"ws://127.0.0.1:{firstPort}")
            };
            var fullCluster = new List<FailoverAddress>
            {
                new FailoverAddress("127.0.0.1", firstPort, $"ws://127.0.0.1:{firstPort}"),
                new FailoverAddress("127.0.0.1", secondPort, $"ws://127.0.0.1:{secondPort}")
            };
            AdapterClusterRegistry.RegisterCluster(seeds, fullCluster);

            ResetFailoverCacheConnectionCount($"ws://127.0.0.1:{firstPort}");
            ResetFailoverCacheConnectionCount($"ws://127.0.0.1:{secondPort}");

            try
            {
                firstServer.Start();
                secondServer.Start();

                // Only seed firstPort in connection string, but cluster is already known
                var connStr = "protocol=WebSocket;" +
                              $"host=127.0.0.1:{firstPort};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "adapterHA=true;" +
                              "connTimeout=00:00:05;";

                // Open 2 connections - with least-connections they should distribute
                using (var client1 = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                using (var client2 = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                {
                    Assert.True(client1.ConnectionAvailable());
                    Assert.True(client2.ConnectionAvailable());

                    // Both servers should have received connections thanks to registry expansion
                    Assert.True(Volatile.Read(ref firstConnCount) >= 1,
                        "First server should have at least 1 connection");
                    Assert.True(Volatile.Read(ref secondConnCount) >= 1,
                        "Second server should have at least 1 connection (expanded from registry)");
                }
            }
            finally
            {
                firstServer.Dispose();
                secondServer.Dispose();
                AdapterClusterRegistry.Clear();
            }
        }

        #region Connection.Connect() Overload Tests

        [Fact]
        public void ConnectionConnectNoArgShouldNotSendListInstances()
        {
            bool? receivedListInstances = null;
            bool connReceived = false;

            var server = MockWSServer.CreateOnFreePort((webSocket, messageType, message) =>
            {
                var raw = Encoding.UTF8.GetString(message);
                var req = JsonConvert.DeserializeObject<WSActionReq<WSConnReq>>(raw);
                if (req == null) return;

                switch (req.Action)
                {
                    case WSAction.Version:
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0, Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    case WSAction.Conn:
                        connReceived = true;
                        receivedListInstances = req.Args?.ListInstances;
                        SendResponse(webSocket, messageType, new WSConnResp
                        {
                            Code = 0, Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId
                        });
                        break;
                }
            });
            var port = server.Port;

            try
            {
                server.Start();
                var addr = $"ws://127.0.0.1:{port}/ws";
                var conn = new TDengine.Driver.Impl.WebSocketMethods.Connection(
                    addr, "root", "taosdata", null, null,
                    TimeSpan.FromSeconds(5));
                conn.Connect();

                Assert.True(connReceived);
                Assert.Null(receivedListInstances);
            }
            finally
            {
                server.Dispose();
            }
        }

        [Fact]
        public void ConnectionConnectWithListInstancesTrueShouldSendFlag()
        {
            int port = 0;
            bool? receivedListInstances = null;
            bool connReceived = false;

            var server = MockWSServer.CreateOnFreePort((webSocket, messageType, message) =>
            {
                var raw = Encoding.UTF8.GetString(message);
                var req = JsonConvert.DeserializeObject<WSActionReq<WSConnReq>>(raw);
                if (req == null) return;

                switch (req.Action)
                {
                    case WSAction.Version:
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0, Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    case WSAction.Conn:
                        connReceived = true;
                        receivedListInstances = req.Args?.ListInstances;
                        SendResponse(webSocket, messageType, new WSConnResp
                        {
                            Code = 0, Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            ListInstances = new[] { $"127.0.0.1:{port}" }
                        });
                        break;
                }
            });
            port = server.Port;

            try
            {
                server.Start();
                var addr = $"ws://127.0.0.1:{port}/ws";
                var conn = new TDengine.Driver.Impl.WebSocketMethods.Connection(
                    addr, "root", "taosdata", null, null,
                    TimeSpan.FromSeconds(5));
                var resp = conn.Connect(true);

                Assert.True(connReceived);
                Assert.NotNull(receivedListInstances);
                Assert.True(receivedListInstances.Value);
                Assert.NotNull(resp.ListInstances);
                Assert.Single(resp.ListInstances);
            }
            finally
            {
                server.Dispose();
            }
        }

        #endregion

        #region Protocol Serialization Tests

        [Fact]
        public void WSConnReqListInstancesSerializesToJson()
        {
            var req = new WSConnReq
            {
                ReqId = 1,
                User = "root",
                Password = "taosdata",
                ListInstances = true
            };

            var json = JsonConvert.SerializeObject(req);
            Assert.Contains("\"list_instances\":true", json);
        }

        [Fact]
        public void WSConnReqListInstancesNullOmitsField()
        {
            var req = new WSConnReq
            {
                ReqId = 1,
                User = "root",
                Password = "taosdata",
                ListInstances = null
            };

            var json = JsonConvert.SerializeObject(req);
            Assert.DoesNotContain("list_instances", json);
        }

        [Fact]
        public void WSConnRespDeserializesListInstances()
        {
            var json = "{\"code\":0,\"message\":\"\",\"action\":\"conn\",\"req_id\":1,\"timing\":100," +
                       "\"list_instances\":[\"192.168.1.1:6041\",\"192.168.1.2:6041\"]}";

            var resp = JsonConvert.DeserializeObject<WSConnResp>(json);
            Assert.NotNull(resp);
            Assert.NotNull(resp.ListInstances);
            Assert.Equal(2, resp.ListInstances.Length);
            Assert.Equal("192.168.1.1:6041", resp.ListInstances[0]);
            Assert.Equal("192.168.1.2:6041", resp.ListInstances[1]);
        }

        [Fact]
        public void WSConnRespDeserializesWithoutListInstances()
        {
            var json = "{\"code\":0,\"message\":\"\",\"action\":\"conn\",\"req_id\":1,\"timing\":100}";

            var resp = JsonConvert.DeserializeObject<WSConnResp>(json);
            Assert.NotNull(resp);
            Assert.Null(resp.ListInstances);
        }

        [Fact]
        public void WSTMQSubscribeReqListInstancesSerializesToJson()
        {
            var req = new WSTMQSubscribeReq
            {
                ReqId = 1,
                User = "root",
                Password = "taosdata",
                GroupId = "g1",
                Topics = new List<string> { "topic1" },
                ListInstances = true
            };

            var json = JsonConvert.SerializeObject(req);
            Assert.Contains("\"list_instances\":true", json);
        }

        [Fact]
        public void WSTMQSubscribeReqListInstancesNullOmitsField()
        {
            var req = new WSTMQSubscribeReq
            {
                ReqId = 1,
                User = "root",
                Password = "taosdata",
                GroupId = "g1",
                Topics = new List<string> { "topic1" },
                ListInstances = null
            };

            var json = JsonConvert.SerializeObject(req);
            Assert.DoesNotContain("list_instances", json);
        }

        [Fact]
        public void WSTMQSubscribeRespDeserializesListInstances()
        {
            var json = "{\"code\":0,\"message\":\"\",\"action\":\"subscribe\",\"req_id\":1,\"timing\":100," +
                       "\"list_instances\":[\"10.0.0.1:6041\",\"10.0.0.2:6041\",\"10.0.0.3:6041\"]}";

            var resp = JsonConvert.DeserializeObject<WSTMQSubscribeResp>(json);
            Assert.NotNull(resp);
            Assert.NotNull(resp.ListInstances);
            Assert.Equal(3, resp.ListInstances.Length);
            Assert.Equal("10.0.0.1:6041", resp.ListInstances[0]);
        }

        [Fact]
        public void WSTMQSubscribeRespDeserializesWithoutListInstances()
        {
            var json = "{\"code\":0,\"message\":\"\",\"action\":\"subscribe\",\"req_id\":1,\"timing\":100}";

            var resp = JsonConvert.DeserializeObject<WSTMQSubscribeResp>(json);
            Assert.NotNull(resp);
            Assert.Null(resp.ListInstances);
        }

        #endregion

        #region TMQ Config Tests

        [Fact]
        public void TMQOptionsAdapterHADefaultIsFalse()
        {
            var cfg = new Dictionary<string, string>
            {
                { "td.connect.type", "WebSocket" },
                { "group.id", "test-group" },
                { "td.connect.ip", "localhost" },
                { "td.connect.user", "root" },
                { "td.connect.pass", "taosdata" }
            };

            var options = new TDengine.Driver.Impl.WebSocketMethods.TMQOptions(cfg);
            Assert.True(options.TDAdapterHA == null || options.TDAdapterHA != "true");
        }

        [Fact]
        public void TMQOptionsAdapterHASetToTrue()
        {
            var cfg = new Dictionary<string, string>
            {
                { "td.connect.type", "WebSocket" },
                { "group.id", "test-group" },
                { "td.connect.ip", "localhost" },
                { "td.connect.user", "root" },
                { "td.connect.pass", "taosdata" },
                { "ws.adapterHA", "true" }
            };

            var options = new TDengine.Driver.Impl.WebSocketMethods.TMQOptions(cfg);
            Assert.Equal("true", options.TDAdapterHA);
        }

        [Fact]
        public void TMQOptionsAdapterHASetToFalse()
        {
            var cfg = new Dictionary<string, string>
            {
                { "td.connect.type", "WebSocket" },
                { "group.id", "test-group" },
                { "td.connect.ip", "localhost" },
                { "td.connect.user", "root" },
                { "td.connect.pass", "taosdata" },
                { "ws.adapterHA", "false" }
            };

            var options = new TDengine.Driver.Impl.WebSocketMethods.TMQOptions(cfg);
            Assert.Equal("false", options.TDAdapterHA);
        }

        #endregion

        #region AdapterHAHelper SSL Tests

        [Fact]
        public void AdapterHAHelperMergesWithSSLProtocol()
        {
            var existing = new List<FailoverAddress>
            {
                new FailoverAddress("localhost", 6041, "wss://localhost:6041")
            };

            var discovered = new[] { "localhost:6042" };
            var newAddresses = AdapterHAHelper.MergeDiscoveredAddresses(
                existing, discovered, TDengineConstant.ProtocolWebSocket, true);

            Assert.NotNull(newAddresses);
            Assert.Single(newAddresses);
            Assert.Equal("wss://localhost:6042", newAddresses[0].CacheKey);
        }

        [Fact]
        public void AdapterClusterRegistryRegisterAndExpandMultipleSeeds()
        {
            AdapterClusterRegistry.Clear();

            var seeds = new List<FailoverAddress>
            {
                new FailoverAddress("host1", 6041, "ws://host1:6041"),
                new FailoverAddress("host2", 6041, "ws://host2:6041")
            };

            var fullCluster = new List<FailoverAddress>
            {
                new FailoverAddress("host1", 6041, "ws://host1:6041"),
                new FailoverAddress("host2", 6041, "ws://host2:6041"),
                new FailoverAddress("host3", 6041, "ws://host3:6041")
            };

            AdapterClusterRegistry.RegisterCluster(seeds, fullCluster);

            // Expand with just one seed that's in the cluster
            var singleSeed = new List<FailoverAddress>
            {
                new FailoverAddress("host1", 6041, "ws://host1:6041")
            };

            var expanded = AdapterClusterRegistry.ExpandIfKnown(singleSeed);
            Assert.Equal(3, expanded.Count);

            AdapterClusterRegistry.Clear();
        }

        [Fact]
        public void AdapterClusterRegistryDoesNotExpandWhenClusterSameSize()
        {
            AdapterClusterRegistry.Clear();

            var seeds = new List<FailoverAddress>
            {
                new FailoverAddress("host1", 6041, "ws://host1:6041"),
                new FailoverAddress("host2", 6041, "ws://host2:6041")
            };

            // Register cluster same size as seeds with identical members
            AdapterClusterRegistry.RegisterCluster(seeds, seeds);

            var result = AdapterClusterRegistry.ExpandIfKnown(seeds);
            // Should return original since cluster has no new members
            Assert.Same(seeds, result);

            AdapterClusterRegistry.Clear();
        }

        [Fact]
        public void AdapterClusterRegistryExpandsWhenClusterHasDifferentMembersOfSameSize()
        {
            AdapterClusterRegistry.Clear();

            var seeds = new List<FailoverAddress>
            {
                new FailoverAddress("host1", 6041, "ws://host1:6041"),
                new FailoverAddress("stale-x", 6041, "ws://stale-x:6041")
            };

            // Registry knows [host1, host2] — same count as seeds but different member
            var cluster = new List<FailoverAddress>
            {
                new FailoverAddress("host1", 6041, "ws://host1:6041"),
                new FailoverAddress("host2", 6041, "ws://host2:6041")
            };

            AdapterClusterRegistry.RegisterCluster(
                new List<FailoverAddress> { new FailoverAddress("host1", 6041, "ws://host1:6041") },
                cluster);

            var result = AdapterClusterRegistry.ExpandIfKnown(seeds);
            // Should expand because cluster has host2 which is not in seeds
            Assert.NotSame(seeds, result);
            Assert.Equal(2, result.Count);

            AdapterClusterRegistry.Clear();
        }

        #endregion

        #region Helper Methods

        private class TestBaseReq
        {
            [JsonProperty("req_id")] public ulong ReqId { get; set; }
        }

        private static Action<WebSocket, WebSocketMessageType, byte[]> CreateHandshakeMessageHandler(
            Action onConnected)
        {
            return (webSocket, messageType, message) =>
            {
                var req =
                    JsonConvert.DeserializeObject<WSActionReq<TestBaseReq>>(Encoding.UTF8.GetString(message));
                if (req == null) throw new Exception("invalid websocket request");

                switch (req.Action)
                {
                    case WSAction.Version:
                        SendResponse(webSocket, messageType, new WSVersionResp
                        {
                            Code = 0, Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            Version = "3.3.6.0"
                        });
                        break;
                    case WSAction.Conn:
                        onConnected?.Invoke();
                        SendResponse(webSocket, messageType, new WSConnResp
                        {
                            Code = 0, Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId
                        });
                        break;
                }
            };
        }

        private static void SendResponse(WebSocket webSocket, WebSocketMessageType messageType, object response)
        {
            var respStr = JsonConvert.SerializeObject(response);
            var data = new ArraySegment<byte>(Encoding.UTF8.GetBytes(respStr));
            webSocket.SendAsync(data, messageType, true, CancellationToken.None).GetAwaiter().GetResult();
        }

        private static void ResetFailoverCacheConnectionCount(string cacheKey)
        {
            var cacheType = typeof(FailoverAddress).Assembly.GetType("TDengine.Driver.FailoverAddressCache");
            if (cacheType == null) return;

            var syncLockField = cacheType.GetField("SyncLock", BindingFlags.Static | BindingFlags.NonPublic);
            var countsField = cacheType.GetField("ConnectionCountByAddress",
                BindingFlags.Static | BindingFlags.NonPublic);
            if (syncLockField == null || countsField == null) return;

            var syncLock = syncLockField.GetValue(null);
            var counts = countsField.GetValue(null) as Dictionary<string, int>;
            if (syncLock == null || counts == null) return;

            lock (syncLock)
            {
                counts[cacheKey] = 0;
            }
        }

        #endregion
    }
}
