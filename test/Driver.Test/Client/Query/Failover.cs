using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using TDengine.Driver;
using TDengine.Driver.Client;
using TDengine.Driver.Impl.WebSocketMethods.Protocol;
using Xunit;

namespace Driver.Test.Client.Query
{
    public class Failover
    {
        [Fact]
        public void MultiAddressConnectShouldSelectLeastConnectionAddress()
        {
            var firstPort = GetFreePort();
            var secondPort = GetFreePort();
            while (secondPort == firstPort)
            {
                secondPort = GetFreePort();
            }
            var firstConnCount = 0;
            var secondConnCount = 0;

            var firstServer = new MockWSServer(firstPort,
                CreateHandshakeMessageHandler(() => { Interlocked.Increment(ref firstConnCount); }));
            var secondServer = new MockWSServer(secondPort,
                CreateHandshakeMessageHandler(() => { Interlocked.Increment(ref secondConnCount); }));
            try
            {
                firstServer.Start();
                secondServer.Start();

                var connStr = "protocol=WebSocket;" +
                              $"host=localhost:{firstPort},localhost:{secondPort};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "enableCompression=true;" +
                              "connTimeout=00:00:02;";

                using (var firstClient = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                {
                    using (var secondClient = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                    {
                        Assert.Equal(1, Volatile.Read(ref firstConnCount));
                        Assert.Equal(1, Volatile.Read(ref secondConnCount));
                        Assert.True(firstClient.ConnectionAvailable());
                        Assert.True(secondClient.ConnectionAvailable());
                    }
                }
            }
            finally
            {
                firstServer.Dispose();
                secondServer.Dispose();
            }
        }

        [Fact]
        public void MultiAddressConnectShouldDistributeEvenlyUnderHighConcurrency()
        {
            const int addressCount = 2;
            const int clientCount = 20;
            var ports = new List<int>(addressCount);
            while (ports.Count < addressCount)
            {
                var candidate = GetFreePort();
                if (ports.Contains(candidate))
                {
                    continue;
                }

                ports.Add(candidate);
            }

            var connCounts = new int[addressCount];
            var servers = new List<MockWSServer>(addressCount);
            for (var i = 0; i < addressCount; i++)
            {
                var index = i;
                servers.Add(new MockWSServer(ports[i],
                    CreateHandshakeMessageHandler(() => { Interlocked.Increment(ref connCounts[index]); })));
            }

            var clients = new ConcurrentBag<ITDengineClient>();
            var errors = new ConcurrentQueue<Exception>();
            var startGate = new ManualResetEventSlim(false);
            try
            {
                for (var i = 0; i < servers.Count; i++)
                {
                    servers[i].Start();
                }

                var hostList = string.Join(",", ports.Select(p => $"localhost:{p}"));
                var connStr = "protocol=WebSocket;" +
                              $"host={hostList};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "enableCompression=true;" +
                              "connTimeout=00:00:05;";

                var tasks = new Task[clientCount];
                for (var i = 0; i < clientCount; i++)
                {
                    tasks[i] = Task.Run(() =>
                    {
                        startGate.Wait();
                        Exception lastException = null;
                        for (var attempt = 0; attempt < 3; attempt++)
                        {
                            try
                            {
                                var client = DbDriver.Open(new ConnectionStringBuilder(connStr));
                                clients.Add(client);
                                return;
                            }
                            catch (Exception ex)
                            {
                                lastException = ex;
                                Thread.Sleep(30);
                            }
                        }

                        if (lastException != null)
                        {
                            errors.Enqueue(lastException);
                        }
                    });
                }

                startGate.Set();
                Task.WaitAll(tasks);

                Assert.Empty(errors);
                Assert.Equal(clientCount, connCounts.Sum());
                var minCount = connCounts.Min();
                var maxCount = connCounts.Max();
                Assert.True(maxCount - minCount <= 2,
                    $"connection distribution is not balanced under concurrency pressure: [{string.Join(",", connCounts)}]");
            }
            finally
            {
                while (clients.TryTake(out var client))
                {
                    client.Dispose();
                }

                for (var i = 0; i < servers.Count; i++)
                {
                    servers[i].Dispose();
                }

                startGate.Dispose();
            }
        }

        [Fact]
        public void MultiAddressConnectShouldDistributeStrictlyUnderModerateConcurrency()
        {
            const int addressCount = 2;
            const int clientCount = 10;
            var ports = new List<int>(addressCount);
            while (ports.Count < addressCount)
            {
                var candidate = GetFreePort();
                if (ports.Contains(candidate))
                {
                    continue;
                }

                ports.Add(candidate);
            }

            var connCounts = new int[addressCount];
            var servers = new List<MockWSServer>(addressCount);
            for (var i = 0; i < addressCount; i++)
            {
                var index = i;
                servers.Add(new MockWSServer(ports[i],
                    CreateHandshakeMessageHandler(() => { Interlocked.Increment(ref connCounts[index]); })));
            }

            var clients = new ConcurrentBag<ITDengineClient>();
            var errors = new ConcurrentQueue<Exception>();
            var startGate = new ManualResetEventSlim(false);
            try
            {
                for (var i = 0; i < servers.Count; i++)
                {
                    servers[i].Start();
                }

                var hostList = string.Join(",", ports.Select(p => $"localhost:{p}"));
                var connStr = "protocol=WebSocket;" +
                              $"host={hostList};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "enableCompression=true;" +
                              "connTimeout=00:00:05;";

                var tasks = new Task[clientCount];
                for (var i = 0; i < clientCount; i++)
                {
                    tasks[i] = Task.Run(() =>
                    {
                        startGate.Wait();
                        try
                        {
                            var client = DbDriver.Open(new ConnectionStringBuilder(connStr));
                            clients.Add(client);
                        }
                        catch (Exception ex)
                        {
                            errors.Enqueue(ex);
                        }
                    });
                }

                startGate.Set();
                Task.WaitAll(tasks);

                Assert.Empty(errors);
                Assert.Equal(clientCount, connCounts.Sum());
                var minCount = connCounts.Min();
                var maxCount = connCounts.Max();
                Assert.True(maxCount - minCount <= 1,
                    $"strict connection distribution is not balanced: [{string.Join(",", connCounts)}]");
            }
            finally
            {
                while (clients.TryTake(out var client))
                {
                    client.Dispose();
                }

                for (var i = 0; i < servers.Count; i++)
                {
                    servers[i].Dispose();
                }

                startGate.Dispose();
            }
        }

        [Fact]
        public void MultiAddressConnectShouldNotUseAddressFromOtherConnection()
        {
            var servedPort = GetFreePort();
            var unavailablePort = GetFreePort();
            while (unavailablePort == servedPort)
            {
                unavailablePort = GetFreePort();
            }
            var servedConnCount = 0;

            var servedServer = new MockWSServer(servedPort,
                CreateHandshakeMessageHandler(() => { Interlocked.Increment(ref servedConnCount); }));
            try
            {
                servedServer.Start();

                var servedConnStr = "protocol=WebSocket;" +
                                    $"host=localhost:{servedPort};" +
                                    "useSSL=false;" +
                                    "username=root;" +
                                    "password=taosdata;" +
                                    "enableCompression=true;" +
                                    "connTimeout=00:00:02;";
                using (var servedClient = DbDriver.Open(new ConnectionStringBuilder(servedConnStr)))
                {
                    var unavailableConnStr = "protocol=WebSocket;" +
                                             $"host=localhost:{unavailablePort};" +
                                             "useSSL=false;" +
                                             "username=root;" +
                                             "password=taosdata;" +
                                             "enableCompression=true;" +
                                             "connTimeout=00:00:01;";
                    Assert.ThrowsAny<Exception>(() =>
                    {
                        using (var shouldFailClient = DbDriver.Open(new ConnectionStringBuilder(unavailableConnStr)))
                        {
                        }
                    });
                }
            }
            finally
            {
                servedServer.Dispose();
            }

            Assert.Equal(1, Volatile.Read(ref servedConnCount));
        }

        [Fact]
        public void MultiAddressReconnectShouldPreferPreviousAddressForTransientDisconnect()
        {
            var firstPort = GetFreePort();
            var secondPort = GetFreePort();
            while (secondPort == firstPort)
            {
                secondPort = GetFreePort();
            }

            var firstConnCount = 0;
            var secondConnCount = 0;
            var firstStmtInitClosed = 0;
            ulong stmtId = 0;

            Action<WebSocket, WebSocketMessageType, byte[]> firstHandler = (webSocket, messageType, message) =>
            {
                var req = JsonConvert.DeserializeObject<WSActionReq<TestBaseReq>>(Encoding.UTF8.GetString(message));
                if (req == null)
                {
                    throw new Exception("invalid websocket request");
                }

                switch (req.Action)
                {
                    case WSAction.Version:
                    {
                        var resp = new WSVersionResp
                        {
                            Code = 0,
                            Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            Version = "3.3.6.0"
                        };
                        SendResponse(webSocket, messageType, resp);
                        break;
                    }
                    case WSAction.Conn:
                    {
                        Interlocked.Increment(ref firstConnCount);
                        var resp = new WSConnResp
                        {
                            Code = 0,
                            Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId
                        };
                        SendResponse(webSocket, messageType, resp);
                        break;
                    }
                    case "stmt2_init":
                    {
                        if (Interlocked.CompareExchange(ref firstStmtInitClosed, 1, 0) == 0)
                        {
                            webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "",
                                CancellationToken.None).GetAwaiter().GetResult();
                            return;
                        }

                        var resp = new WSStmt2InitResp
                        {
                            Code = 0,
                            Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            StmtId = ++stmtId
                        };
                        SendResponse(webSocket, messageType, resp);
                        break;
                    }
                }
            };

            Action<WebSocket, WebSocketMessageType, byte[]> secondHandler = (webSocket, messageType, message) =>
            {
                var req = JsonConvert.DeserializeObject<WSActionReq<TestBaseReq>>(Encoding.UTF8.GetString(message));
                if (req == null)
                {
                    throw new Exception("invalid websocket request");
                }

                switch (req.Action)
                {
                    case WSAction.Version:
                    {
                        var resp = new WSVersionResp
                        {
                            Code = 0,
                            Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            Version = "3.3.6.0"
                        };
                        SendResponse(webSocket, messageType, resp);
                        break;
                    }
                    case WSAction.Conn:
                    {
                        Interlocked.Increment(ref secondConnCount);
                        var resp = new WSConnResp
                        {
                            Code = 0,
                            Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId
                        };
                        SendResponse(webSocket, messageType, resp);
                        break;
                    }
                    case "stmt2_init":
                    {
                        var resp = new WSStmt2InitResp
                        {
                            Code = 0,
                            Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            StmtId = ++stmtId
                        };
                        SendResponse(webSocket, messageType, resp);
                        break;
                    }
                }
            };

            var firstServer = new MockWSServer(firstPort, firstHandler);
            var secondServer = new MockWSServer(secondPort, secondHandler);
            try
            {
                firstServer.Start();
                secondServer.Start();

                var connStr = "protocol=WebSocket;" +
                              $"host=localhost:{firstPort},localhost:{secondPort};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "enableCompression=true;" +
                              "autoReconnect=true;" +
                              "reconnectRetryCount=3;" +
                              "reconnectIntervalMs=10;" +
                              "connTimeout=00:00:02;";
                using (var client = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                {
                    using (var stmt = client.StmtInit())
                    {
                        Assert.NotNull(stmt);
                    }

                    Assert.True(client.ConnectionAvailable());
                }
            }
            finally
            {
                firstServer.Dispose();
                secondServer.Dispose();
            }

            Assert.Equal(2, Volatile.Read(ref firstConnCount));
            Assert.Equal(0, Volatile.Read(ref secondConnCount));
        }

        [Fact]
        public void MultiAddressDisposeShouldReleaseConnectionCountAndRebalance()
        {
            var firstPort = GetFreePort();
            var secondPort = GetFreePort();
            while (secondPort == firstPort)
            {
                secondPort = GetFreePort();
            }

            var firstConnCount = 0;
            var secondConnCount = 0;

            var firstServer = new MockWSServer(firstPort,
                CreateHandshakeMessageHandler(() => { Interlocked.Increment(ref firstConnCount); }));
            var secondServer = new MockWSServer(secondPort,
                CreateHandshakeMessageHandler(() => { Interlocked.Increment(ref secondConnCount); }));
            try
            {
                firstServer.Start();
                secondServer.Start();

                var connStr = "protocol=WebSocket;" +
                              $"host=localhost:{firstPort},localhost:{secondPort};" +
                              "useSSL=false;" +
                              "username=root;" +
                              "password=taosdata;" +
                              "enableCompression=true;" +
                              "connTimeout=00:00:02;";

                using (var firstClient = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                {
                    using (var secondClient = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                    {
                        Assert.Equal(1, Volatile.Read(ref firstConnCount));
                        Assert.Equal(1, Volatile.Read(ref secondConnCount));
                        Assert.True(firstClient.ConnectionAvailable());
                        Assert.True(secondClient.ConnectionAvailable());
                    }
                }

                using (var thirdClient = DbDriver.Open(new ConnectionStringBuilder(connStr)))
                {
                    Assert.True(thirdClient.ConnectionAvailable());
                }
            }
            finally
            {
                firstServer.Dispose();
                secondServer.Dispose();
            }

            Assert.Equal(2, Volatile.Read(ref firstConnCount));
            Assert.Equal(1, Volatile.Read(ref secondConnCount));
        }

        private static Action<WebSocket, WebSocketMessageType, byte[]> CreateHandshakeMessageHandler(Action onConnected)
        {
            return (webSocket, messageType, message) =>
            {
                var req = JsonConvert.DeserializeObject<WSActionReq<TestBaseReq>>(Encoding.UTF8.GetString(message));
                if (req == null)
                {
                    throw new Exception("invalid websocket request");
                }

                switch (req.Action)
                {
                    case WSAction.Version:
                    {
                        var resp = new WSVersionResp
                        {
                            Code = 0,
                            Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId,
                            Version = "3.3.6.0"
                        };
                        SendResponse(webSocket, messageType, resp);
                        break;
                    }
                    case WSAction.Conn:
                    {
                        onConnected?.Invoke();
                        var resp = new WSConnResp
                        {
                            Code = 0,
                            Action = req.Action,
                            ReqId = req.Args == null ? 0 : req.Args.ReqId
                        };
                        SendResponse(webSocket, messageType, resp);
                        break;
                    }
                }
            };
        }

        private static void SendResponse(WebSocket webSocket, WebSocketMessageType messageType, object response)
        {
            var respStr = JsonConvert.SerializeObject(response);
            var data = new ArraySegment<byte>(Encoding.UTF8.GetBytes(respStr));
            webSocket.SendAsync(data, messageType, true, CancellationToken.None).GetAwaiter().GetResult();
        }

        private static int GetFreePort()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var endpoint = (IPEndPoint)listener.LocalEndpoint;
            listener.Stop();
            return endpoint.Port;
        }

        private class TestBaseReq
        {
            [JsonProperty("req_id")] public ulong ReqId { get; set; }
        }
    }
}
