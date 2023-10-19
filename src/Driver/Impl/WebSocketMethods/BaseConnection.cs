using System;
using System.IO;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using TDengine.Driver.Impl.WebSocketMethods.Protocol;

namespace TDengine.Driver.Impl.WebSocketMethods
{
    public class BaseConnection
    {
        private readonly ClientWebSocket _client;

        private readonly TimeSpan _readTimeout;
        private readonly TimeSpan _writeTimeout;

        private const int InternalError = -1;
        private ulong _reqId;
        private readonly TimeSpan _defaultConnTimeout = TimeSpan.FromMinutes(1);
        private readonly TimeSpan _defaultReadTimeout = TimeSpan.FromMinutes(5);
        private readonly TimeSpan _defaultWriteTimeout = TimeSpan.FromSeconds(10);

        protected BaseConnection(string addr, TimeSpan connectTimeout = default,
            TimeSpan readTimeout = default, TimeSpan writeTimeout = default)
        {
            _client = new ClientWebSocket();
            _client.Options.KeepAliveInterval = TimeSpan.FromSeconds(30);

            if (connectTimeout == default)
            {
                connectTimeout = _defaultConnTimeout;
            }

            if (readTimeout == default)
            {
                readTimeout = _defaultReadTimeout;
            }

            if (writeTimeout == default)
            {
                writeTimeout = _defaultWriteTimeout;
            }

            var connTimeout = connectTimeout;
            _readTimeout = readTimeout;
            _writeTimeout = writeTimeout;
            using (var cts = new CancellationTokenSource())
            {
                cts.CancelAfter(connTimeout);
                _client.ConnectAsync(new Uri(addr), cts.Token).Wait(connTimeout);
            }

            if (_client.State != WebSocketState.Open)
            {
                throw new TDengineError(InternalError, $"connect to {addr} fail");
            }
        }

        protected ulong _GetReqId()
        {
            _reqId += 1;
            return _reqId;
        }


        protected static void WriteUInt64ToBytes(byte[] byteArray, ulong value, int offset)
        {
            byteArray[offset + 0] = (byte)value;
            byteArray[offset + 1] = (byte)(value >> 8);
            byteArray[offset + 2] = (byte)(value >> 16);
            byteArray[offset + 3] = (byte)(value >> 24);
            byteArray[offset + 4] = (byte)(value >> 32);
            byteArray[offset + 5] = (byte)(value >> 40);
            byteArray[offset + 6] = (byte)(value >> 48);
            byteArray[offset + 7] = (byte)(value >> 56);
        }

        protected T SendBinaryBackJson<T>(byte[] request) where T : IWSBaseResp
        {
            SendBinary(request);
            var respBytes = Receive(out var messageType);
            if (messageType != WebSocketMessageType.Text)
            {
                throw new TDengineError(-1, "receive unexpected binary message");
            }

            var resp = JsonConvert.DeserializeObject<T>(Encoding.UTF8.GetString(respBytes));
            if (resp.Code == 0) return resp;
            throw new TDengineError(resp.Code, resp.Message);
        }

        protected T2 SendJsonBackJson<T1, T2>(string action, T1 req) where T2 : IWSBaseResp
        {
            var reqStr = SendJson(action, req);
            var respBytes = Receive(out var messageType);
            if (messageType != WebSocketMessageType.Text)
            {
                throw new TDengineError(-1, "receive unexpected binary message", respBytes, reqStr);
            }

            var resp = JsonConvert.DeserializeObject<T2>(Encoding.UTF8.GetString(respBytes));
            if (resp.Code == 0) return resp;
            throw new TDengineError(resp.Code, resp.Message);
        }

        protected byte[] SendJsonBackBytes<T>(string action, T req)
        {
            var reqStr = SendJson(action, req);
            var respBytes = Receive(out var messageType);
            if (messageType == WebSocketMessageType.Binary)
            {
                return respBytes;
            }

            var resp = JsonConvert.DeserializeObject<IWSBaseResp>(Encoding.UTF8.GetString(respBytes));
            throw new TDengineError(resp.Code, resp.Message, reqStr);
        }

        protected string SendJson<T>(string action, T req)
        {
            var request = JsonConvert.SerializeObject(new WSActionReq<T>
            {
                Action = action,
                Args = req
            });
            SendText(request);
            return request;
        }

        private async Task SendAsync(ArraySegment<byte> data, WebSocketMessageType messageType)
        {
            using (var cts = new CancellationTokenSource())
            {
                cts.CancelAfter(_writeTimeout);
                try
                {
                    await _client.SendAsync(data, messageType, true, cts.Token);
                }
                catch (OperationCanceledException)
                {
                    throw new TDengineError(InternalError, "write message timeout");
                }
            }
        }

        private void SendText(string request)
        {
            var data = new ArraySegment<byte>(Encoding.UTF8.GetBytes(request));
            SendAsync(data, WebSocketMessageType.Text).Wait();
        }

        private void SendBinary(byte[] request)
        {
            var data = new ArraySegment<byte>(request);
            SendAsync(data, WebSocketMessageType.Binary).Wait();
        }

        private byte[] Receive(out WebSocketMessageType messageType)
        {
            using (var cts = new CancellationTokenSource())
            {
                cts.CancelAfter(_readTimeout);
                using (MemoryStream memoryStream = new MemoryStream())
                {
                    int bufferSize = 1024 * 1024 * 4;
                    byte[] buffer = new byte[bufferSize];
                    WebSocketReceiveResult result;

                    do
                    {
                        result = _client.ReceiveAsync(new ArraySegment<byte>(buffer), cts.Token).GetAwaiter()
                            .GetResult();

                        if (result.MessageType == WebSocketMessageType.Close)
                        {
                            _client.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None)
                                .Wait(-1);
                            throw new TDengineError(InternalError, "receive websocket close frame");
                        }

                        messageType = result.MessageType;
                        memoryStream.Write(buffer, 0, result.Count);
                    } while (!result.EndOfMessage);

                    return memoryStream.ToArray();
                }
            }
        }

        public void Close()
        {
            try
            {
                _client.CloseAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None).GetAwaiter()
                    .GetResult();
            }
            catch (Exception)
            {
                // ignored
            }
        }
    }
}