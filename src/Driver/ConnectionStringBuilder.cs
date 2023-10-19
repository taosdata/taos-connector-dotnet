using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;

namespace TDengine.Driver
{
    public class ConnectionStringBuilder : DbConnectionStringBuilder
    {
        private const string HostKey = "host";
        private const string PortKey = "port";
        private const string DatabaseKey = "db";
        private const string UsernameKey = "username";
        private const string PasswordKey = "password";
        private const string ProtocolKey = "protocol";
        private const string TokenKey = "token";
        private const string TimezoneKey = "timezone";
        private const string ConnTimeoutKey = "connTimeout";
        private const string ReadTimeoutKey = "readTimeout";
        private const string WriteTimeoutKey = "writeTimeout";

        private enum KeysEnum
        {
            Host,
            Port,
            Database,
            Username,
            Password,
            Protocol,
            Token,
            Timezone,
            ConnTimeout,
            ReadTimeout,
            WriteTimeout,
        }

        private string _host = string.Empty;
        private int _port = 6030;
        private string _db = string.Empty;
        private string _user = string.Empty;
        private string _password = string.Empty;
        private string _protocol = TDengineConstant.ProtocolNative;
        private string _token = string.Empty;
        private TimeZoneInfo _timezone = TimeZoneInfo.Local;
        private TimeSpan _connTimeout = TimeSpan.Zero;
        private TimeSpan _readTimeout = TimeSpan.Zero;
        private TimeSpan _writeTimeout = TimeSpan.Zero;

        private static readonly IReadOnlyList<string> KeysList;
        private static readonly IReadOnlyDictionary<string, KeysEnum> KeysDict;

        static ConnectionStringBuilder()
        {
            var list = new string[11];
            list[(int)KeysEnum.Host] = HostKey;
            list[(int)KeysEnum.Port] = PortKey;
            list[(int)KeysEnum.Database] = DatabaseKey;
            list[(int)KeysEnum.Username] = UsernameKey;
            list[(int)KeysEnum.Password] = PasswordKey;
            list[(int)KeysEnum.Protocol] = ProtocolKey;
            list[(int)KeysEnum.Token] = TokenKey;
            list[(int)KeysEnum.Timezone] = TimezoneKey;
            list[(int)KeysEnum.ConnTimeout] = ConnTimeoutKey;
            list[(int)KeysEnum.ReadTimeout] = ReadTimeoutKey;
            list[(int)KeysEnum.WriteTimeout] = WriteTimeoutKey;
            KeysList = list;

            KeysDict = new Dictionary<string, KeysEnum>(8, StringComparer.OrdinalIgnoreCase)
            {
                [HostKey] = KeysEnum.Host,
                [PortKey] = KeysEnum.Port,
                [DatabaseKey] = KeysEnum.Database,
                [UsernameKey] = KeysEnum.Username,
                [PasswordKey] = KeysEnum.Password,
                [ProtocolKey] = KeysEnum.Protocol,
                [TokenKey] = KeysEnum.Token,
                [TimezoneKey] = KeysEnum.Timezone,
                [ConnTimeoutKey] = KeysEnum.ConnTimeout,
                [ReadTimeoutKey] = KeysEnum.ReadTimeout,
                [WriteTimeoutKey] = KeysEnum.WriteTimeout,
            };
        }

        public ConnectionStringBuilder(string connectionString)
        {
            ConnectionString = connectionString;
            if (!string.IsNullOrWhiteSpace(connectionString))
            {
                string[] queries = connectionString.Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
                foreach (string query in queries)
                {
                    string[] keyValue = query.Split('=');
                    if (keyValue.Length % 2 != 0)
                    {
                        continue;
                    }

                    var keyword = keyValue[0].Trim();
                    var value = query.Contains(",") ? query.Replace(keyword, "") : keyValue[1];
                    KeysEnum index;
                    var exist = KeysDict.TryGetValue(keyword, out index);
                    if (exist)
                    {
                        switch (index)
                        {
                            case KeysEnum.Host:
                                Host = value;
                                break;
                            case KeysEnum.Port:
                                Port = Convert.ToInt32(value);
                                break;
                            case KeysEnum.Database:
                                Database = value;
                                break;
                            case KeysEnum.Username:
                                Username = value;
                                break;
                            case KeysEnum.Password:
                                Password = value;
                                break;
                            case KeysEnum.Protocol:
                                Protocol = value;
                                break;
                            case KeysEnum.Token:
                                Token = value;
                                break;
                            case KeysEnum.Timezone:
                                Timezone = TimeZoneInfo.FindSystemTimeZoneById(value);
                                break;
                            case KeysEnum.ConnTimeout:
                                ConnTimeout = TimeSpan.Parse(value);
                                break;
                            case KeysEnum.ReadTimeout:
                                ReadTimeout = TimeSpan.Parse(value);
                                break;
                            case KeysEnum.WriteTimeout:
                                WriteTimeout = TimeSpan.Parse(value);
                                break;
                            default:
                                throw new ArgumentOutOfRangeException(nameof(index), index, "get value error");
                        }
                    }
                }
            }
        }

        public string Host
        {
            get => _host;
            set => base[HostKey] = _host = value;
        }

        public int Port
        {
            get => _port;
            set => base[PortKey] = _port = value;
        }

        public string Database
        {
            get => _db;
            set => base[DatabaseKey] = _db = value;
        }

        public string Username
        {
            get => _user;
            set => base[UsernameKey] = _user = value;
        }

        public string Password
        {
            get => _password;
            set => base[PasswordKey] = _password = value;
        }

        public string Protocol
        {
            get => _protocol;
            set
            {
                if (value != TDengineConstant.ProtocolNative && value != TDengineConstant.ProtocolWebSocket)
                    throw new ArgumentException("invalid protocol value", ProtocolKey);
                base[ProtocolKey] = _protocol = value;
            }
        }

        public string Token
        {
            get => _token;
            set => base[TokenKey] = _token = value;
        }

        public TimeZoneInfo Timezone
        {
            get => _timezone;
            set => base[TimezoneKey] = _timezone = value;
        }

        public TimeSpan ConnTimeout
        {
            get => _connTimeout;
            set => base[ConnTimeoutKey] = _connTimeout = value;
        }

        public TimeSpan ReadTimeout
        {
            get => _readTimeout;
            set => base[ReadTimeoutKey] = _readTimeout = value;
        }

        public TimeSpan WriteTimeout
        {
            get => _writeTimeout;
            set => base[WriteTimeoutKey] = _writeTimeout = value;
        }

        public override ICollection Keys => new ReadOnlyCollection<string>((string[])KeysList);

        public override ICollection Values
        {
            get
            {
                var values = new object[KeysList.Count];
                for (int i = 0; i < KeysList.Count; i++)
                {
                    values[i] = GetAt((KeysEnum)i);
                }

                return new ReadOnlyCollection<object>(values);
            }
        }

        private object GetAt(KeysEnum index)
        {
            switch (index)
            {
                case KeysEnum.Host:
                    return Host;
                case KeysEnum.Port:
                    return Port;
                case KeysEnum.Database:
                    return Database;
                case KeysEnum.Username:
                    return Username;
                case KeysEnum.Password:
                    return Password;
                case KeysEnum.Protocol:
                    return Protocol;
                case KeysEnum.Token:
                    return Token;
                case KeysEnum.Timezone:
                    return Timezone;
                default:
                    throw new ArgumentOutOfRangeException(nameof(index), index, "get value error");
            }
        }

        public override bool TryGetValue(string keyword, out object value)
        {
            if (!KeysDict.TryGetValue(keyword, out var index))
            {
                value = null;

                return false;
            }

            value = GetAt(index);

            return true;
        }

        private void Reset(KeysEnum index)
        {
            switch (index)
            {
                case KeysEnum.Host:
                    _host = string.Empty;
                    return;
                case KeysEnum.Port:
                    _port = 6030;
                    return;
                case KeysEnum.Database:
                    _db = string.Empty;
                    return;
                case KeysEnum.Username:
                    _user = string.Empty;
                    return;
                case KeysEnum.Password:
                    _password = string.Empty;
                    return;
                case KeysEnum.Protocol:
                    _protocol = TDengineConstant.ProtocolNative;
                    return;
                case KeysEnum.Token:
                    _token = string.Empty;
                    return;
                case KeysEnum.Timezone:
                    _timezone = TimeZoneInfo.Local;
                    return;
                case KeysEnum.ConnTimeout:
                    _connTimeout = TimeSpan.Zero;
                    return;
                case KeysEnum.ReadTimeout:
                    _readTimeout = TimeSpan.Zero;
                    return;
                case KeysEnum.WriteTimeout:
                    _writeTimeout = TimeSpan.Zero;
                    return;
                default:
                    throw new ArgumentOutOfRangeException(nameof(index), index, null);
            }
        }

        public override bool Remove(string keyword)
        {
            if (!KeysDict.TryGetValue(keyword, out var index)
                || !base.Remove(KeysList[(int)index]))
            {
                return false;
            }

            Reset(index);

            return true;
        }

        public override void Clear()
        {
            base.Clear();

            for (var i = 0; i < KeysList.Count; i++)
            {
                Reset((KeysEnum)i);
            }
        }

        public void DefaultNative()
        {
            Port = 6030;
            Host = String.Empty;
            Protocol = TDengineConstant.ProtocolNative;
        }


        public void DefaultWebSocket()
        {
            Port = 6041;
            Host = "localhost";
            Protocol = TDengineConstant.ProtocolWebSocket;
        }
    }
}