using System;
using System.Collections.Generic;


namespace TDengineTMQ
{
    public class ConsumerConfig : Config
    {
        public ConsumerConfig() : base() { }

        public ConsumerConfig(Config config) : base(config) { }

        public ConsumerConfig(Dictionary<string, string> config) : base(config) { }

        public string GourpId
        {
            get
            {
                return Get("group.id");
            }
            set
            {
                this.SetObject("group.id", value);
            }
        }
        public string? ClientId
        {
            get
            {
                return Get("client.id");
            }
            set
            {
                this.SetObject("client.id", value);
            }
        }

        public string? EnableAutoCommit
        {
            get
            {
                return Get("enable.auto.commit");
            }
            set
            {
                this.SetObject("enable.auto.commit", value);
            }
        }

        public string? AutoCommitIntervalMs
        {
            get => Get("auto.commit.interval.ms");

            set => this.SetObject("auto.commit.interval.ms", value);
        }


        public string? AutoOffsetReset
        {
            get => Get("auto.offset.reset");
            set => this.SetObject("auto.offset.reset", value);
        }

        public string? MsgWithTableName
        {
            get => Get("msg.with.table.name");
            set => this.SetObject("msg.with.table.name", value);
        }

        public string? TDConnectIp
        {
            get => Get("td.connect.ip");
            set => this.SetObject("td.connect.ip", value);
        }

        public string? TDConnectUser
        {
            get => Get("td.connect.ip");
            set => this.SetObject("td.connect.ip", value);
        }

        public string? TDConnectPasswd
        {
            get => Get("td.connect.pass");
            set => SetObject("td.connect.pass", value);
        }

        public string? TDConnectPort
        {
            get => Get("td.connect.port");
            set => this.SetObject("td.connect.port", value);
        }

        public string? TDDatabase
        {
            get => Get("td.connect.db");
            set => this.SetObject("td.connect.db", value);
        }
    }
}
