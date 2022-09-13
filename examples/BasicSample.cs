using Sample.UtilsTools;
using System;

namespace examples
{
    internal class BasicSample
    {
        readonly string db = "utf8test";
        readonly string createDB = $"create database if not exists utf8test";
        readonly string dropDB = "drop database if exists utf8test";
        string createTable;
        string insert;
        string select;
        IntPtr conn;

        public BasicSample(IntPtr connection, string table)
        {
            createTable = $"create table if not exists {db}.{table} (ts timestamp,bl bool,i8 tinyint,i16 smallint,i32 int,i64 bigint,u8 tinyint unsigned,u16 smallint unsigned,u32 int unsigned,u64 bigint unsigned,f32 float,d64 double,bnr binary(50),nchr nchar(50))tags(t_bl bool,t_i8 tinyint,t_i16 smallint,t_i32 int,t_i64 bigint,t_u8 tinyint unsigned,t_u16 smallint unsigned,t_u32 int unsigned,t_u64 bigint unsigned,t_f32 float,t_d64 double,t_bnr binary(50),t_nchr nchar(50));";
            insert = $"insert into {db}.{table}_1 using {db}.{table} tags(true,-1,-2,-3,-4,1,2,3,4,3.1415,3.14159265358979,'bnr_tag_1','ncr_tag_1') values (1659283200000,true,-1,-2,-3,-4,1,2,3,4,3.1415,3.14159265358979,'bnr_tag_1','ncr_tag_1')";
            select = $"select * from {db}.{table}";
            conn = connection;
        }
        public void Run()
        {
            UtilsTools.ExecuteUpdate(conn, createDB);
            UtilsTools.ExecuteUpdate(conn, createTable);
            UtilsTools.ExecuteUpdate(conn, insert);
            IntPtr res = UtilsTools.ExecuteQuery(conn, select);
            UtilsTools.DisplayRes(res);
            UtilsTools.FreeResult(res);

        }
        public void CleanBasicSampleData()
        {
            UtilsTools.ExecuteUpdate(conn,dropDB);
        }
    }
}
