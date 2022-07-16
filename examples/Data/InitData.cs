using System;
using System.Collections.Generic;
using System.Text;
using Examples.UtilsTools;

namespace Examples.Data
{

    internal class InitData
    {
        internal string createDB { get; set; }
        internal string useDB { get; set; }
        internal string createTable { get; set; }
        internal string dropTable { get; set; }
        internal string dropDB { get; set; }
        internal string insert { get; set; }


        //
        private void initSql(string db, string? stable, string? table)
        {
            StringBuilder sb = new StringBuilder();
            string tableName = String.IsNullOrEmpty(stable) ? table : stable;
            Console.WriteLine($"tablename:{tableName}");
            // use db;
            sb.Append("use ");
            sb.Append(db);
            useDB = sb.ToString();
            sb.Clear();

            //create table
            sb.Append($"create table if not exists ");
            sb.Append(tableName);
            sb.Append("(");
            sb.Append(" ts timestamp");
            sb.Append(",v1 tinyint");
            sb.Append(",v2 smallint");
            sb.Append(",v4 int");
            sb.Append(",v8 bigint");
            sb.Append(",u1 tinyint unsigned");
            sb.Append(",u2 smallint unsigned");
            sb.Append(",u4 int unsigned");
            sb.Append(",u8 bigint unsigned");
            sb.Append(",f4 float");
            sb.Append(",f8 double");
            sb.Append(",bin binary(200)");
            sb.Append(",nchr nchar(200)");
            sb.Append(",b bool");
            sb.Append(",nilcol int");
            sb.Append(")");

            if (!String.IsNullOrEmpty(stable))
            {
                sb.Append("tags");
                sb.Append("(");
                sb.Append(" bo bool");
                sb.Append(",tt tinyint");
                sb.Append(",si smallint");
                sb.Append(",ii int");
                sb.Append(",bi bigint");
                sb.Append(",tu tinyint unsigned");
                sb.Append(",su smallint unsigned");
                sb.Append(",iu int unsigned");
                sb.Append(",bu bigint unsigned");
                sb.Append(",ff float");
                sb.Append(",dd double");
                sb.Append(",bb binary(200)");
                sb.Append(",nc nchar(200)");
                sb.Append(")");
            }
            createTable = sb.ToString();
            sb.Clear();

            // drop table
            sb.Append("drop table if exists ");
            sb.Append(tableName);
            dropTable = sb.ToString();
            sb.Clear();

            //db
            if (!string.IsNullOrEmpty(db))
            {
                sb.Append("create database if not exists ");
                sb.Append(db);
                sb.Append(" keep 36500");
                createDB = sb.ToString();
                sb.Clear();
                //sb.Append("drop database if exists ");
                //sb.Append(db);
                //dropDB = sb.ToString();
                //sb.Clear();
            }

        }
        internal void Create(IntPtr conn, string db, string table, bool ifStable)
        {
            if (ifStable)
            {
                initSql(db, table, null);
            }
            else
            {
                initSql(db, null, table);
            }

            if (string.IsNullOrEmpty(db))
            {
                Tools.ExecuteUpdate(conn, useDB);
                Tools.ExecuteUpdate(conn, createTable);
            }
            else
            {
                Tools.ExecuteUpdate(conn, createDB);
                Tools.ExecuteUpdate(conn, useDB);
                Tools.ExecuteUpdate(conn, createTable);
            }
        }

        internal void Drop(IntPtr conn, string? db, string? table)
        {
            var sql = new StringBuilder();
            if (string.IsNullOrEmpty(db) && (!string.IsNullOrEmpty(table)))
            {
                sql.Append("drop table if exists ");
                sql.Append(table);
                dropDB = sql.ToString();
                Tools.ExecuteUpdate(conn, dropTable);
                sql.Clear();
            }
            else
            {
                sql.Append("drop database if exists ");
                sql.Append(db);
                dropDB = sql.ToString();
                Tools.ExecuteUpdate(conn, dropDB);
                sql.Clear();
            }
        }

        internal void InsertData(IntPtr conn, string? db, string? stable, string table, int numOfRows,long begin = 1656677700000)
        {
            StringBuilder sb = new StringBuilder();
            long ts = begin;
            sb.Append("insert into ");

            if (string.IsNullOrEmpty(stable))
            {
                sb.Append(table);
            }
            else
            {
                sb.Append(table);
                sb.Append(" using ");
                sb.Append(stable);
                sb.Append(" tags ");
                sb.Append('(');
                sb.Append(table.Length % 2 == 0 ? "true" : "false");
                sb.Append(',');
                sb.Append(-1);
                sb.Append(',');
                sb.Append(-2);
                sb.Append(',');
                sb.Append(-3);
                sb.Append(',');
                sb.Append(-4);
                sb.Append(',');
                sb.Append(1);
                sb.Append(',');
                sb.Append(2);
                sb.Append(',');
                sb.Append(3);
                sb.Append(',');
                sb.Append(4);
                sb.Append(',');
                sb.Append(5.0F);
                sb.Append(',');
                sb.Append(5.550D);
                sb.Append(',');
                sb.Append("\'varchar_tag\'");
                sb.Append(',');
                sb.Append("\'nchar_tag\'");
                sb.Append(')');
            }
            sb.Append(" values");
            for (int i = 0; i < numOfRows; i++)
            {
                sb.Append('(');
                sb.Append(ts);
                ts = ts + 100;
                sb.Append(',');
                sb.Append(i + 0); //tiny int
                sb.Append(',');
                sb.Append(i + 1); // short
                sb.Append(',');
                sb.Append(i + 2); //int
                sb.Append(',');
                sb.Append(i + 3); //long
                sb.Append(',');
                sb.Append(i + 0); //byte
                sb.Append(',');
                sb.Append(i + 1); //ushort
                sb.Append(',');
                sb.Append(i + 2); //uint
                sb.Append(',');
                sb.Append(i + 3); //ulong
                sb.Append(',');
                sb.Append(i * 1F); //float
                sb.Append(',');
                sb.Append(i * 2D); //double
                sb.Append(',');
                sb.Append("\'varchar_col_" + i + "\'");
                sb.Append(',');
                sb.Append("\'nchar_col_" + i + "\'");
                sb.Append(',');
                sb.Append(i % 2 == 0 ? "true" : "false");
                sb.Append(',');
                sb.Append("NULL");
                sb.Append(')');
            }
            sb.Append(';');
            insert = sb.ToString();
            Tools.ExecuteUpdate(conn, insert);
        }
    }
}
