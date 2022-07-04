using System;
using Examples.UtilsTools;
using System.Runtime.InteropServices;
using TDengineDriver;
using Examples.Data;
using System.Collections.Generic;

namespace Examples
{
    public class EntryPoint
    {
        static void Main(string[] args)
        {
            IntPtr conn = Tools.TDConnection();


            InitData data = new InitData();
            data.Create(conn, "tmp_db", "sb", true);
            data.InsertData(conn, "tmp_db", "sb", "sb_01", 5);
            //data.Drop(conn, "tmp_db", null);

            InitData data2 = new InitData();
            data2.Create(conn, "tmp_db", "tb", false);
            data2.InsertData(conn, "tmp_db", null, "tb", 5);
            //data2.Drop(conn,null,"tb");

            // Query
            Query.QueryData(conn, "query", "q", "q_01", 5);

            // Stmt
            Tools.CloseConnection(conn);


        }
    }
}
