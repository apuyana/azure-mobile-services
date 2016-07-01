// ----------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ----------------------------------------------------------------------------

using System;
using Microsoft.WindowsAzure.MobileServices.Sync;
using SQLite.Net;

namespace Microsoft.WindowsAzure.MobileServices.SQLiteStore.Test
{
    internal class TestUtilities
    {
        public static void ResetDatabase(string dbName)
        {
            TestUtilities.DropTestTable(dbName, MobileServiceLocalSystemTables.OperationQueue);
            TestUtilities.DropTestTable(dbName, MobileServiceLocalSystemTables.SyncErrors);
            TestUtilities.DropTestTable(dbName, MobileServiceLocalSystemTables.Config);
        }

        public static void DropTestTable(string dbName, string tableName)
        {
            ExecuteNonQuery(dbName, "DROP TABLE IF EXISTS " + tableName);
        }

        public static long CountRows(string dbName, string tableName)
        {
            long count;
            string sql = "SELECT COUNT(1) from " + tableName;

            using (var connection = new SQLiteConnection(Mobile.SQLite.CrossConnection.Current, dbName))
            {
                var command = connection.CreateCommand(sql);
                count = command.ExecuteScalar<int>();
            }

            return count;
        }

        public static void Truncate(string dbName, string tableName)
        {
            ExecuteNonQuery(dbName, "DELETE FROM " + tableName);
        }

        public static void ExecuteNonQuery(string dbName, string sql)
        {
            using (var connection = new SQLiteConnection(Mobile.SQLite.CrossConnection.Current, dbName))
            {
                var command = connection.CreateCommand(sql);
                command.ExecuteNonQuery();
            }
        }
    }
}
