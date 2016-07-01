// <copyright file="SQLiteConnector.cs" company="Anura Code">
// All rights reserved.
// </copyright>
// <author>Alberto Puyana</author>

using SQLite.Net;
using System;
using System.IO;

namespace Microsoft.WindowsAzure.Mobile.SQLite
{
    /// <summary>
    /// Component for creating a sqlite connection.
    /// </summary>
    public class SQLiteConnector : SQLiteConnectorBase
    {
        /// <summary>
        /// Gets a connection to a sqllite database.
        /// </summary>
        /// <param name="fullFileName">Filename with extension.</param>
        /// <returns></returns>
        protected override SQLiteConnectionWithLock GetConnectionPlatform(string fullFileName)
        {
            var documentsPath = Environment.GetFolderPath(Environment.SpecialFolder.Personal);
            var libraryPath = Path.Combine(documentsPath, "..", "Library");
            var path = Path.Combine(libraryPath, fullFileName);

            var platfrom = new global::SQLite.Net.Platform.XamarinIOS.SQLitePlatformIOS();
            var connString = new SQLiteConnectionString(path, true);
            var sqliteConnection = new global::SQLite.Net.SQLiteConnectionWithLock(platfrom, connString);

            return sqliteConnection;
        }
    }
}