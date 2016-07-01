// <copyright file="SQLiteConnector.cs" company="Anura Code">
// All rights reserved.
// </copyright>
// <author>Alberto Puyana</author>

using SQLite.Net;
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
            var documentspath = System.Environment.GetFolderPath(System.Environment.SpecialFolder.Personal);
            var path = Path.Combine(documentspath, fullFileName);

            var platfrom = new global::SQLite.Net.Platform.XamarinAndroid.SQLitePlatformAndroid();
            var connString = new SQLiteConnectionString(path, true);
            var sqliteConnection = new global::SQLite.Net.SQLiteConnectionWithLock(platfrom, connString);

            return sqliteConnection;
        }
    }
}