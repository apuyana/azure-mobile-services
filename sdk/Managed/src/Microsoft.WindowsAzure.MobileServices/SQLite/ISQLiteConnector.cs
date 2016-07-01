// <copyright file="ISQLiteConnector.cs" company="Anura Code">
// All rights reserved.
// </copyright>
// <author>Alberto Puyana</author>

using SQLite.Net;

namespace Microsoft.WindowsAzure.Mobile.SQLite
{
    /// <summary>
    /// Interface for creating a sqlite connection.
    /// </summary>
    public interface ISQLiteConnector
    {
        /// <summary>
        /// Gets a connection to a sqllite database.
        /// </summary>
        /// <param name="fileName">File name to use.</param>
        /// <param name="extension">Extension to append.</param>
        /// <returns>Connection to use.</returns>
        SQLiteConnectionWithLock GetConnection(string fileName, string extension = ".db3");
    }
}