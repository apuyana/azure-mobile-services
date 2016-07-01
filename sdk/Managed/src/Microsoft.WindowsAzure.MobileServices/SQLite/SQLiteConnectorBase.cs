// <copyright file="SQLiteConnectorBase.cs" company="Anura Code">
// All rights reserved.
// </copyright>
// <author>Alberto Puyana</author>

using SQLite.Net;
using System;

namespace Microsoft.WindowsAzure.Mobile.SQLite
{
    /// <summary>
    /// Component for creating a sqlite connection.
    /// </summary>
    public abstract class SQLiteConnectorBase : ISQLiteConnector
    {
        /// <summary>
        /// Gets a connection to a sqllite database.
        /// </summary>
        /// <param name="fileName">File name without extension to use.</param>
        /// <param name="extension">Extension to append.</param>
        /// <returns>Connection to use.</returns>
        public SQLiteConnectionWithLock GetConnection(string fileName, string extension = ".db3")
        {
            if (string.IsNullOrEmpty(fileName))
            {
                throw new ArgumentNullException("fileName", "File name required");
            }

            string fullFileName = fileName;

            if (!string.IsNullOrWhiteSpace(extension))
            {
                fullFileName += extension;
            }

            return GetConnectionPlatform(fullFileName);
        }

        /// <summary>
        /// Gets a connection to a sqllite database.
        /// </summary>
        /// <param name="fullFileName">Filename with extension.</param>
        /// <returns></returns>
        protected abstract SQLiteConnectionWithLock GetConnectionPlatform(string fullFileName);
    }
}