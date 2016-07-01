// <copyright file="CrossConnection.cs" company="Anura Code">
// All rights reserved.
// </copyright>
// <author>Alberto Puyana</author>

namespace Microsoft.WindowsAzure.Mobile.SQLite
{
    /// <summary>
    /// Cross platform sqlite connection
    /// </summary>
    public static class CrossConnectionExtension
    {
        /// <summary>
        /// Init the connection.
        /// </summary>
        /// <param name="connection">Connection to use.</param>
        public static void Init(this CrossConnection connection)
        {
            if (CrossConnection.PlatformCreationDelegate == null)
            {
                CrossConnection.PlatformCreationDelegate = () => { return new global::SQLite.Net.Platform.XamarinIOS.SQLitePlatformIOS(); };
            }
        }
    }
}