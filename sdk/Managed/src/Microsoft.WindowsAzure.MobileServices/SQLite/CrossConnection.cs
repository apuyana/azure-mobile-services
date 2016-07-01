// <copyright file="CrossConnection.cs" company="Anura Code">
// All rights reserved.
// </copyright>
// <author>Alberto Puyana</author>

using SQLite.Net.Interop;
using System;

namespace Microsoft.WindowsAzure.Mobile.SQLite
{
    /// <summary>
    /// Cross platform sqlite connection
    /// </summary>
    public class CrossConnection
    {
        /// <summary>
        /// Lazy implementation.
        /// </summary>
        private static Lazy<ISQLitePlatform> Implementation = new Lazy<ISQLitePlatform>(() => CreatePlatform(), System.Threading.LazyThreadSafetyMode.PublicationOnly);

        /// <summary>
        /// Cross connection instance.
        /// </summary>
        private static CrossConnection instance = new CrossConnection();

        /// <summary>
        /// Platform creation delegate.
        /// </summary>
        private static Func<ISQLitePlatform> platformCreationDelegate;

        /// <summary>
        /// Current settings to use
        /// </summary>
        public static ISQLitePlatform Current
        {
            get
            {
                var ret = Implementation.Value;
                if (ret == null)
                {
                    throw NotImplementedInReferenceAssembly();
                }
                return ret;
            }
        }

        /// <summary>
        /// Cross connection instance.
        /// </summary>
        public static CrossConnection Instance
        {
            get
            {
                return instance;
            }
        }

        /// <summary>
        /// Platform creation delegate.
        /// </summary>
        public static Func<ISQLitePlatform> PlatformCreationDelegate
        {
            get
            {
                return platformCreationDelegate;
            }

            set
            {
                platformCreationDelegate = value;

                Dispose();
            }
        }

        /// <summary>
        /// Dispose of everything
        /// </summary>
        public static void Dispose()
        {
            if (Implementation != null && Implementation.IsValueCreated)
            {
                Implementation = new Lazy<ISQLitePlatform>(() => CreatePlatform(), System.Threading.LazyThreadSafetyMode.PublicationOnly);
            }
        }

        /// <summary>
        /// Not implemented Exception.
        /// </summary>
        /// <returns>Exception to use.</returns>
        internal static Exception NotImplementedInReferenceAssembly()
        {
            return new NotImplementedException("This functionality is not implemented in the portable version of this assembly.  You should reference the NuGet package from your main application project in order to reference the platform-specific implementation.");
        }

        /// <summary>
        /// Create platform.
        /// </summary>
        /// <returns>Platform to use.</returns>
        private static ISQLitePlatform CreatePlatform()
        {
            ISQLitePlatform platform = null;

            if (PlatformCreationDelegate != null)
            {
                platform = PlatformCreationDelegate();
            }

            return platform;
        }
    }
}