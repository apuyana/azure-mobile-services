// ----------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ----------------------------------------------------------------------------

using Microsoft.WindowsAzure.Mobile.SQLite;
using Microsoft.WindowsAzure.MobileServices.SQLiteStore.Test.UnitTests;
using Microsoft.WindowsAzure.MobileServices.TestFramework;
using System.IO;
using System.Reflection;
using Windows.ApplicationModel.Activation;
using Windows.UI.Xaml;
using Windows.UI.Xaml.Controls;

namespace Microsoft.WindowsAzure.MobileServices.SQLiteStore.Test
{
    /// <summary>
    /// Provides application-specific behavior to supplement the default
    /// Application class.
    /// </summary>
    public sealed partial class App : Application
    {
        /// <summary>
        /// Initialize the test harness.
        /// </summary>
        static App()
        {
            Microsoft.WindowsAzure.Mobile.SQLite.CrossConnection.Instance.Init();            

            Harness = new TestHarness();
            Harness.LoadTestAssembly(typeof(SQLiteStoreTests).GetTypeInfo().Assembly);
        }

        /// <summary>
        /// Initializes a new instance of the App class.
        /// </summary>
        public App()
        {
            InitializeComponent();
        }

        /// <summary>
        /// Gets the test harness used by the application.
        /// </summary>
        public static TestHarness Harness { get; private set; }

        /// <summary>
        /// Setup the application and initialize the tests.
        /// </summary>
        /// <param name="args">
        /// Details about the launch request and process.
        /// </param>
        protected override void OnLaunched(LaunchActivatedEventArgs args)
        {
            // Do not repeat app initialization when already running, just
            // ensure that the window is active
            if (args.PreviousExecutionState == ApplicationExecutionState.Running)
            {
                Window.Current.Activate();
                return;
            }

            Frame rootFrame = new Frame();
            rootFrame.Navigate(typeof(MainPage));
            Window.Current.Content = rootFrame;
            Window.Current.Activate();
        }
    }
}