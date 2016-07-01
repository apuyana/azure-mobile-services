using Foundation;
using Microsoft.WindowsAzure.Mobile.SQLite;
using Microsoft.WindowsAzure.MobileServices;
using Microsoft.WindowsAzure.MobileServices.SQLiteStore.Test.UnitTests;
using Microsoft.WindowsAzure.MobileServices.TestFramework;
using UIKit;

namespace Microsoft.WindowsAzure.Mobile.SQLiteStore.iOS.Test
{
    [Register("AppDelegate")]
    public partial class AppDelegate : UIApplicationDelegate
    {
        private UIWindow window;

        static AppDelegate()
        {
            SQLite.CrossConnection.Instance.Init();
            CurrentPlatform.Init();

            Harness = new TestHarness();
            Harness.LoadTestAssembly(typeof(SQLiteStoreTests).Assembly);
        }

        public static TestHarness Harness { get; private set; }

        public override bool FinishedLaunching(UIApplication app, NSDictionary options)
        {
            window = new UIWindow(UIScreen.MainScreen.Bounds);
            window.RootViewController = new UINavigationController(new LoginViewController());
            window.MakeKeyAndVisible();

            return true;
        }
    }
}