using System.Windows;
using Microsoft.WindowsAzure.MobileServices;

namespace WPFQuickStart
{
    /// <summary>
    /// Interaction logic for App.xaml
    /// </summary>
    public partial class App : Application
    {
        public static MobileServiceClient MobileService = new MobileServiceClient(
             "https://<yourservice>.azure-mobile.net/",
             "<yourapplicationkey>"
         );
    }
}
