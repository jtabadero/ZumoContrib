using System;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using Microsoft.WindowsAzure.MobileServices;
using Microsoft.WindowsAzure.MobileServices.Sync;
using Newtonsoft.Json;
using ZumoContrib.Sync.SQLStore;

namespace WPFQuickStart
{

    public partial class MainWindow : Window
    {
        private MobileServiceCollection<TodoItem, TodoItem> items;
        //private IMobileServiceTable<TodoItem> todoTable = App.MobileService.GetTable<TodoItem>();
        private IMobileServiceSyncTable<TodoItem> todoTable = App.MobileService.GetSyncTable<TodoItem>();

        public MainWindow()
        {
            InitializeComponent();
        }

        protected override void OnActivated(EventArgs e)
        {
            if (!App.MobileService.SyncContext.IsInitialized)
            {
                //TODO: Comment out to use SQLite
                //var store = new MobileServiceSQLiteStore("localsync.db");

                //TODO: Comment out to use SQLCe
                //NOTE: This will automatically create the SQLCe database file if it's not existing
                // var store = new MobileServiceSqlCeStore(@"c:\temp\offlinestoretest.sdf");
                
                //TODO: Comment out to use SQL (SQL Express/Server/LocalDB/AzureSQL)
                //NOTE: The SQL database need to be existing/pre-created already 
                //var store = new MobileServiceSqlStore(@"Data Source=.;Initial Catalog=OfflineStoreTest;Integrated Security=SSPI;");

                store.DefineTable<TodoItem>();
                App.MobileService.SyncContext.InitializeAsync(store, new MobileServiceSyncHandler());
            }
            RefreshTodoItems();
        }

        private async void InsertTodoItem(TodoItem todoItem)
        {
            // This code inserts a new TodoItem into the database. When the operation completes
            // and Mobile Services has assigned an Id, the item is added to the CollectionView
            await todoTable.InsertAsync(todoItem);
            items.Add(todoItem);
        }

        private async void RefreshTodoItems()
        {
            MobileServiceInvalidOperationException exception = null;
            try
            {
                // This code refreshes the entries in the list view by querying the TodoItems table.
                // The query excludes completed TodoItems
                items = await todoTable
                    .Where(todoItem => todoItem.Complete == false)
                    .ToCollectionAsync();
            }
            catch (MobileServiceInvalidOperationException e)
            {
                exception = e;
            }

            if (exception != null)
            {
                MessageBox.Show(exception.Message, "Error loading items");
            }
            else
            {
                ListItems.ItemsSource = items;
            }
        }

        private async void UpdateCheckedTodoItem(TodoItem item)
        {
            // This code takes a freshly completed TodoItem and updates the database. When the MobileService 
            // responds, the item is removed from the list 
            await todoTable.UpdateAsync(item);
            items.Remove(item);
        }

        private void ButtonRefresh_Click(object sender, RoutedEventArgs e)
        {
            RefreshTodoItems();
        }

        private void ButtonSave_Click(object sender, RoutedEventArgs e)
        {
            var todoItem = new TodoItem { Text = TextInput.Text };
            InsertTodoItem(todoItem);
        }

        private void CheckBoxComplete_Checked(object sender, RoutedEventArgs e)
        {
            CheckBox cb = (CheckBox)sender;
            TodoItem item = cb.DataContext as TodoItem;
            UpdateCheckedTodoItem(item);
        }

        private async void ButtonPull_Click(object sender, RoutedEventArgs e)
        {
            Exception pullException = null;
            try
            {
                await todoTable.PullAsync(todoTable.Where(todoItem => todoItem.Complete == false));
                RefreshTodoItems();
            }
            catch (Exception ex)
            {
                pullException = ex;
            }
            if (pullException != null)
            {
                MessageBox.Show("Pull failed: " + pullException.Message +
                  "\n\nIf you are in an offline scenario, " +
                  "try your Pull again when connected with your Mobile Service.");

            }
        }
        private async void ButtonPush_Click(object sender, RoutedEventArgs e)
        {
            string errorString = null;
            try
            {
                await App.MobileService.SyncContext.PushAsync();
                RefreshTodoItems();
            }
            catch (MobileServicePushFailedException ex)
            {
                errorString = "Push failed because of sync errors: " +
                  ex.PushResult.Errors.Count() + ", message: " + ex.Message;
            }
            catch (Exception ex)
            {
                errorString = "Push failed: " + ex.Message;
            }
            if (errorString != null)
            {
                MessageBox.Show(errorString +
                  "\n\nIf you are in an offline scenario, " +
                  "try your Push again when connected with your Mobile Serice.");

            }
        }
    }

    public class TodoItem
    {
        public string Id { get; set; }
        [JsonProperty(PropertyName = "text")]
        public string Text { get; set; }
        [JsonProperty(PropertyName = "complete")]
        public bool Complete { get; set; }
        [Version]
        public string Version { get; set; }
    }
}
