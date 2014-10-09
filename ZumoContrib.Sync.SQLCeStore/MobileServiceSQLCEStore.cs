using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlServerCe;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.MobileServices;
using Microsoft.WindowsAzure.MobileServices.Query;
using Microsoft.WindowsAzure.MobileServices.Sync;
using Newtonsoft.Json.Linq;

namespace ZumoContrib.Sync.SQLCeStore
{
    public class MobileServiceSqlCeStore : MobileServiceLocalStore
    {
        private const int MaxParametersPerUpsertQuery = 1000; //this limit is 2100, but let's set it to 1000

        private Dictionary<string, TableDefinition> tableMap = new Dictionary<string, TableDefinition>(StringComparer.OrdinalIgnoreCase);

        private readonly string _dataSource;

        protected MobileServiceSqlCeStore() { }

        /// <summary>
        /// Initializes a new instance of <see cref="MobileServiceSqlCeStore"/>
        /// </summary>
        /// <param name="fileName">Name/path of the local SQLCe database file.</param>
        public MobileServiceSqlCeStore(string fileName)
        {
            if (fileName == null)
            {
                throw new ArgumentNullException("fileName");
            }

            _dataSource = string.Format("Data Source={0}", fileName);

            if (!File.Exists(fileName))
            {
                using (var engine = new SqlCeEngine(_dataSource))
                {
                    engine.CreateDatabase();
                }
            }

        }

        /// <summary>
        /// Defines the local table on the store.
        /// </summary>
        /// <param name="tableName">Name of the local table.</param>
        /// <param name="item">An object that represents the structure of the table.</param>
        public override void DefineTable(string tableName, JObject item)
        {
            if (tableName == null)
            {
                throw new ArgumentNullException("tableName");
            }
            if (item == null)
            {
                throw new ArgumentNullException("item");
            }

            if (this.Initialized)
            {
                throw new InvalidOperationException(Properties.Resources.SqlCeStore_DefineAfterInitialize);
            }

            // add id if it is not defined
            JToken ignored;
            if (!item.TryGetValue(MobileServiceSystemColumns.Id, StringComparison.OrdinalIgnoreCase, out ignored))
            {
                item[MobileServiceSystemColumns.Id] = String.Empty;
            }

            var tableDefinition = (from property in item.Properties()
                                   let storeType = GetStoreType(property)
                                   select new ColumnDefinition(property.Name, property.Value.Type, storeType))
                                  .ToDictionary(p => p.Name, StringComparer.OrdinalIgnoreCase);

            var sysProperties = GetSystemProperties(item);

            this.tableMap.Add(tableName, new TableDefinition(tableDefinition, sysProperties));
        }

        protected override async Task OnInitialize()
        {
            this.CreateAllTables();
            await this.InitializeConfig();
        }

        /// <summary>
        /// Reads data from local store by executing the query.
        /// </summary>
        /// <param name="query">The query to execute on local store.</param>
        /// <returns>A task that will return with results when the query finishes.</returns>
        public override Task<JToken> ReadAsync(MobileServiceTableQueryDescription query)
        {
            if (query == null)
            {
                throw new ArgumentNullException("query");
            }

            this.EnsureInitialized();

            var formatter = new SqlQueryFormatter(query);
            string sql = formatter.FormatSelect();

            IList<JObject> rows = this.ExecuteQuery(query.TableName, sql, formatter.Parameters);
            JToken result = new JArray(rows.ToArray());

            if (query.IncludeTotalCount)
            {
                sql = formatter.FormatSelectCount();
                IList<JObject> countRows = this.ExecuteQuery(query.TableName, sql, formatter.Parameters);
                long count = countRows[0].Value<long>("count");
                result = new JObject() 
                { 
                    { "results", result },
                    { "count", count}
                };
            }

            return Task.FromResult(result);
        }

        /// <summary>
        /// Updates or inserts data in local table.
        /// </summary>
        /// <param name="tableName">Name of the local table.</param>
        /// <param name="items">A list of items to be inserted.</param>
        /// <param name="fromServer"><code>true</code> if the call is made based on data coming from the server e.g. in a pull operation; <code>false</code> if the call is made by the client, such as insert or update calls on an <see cref="IMobileServiceSyncTable"/>.</param>
        /// <returns>A task that completes when item has been upserted in local table.</returns>
        public override Task UpsertAsync(string tableName, IEnumerable<JObject> items, bool fromServer)
        {
            if (tableName == null)
            {
                throw new ArgumentNullException("tableName");
            }
            if (items == null)
            {
                throw new ArgumentNullException("items");
            }

            this.EnsureInitialized();

            return UpsertAsyncInternal(tableName, items, fromServer);
        }

        private Task UpsertAsyncInternal(string tableName, IEnumerable<JObject> items, bool fromServer)
        {
            TableDefinition table = GetTable(tableName);

            var first = items.FirstOrDefault();
            if (first == null)
            {
                return Task.FromResult(0);
            }

            // Get the columns which we want to map into the database.
            var columns = new List<ColumnDefinition>();
            foreach (var prop in first.Properties())
            {
                ColumnDefinition column;

                // If the column is coming from the server we can just ignore it,
                // otherwise, throw to alert the caller that they have passed an invalid column
                if (!table.TryGetValue(prop.Name, out column) && !fromServer)
                {
                    throw new InvalidOperationException(string.Format(Properties.Resources.SqlCeStore_ColumnNotDefined, prop.Name, tableName));
                }

                if (column != null)
                {
                    columns.Add(column);
                }
            }

            if (columns.Count == 0)
            {
                // no query to execute if there are no columns in the table
                return Task.FromResult(0);
            }

            var insertSqlBase = String.Format(
                "INSERT INTO {0} ({1}) VALUES ",
                SqlHelpers.FormatTableName(tableName),
                String.Join(", ", columns.Select(c => c.Name).Select(SqlHelpers.FormatMember))
                );

            var updateSqlBase = String.Format("UPDATE {0} SET ", SqlHelpers.FormatTableName(tableName));

            // Use int division to calculate how many times this record will fit into our parameter quota
            int batchSize = MaxParametersPerUpsertQuery / columns.Count;
            if (batchSize == 0)
            {
                throw new InvalidOperationException(string.Format(Properties.Resources.SqlCeStore_TooManyColumns, MaxParametersPerUpsertQuery));
            }

            foreach (var batch in items.Split(maxLength: batchSize))
            {
                StringBuilder sql = new StringBuilder();

                var parameters = new Dictionary<string, object>();

                foreach (JObject item in batch)
                {
                    // there's no upsert in SQL CE so we'll settle if an 'If Not Exist, Insert' approach
                    if (!RowExists(SqlHelpers.FormatTableName(tableName),
                            item.GetValue(MobileServiceSystemColumns.Id, StringComparison.OrdinalIgnoreCase).ToString()))
                    {
                        sql = new StringBuilder(insertSqlBase);
                        AppendInsertValuesSql(sql, parameters, columns, item);

                    }
                    else
                    {
                        sql = new StringBuilder(updateSqlBase);
                        AppendUpdateValuesSql(sql, parameters, columns, item);
                        string updateCondition = string.Format(" WHERE {0} = '{1}'", MobileServiceSystemColumns.Id, item.GetValue(MobileServiceSystemColumns.Id, StringComparison.OrdinalIgnoreCase).ToString());
                        sql.Append(updateCondition);


                    }

                    if (parameters.Any())
                    {
                        this.ExecuteNonQuery(sql.ToString(), parameters);
                    }
                }
            }

            return Task.FromResult(0);
        }

        private static void AppendInsertValuesSql(StringBuilder sql, Dictionary<string, object> parameters, List<ColumnDefinition> columns, JObject item)
        {
            sql.Append("(");
            int colCount = 0;
            foreach (var column in columns)
            {
                if (colCount > 0)
                    sql.Append(",");

                JToken rawValue = item.GetValue(column.Name, StringComparison.OrdinalIgnoreCase);
                object value = SqlHelpers.SerializeValue(rawValue, column.StoreType, column.JsonType);

                //The paramname for this field must be unique within this statement
                string paramName = "@p" + parameters.Count;

                sql.Append(paramName);
                parameters[paramName] = value;

                colCount++;
            }
            sql.Append(")");
        }

        private static void AppendUpdateValuesSql(StringBuilder sql, Dictionary<string, object> parameters, List<ColumnDefinition> columns, JObject item)
        {
            int colCount = 0;
            foreach (var column in columns)
            {
                if (colCount > 0)
                    sql.Append(",");

                JToken rawValue = item.GetValue(column.Name, StringComparison.OrdinalIgnoreCase);
                object value = SqlHelpers.SerializeValue(rawValue, column.StoreType, column.JsonType);

                //The paramname for this field must be unique within this statement
                string paramName = "@p" + parameters.Count;

                sql.Append(SqlHelpers.FormatMember(column.Name) + " = " + paramName);
                parameters[paramName] = value;

                colCount++;
            }
        }

        /// <summary>
        /// Deletes items from local table that match the given query.
        /// </summary>
        /// <param name="query">A query to find records to delete.</param>
        /// <returns>A task that completes when delete query has executed.</returns>
        public override Task DeleteAsync(MobileServiceTableQueryDescription query)
        {
            if (query == null)
            {
                throw new ArgumentNullException("query");
            }

            this.EnsureInitialized();

            var formatter = new SqlQueryFormatter(query);
            string sql = formatter.FormatDelete();

            this.ExecuteNonQuery(sql, formatter.Parameters);

            return Task.FromResult(0);
        }

        /// <summary>
        /// Deletes items from local table with the given list of ids
        /// </summary>
        /// <param name="tableName">Name of the local table.</param>
        /// <param name="ids">A list of ids of the items to be deleted</param>
        /// <returns>A task that completes when delete query has executed.</returns>
        public override Task DeleteAsync(string tableName, IEnumerable<string> ids)
        {
            if (tableName == null)
            {
                throw new ArgumentNullException("tableName");
            }
            if (ids == null)
            {
                throw new ArgumentNullException("ids");
            }

            this.EnsureInitialized();

            string idRange = String.Join(",", ids.Select((_, i) => "@id" + i));

            string sql = string.Format("DELETE FROM {0} WHERE {1} IN ({2})",
                                       SqlHelpers.FormatTableName(tableName),
                                       MobileServiceSystemColumns.Id,
                                       idRange);

            var parameters = new Dictionary<string, object>();

            int j = 0;
            foreach (string id in ids)
            {
                parameters.Add("@id" + (j++), id);
            }

            this.ExecuteNonQuery(sql, parameters);
            return Task.FromResult(0);
        }

        /// <summary>
        /// Executes a lookup against a local table.
        /// </summary>
        /// <param name="tableName">Name of the local table.</param>
        /// <param name="id">The id of the item to lookup.</param>
        /// <returns>A task that will return with a result when the lookup finishes.</returns>
        public override Task<JObject> LookupAsync(string tableName, string id)
        {
            if (tableName == null)
            {
                throw new ArgumentNullException("tableName");
            }
            if (id == null)
            {
                throw new ArgumentNullException("id");
            }

            this.EnsureInitialized();

            string sql = string.Format("SELECT * FROM {0} WHERE {1} = @id", SqlHelpers.FormatTableName(tableName), MobileServiceSystemColumns.Id);
            var parameters = new Dictionary<string, object>
            {
                {"@id", id}
            };

            IList<JObject> results = this.ExecuteQuery(tableName, sql, parameters);

            return Task.FromResult(results.FirstOrDefault());
        }

        private TableDefinition GetTable(string tableName)
        {
            TableDefinition table;
            if (!this.tableMap.TryGetValue(tableName, out table))
            {
                throw new InvalidOperationException(string.Format(Properties.Resources.SqlCeStore_TableNotDefined, tableName));
            }
            return table;
        }

        internal virtual async Task SaveSetting(string name, string value)
        {
            var setting = new JObject() 
            { 
                { "id", name }, 
                { "value", value } 
            };
            await this.UpsertAsyncInternal(MobileServiceLocalSystemTables.Config, new[] { setting }, fromServer: false);
        }

        private async Task InitializeConfig()
        {
            foreach (KeyValuePair<string, TableDefinition> table in this.tableMap)
            {
                if (!MobileServiceLocalSystemTables.All.Contains(table.Key))
                {
                    // preserve system properties setting for non-system tables
                    string name = String.Format("{0}_systemProperties", table.Key);
                    string value = ((int)table.Value.SystemProperties).ToString();
                    await this.SaveSetting(name, value);
                }
            }
        }

        private void CreateAllTables()
        {
            foreach (KeyValuePair<string, TableDefinition> table in this.tableMap)
            {
                this.CreateTableFromObject(table.Key, table.Value.Values);
            }
        }

        private bool TableExists(string tableName)
        {
            using (var conn = new SqlCeConnection(_dataSource))
            {
                var sql = string.Format("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE table_name = '{0}'", tableName);
                using (var command = new SqlCeCommand(sql, conn))
                {
                    conn.Open();

                    var count = Convert.ToInt32(command.ExecuteScalar());
                    return (count > 0);
                }
            }
        }

        private bool RowExists(string tableName, string id)
        {
            using (var conn = new SqlCeConnection(_dataSource))
            {
                var sql = string.Format("SELECT COUNT(*) FROM {0} WHERE {1} = @id", tableName, MobileServiceSystemColumns.Id);

                using (var command = new SqlCeCommand(sql, conn))
                {
                    conn.Open();
                    command.Parameters.AddWithValue("@id", id);
                    var count = Convert.ToInt32(command.ExecuteScalar());
                    return (count > 0);
                }
            }
        }

        internal virtual void CreateTableFromObject(string tableName, IEnumerable<ColumnDefinition> columns)
        {
            if (!TableExists(tableName))
            {
                //Id in Zumo is nvarchar(255)
                String tblSql = string.Format("CREATE TABLE {0} ({1} nvarchar(255) PRIMARY KEY)",
                    SqlHelpers.FormatTableName(tableName), SqlHelpers.FormatMember(MobileServiceSystemColumns.Id));
                this.ExecuteNonQuery(tblSql, parameters: null);
            }

            var sql = string.Format("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{0}'", tableName);
            IDictionary<string, JObject> existingColumns = this.ExecuteQuery((TableDefinition)null, sql, parameters: null)
                                                               .ToDictionary(c => c.Value<string>("COLUMN_NAME"), StringComparer.OrdinalIgnoreCase);

            // new columns that do not exist in existing columns of table
            var columnsToCreate = columns.Where(c => !existingColumns.ContainsKey(c.Name));

            foreach (ColumnDefinition column in columnsToCreate)
            {
                string createSql = string.Format("ALTER TABLE {0} ADD {1} {2}",
                                                 SqlHelpers.FormatTableName(tableName),
                                                 SqlHelpers.FormatMember(column.Name),
                                                 column.StoreType);
                this.ExecuteNonQuery(createSql, parameters: null);
            }

            //TODO: Should we allow dropping columns?
        }

        /// <summary>
        /// Executes a sql statement on a given table in local SQLite database.
        /// </summary>
        /// <param name="sql">SQL statement to execute.</param>
        /// <param name="parameters">The query parameters.</param>
        protected virtual void ExecuteNonQuery(string sql, IDictionary<string, object> parameters)
        {
            parameters = parameters ?? new Dictionary<string, object>();

            using (var conn = new SqlCeConnection(_dataSource))
            {

                using (var sqlCeCommand = new SqlCeCommand(sql, conn))
                {
                    foreach (KeyValuePair<string, object> parameter in parameters)
                    {
                        sqlCeCommand.Parameters.AddWithValue(parameter.Key, parameter.Value ?? DBNull.Value);
                    }

                    conn.Open();
                    var result = sqlCeCommand.ExecuteNonQuery();
                    //    ValidateResult(result);
                }
            }

        }

        /// <summary>
        /// Executes a sql statement on a given table in local SQLCe database.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="sql">The SQL query to execute.</param>
        /// <param name="parameters">The query parameters.</param>
        /// <returns>The result of query.</returns>
        protected virtual IList<JObject> ExecuteQuery(string tableName, string sql, IDictionary<string, object> parameters)
        {
            TableDefinition table = GetTable(tableName);
            return this.ExecuteQuery(table, sql, parameters);
        }

        private IList<JObject> ExecuteQuery(TableDefinition table, string sql, IDictionary<string, object> parameters)
        {
            table = table ?? new TableDefinition();
            parameters = parameters ?? new Dictionary<string, object>();

            var rows = new List<JObject>();
            using (var conn = new SqlCeConnection(_dataSource))
            {

                using (var sqlCeCommand = new SqlCeCommand(sql, conn))
                {
                    foreach (KeyValuePair<string, object> parameter in parameters)
                    {
                        sqlCeCommand.Parameters.AddWithValue(parameter.Key, parameter.Value ?? DBNull.Value);
                    }

                    conn.Open();
                    var reader = sqlCeCommand.ExecuteReader();
                    while (reader.Read())
                    {
                        var row = ReadRow(table, reader);
                        rows.Add(row);
                    }

                    //    ValidateResult(result);

                }
            }

            return rows;
        }

        private JToken DeserializeValue(ColumnDefinition column, object value)
        {
            if (value == null || value.Equals(DBNull.Value))
            {
                return null;
            }

            string sqlType = column.StoreType;
            JTokenType jsonType = column.JsonType;
            if (sqlType == SqlColumnType.BigInt)
            {
                return SqlHelpers.ParseInteger(jsonType, value);
            }
            if (sqlType == SqlColumnType.DateTime)
            {
                var date = (value as DateTime?).GetValueOrDefault();

                if (date.Kind == DateTimeKind.Unspecified)
                {
                    date = DateTime.SpecifyKind(date, DateTimeKind.Utc);
                }
                return date;
            }
            if (sqlType == SqlColumnType.Double)
            {
                return SqlHelpers.ParseDouble(jsonType, value);
            }
            if (sqlType == SqlColumnType.Bit)
            {
                return SqlHelpers.ParseBoolean(jsonType, value);
            }
            if (sqlType == SqlColumnType.UniqueIdentifier)
            {
                return SqlHelpers.ParseUniqueIdentier(jsonType, value);
            }

            if (sqlType == SqlColumnType.NText || sqlType == SqlColumnType.NVarchar)
            {
                return SqlHelpers.ParseText(jsonType, value);
            }

            return null;
        }

        //private static void ValidateResult(SQLiteResult result)
        //{
        //    if (result != SQLiteResult.DONE)
        //    {
        //        throw new SQLiteException(string.Format(Properties.Resources.SQLiteStore_QueryExecutionFailed, result));
        //    }
        //}

        private JObject ReadRow(TableDefinition table, IDataRecord dataRecord)
        {
            var row = new JObject();
            for (int i = 0; i < dataRecord.FieldCount; i++)
            {
                string name = dataRecord.GetName(i);
                object value = dataRecord.GetValue(i);

                ColumnDefinition column;
                if (table.TryGetValue(name, out column))
                {
                    JToken jVal = this.DeserializeValue(column, value);
                    row[name] = jVal;
                }
                else
                {
                    row[name] = value == null ? null : JToken.FromObject(value);
                }
            }
            return row;
        }

        private static MobileServiceSystemProperties GetSystemProperties(JObject item)
        {
            var sysProperties = MobileServiceSystemProperties.None;

            if (item[MobileServiceSystemColumns.Version] != null)
            {
                sysProperties = sysProperties | MobileServiceSystemProperties.Version;
            }
            if (item[MobileServiceSystemColumns.CreatedAt] != null)
            {
                sysProperties = sysProperties | MobileServiceSystemProperties.CreatedAt;
            }
            if (item[MobileServiceSystemColumns.UpdatedAt] != null)
            {
                sysProperties = sysProperties | MobileServiceSystemProperties.UpdatedAt;
            }
            if (item[MobileServiceSystemColumns.Deleted] != null)
            {
                sysProperties = sysProperties | MobileServiceSystemProperties.Deleted;
            }
            return sysProperties;
        }

        private string GetStoreType(JProperty property)
        {
            return SqlHelpers.GetColumnType(property.Value.Type, allowNull: false);
        }
    }
}

