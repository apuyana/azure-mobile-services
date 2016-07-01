// ----------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ----------------------------------------------------------------------------

using Microsoft.WindowsAzure.MobileServices.Query;
using Microsoft.WindowsAzure.MobileServices.Sync;
using Newtonsoft.Json.Linq;
using SQLite.Net;
using SQLite.Net.Interop;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.MobileServices.SQLiteStore
{
    /// <summary>
    /// SQLite based implementation of <see cref="IMobileServiceLocalStore"/>
    /// </summary>
    public class MobileServiceSQLiteStore : MobileServiceLocalStore
    {
        /// <summary>
        /// Datetime format.
        /// </summary>
        private const string DateTimeFormat = "yyyy-MM-ddTHH:mm:ss.fffffffZ";

        /// <summary>
        /// The maximum number of parameters allowed in any "upsert" prepared statement.
        /// Note: The default maximum number of parameters allowed by sqlite is 999
        /// See: http://www.sqlite.org/limits.html#max_variable_number
        /// </summary>
        private const int MaxParametersPerQuery = 800;

        /// <summary>
        /// Pointer to negative.
        /// </summary>
        private static readonly IntPtr NegativePointer = new IntPtr(-1);

        /// <summary>
        /// Sql connection.
        /// </summary>
        private SQLiteConnection connection;

        /// <summary>
        /// Table map.
        /// </summary>
        private Dictionary<string, TableDefinition> tableMap = new Dictionary<string, TableDefinition>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Initializes a new instance of <see cref="MobileServiceSQLiteStore"/>
        /// </summary>
        /// <param name="fileName">Name of the local SQLite database file.</param>
        public MobileServiceSQLiteStore(string fileName)
        {
            if (fileName == null)
            {
                throw new ArgumentNullException("fileName");
            }

            this.connection = Mobile.SQLite.CrossConnection.Connector.GetConnection(fileName);
        }

        /// <summary>
        /// Default constructor.
        /// </summary>
        protected MobileServiceSQLiteStore() { }        

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
                throw new InvalidOperationException(Properties.Resources.SQLiteStore_DefineAfterInitialize);
            }

            // add id if it is not defined
            JToken ignored;
            if (!item.TryGetValue(MobileServiceSystemColumns.Id, StringComparison.OrdinalIgnoreCase, out ignored))
            {
                item[MobileServiceSystemColumns.Id] = String.Empty;
            }

            var tableDefinition = (from property in item.Properties()
                                   let storeType = SqlHelpers.GetStoreType(property.Value.Type, allowNull: false)
                                   select new ColumnDefinition(property.Name, property.Value.Type, storeType))
                                  .ToDictionary(p => p.Name, StringComparer.OrdinalIgnoreCase);

            var sysProperties = GetSystemProperties(item);

            this.tableMap.Add(tableName, new TableDefinition(tableDefinition, sysProperties));
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
        /// <param name="ignoreMissingColumns"><code>true</code> if the extra properties on item can be ignored; <code>false</code> otherwise.</param>
        /// <returns>A task that completes when item has been upserted in local table.</returns>
        public override Task UpsertAsync(string tableName, IEnumerable<JObject> items, bool ignoreMissingColumns)
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

            return UpsertAsyncInternal(tableName, items, ignoreMissingColumns);
        }

        /// <summary>
        /// Bind parameter.
        /// </summary>
        /// <param name="isqLite3Api">Api to use.</param>
        /// <param name="stmt">Statement to use.</param>
        /// <param name="index">Index to use.</param>
        /// <param name="value">Value to use.</param>
        /// <param name="storeDateTimeAsTicks">Format for date.</param>
        /// <param name="serializer">Serializer.</param>
        internal static void BindParameter(ISQLiteApi isqLite3Api, IDbStatement stmt, int index, object value, bool storeDateTimeAsTicks,
            IBlobSerializer serializer)
        {
            if (value == null)
            {
                isqLite3Api.BindNull(stmt, index);
            }
            else
            {
                if (value is int)
                {
                    isqLite3Api.BindInt(stmt, index, (int)value);
                }
                else if (value is ISerializable<int>)
                {
                    isqLite3Api.BindInt(stmt, index, ((ISerializable<int>)value).Serialize());
                }
                else if (value is string)
                {
                    isqLite3Api.BindText16(stmt, index, (string)value, -1, NegativePointer);
                }
                else if (value is ISerializable<string>)
                {
                    isqLite3Api.BindText16(stmt, index, ((ISerializable<string>)value).Serialize(), -1, NegativePointer);
                }
                else if (value is byte || value is ushort || value is sbyte || value is short)
                {
                    isqLite3Api.BindInt(stmt, index, Convert.ToInt32(value));
                }
                else if (value is ISerializable<byte>)
                {
                    isqLite3Api.BindInt(stmt, index, Convert.ToInt32(((ISerializable<byte>)value).Serialize()));
                }
                else if (value is ISerializable<ushort>)
                {
                    isqLite3Api.BindInt(stmt, index, Convert.ToInt32(((ISerializable<ushort>)value).Serialize()));
                }
                else if (value is ISerializable<sbyte>)
                {
                    isqLite3Api.BindInt(stmt, index, Convert.ToInt32(((ISerializable<sbyte>)value).Serialize()));
                }
                else if (value is ISerializable<short>)
                {
                    isqLite3Api.BindInt(stmt, index, Convert.ToInt32(((ISerializable<short>)value).Serialize()));
                }
                else if (value is bool)
                {
                    isqLite3Api.BindInt(stmt, index, (bool)value ? 1 : 0);
                }
                else if (value is ISerializable<bool>)
                {
                    isqLite3Api.BindInt(stmt, index, ((ISerializable<bool>)value).Serialize() ? 1 : 0);
                }
                else if (value is uint || value is long)
                {
                    isqLite3Api.BindInt64(stmt, index, Convert.ToInt64(value));
                }
                else if (value is ISerializable<uint>)
                {
                    isqLite3Api.BindInt64(stmt, index, Convert.ToInt64(((ISerializable<uint>)value).Serialize()));
                }
                else if (value is ISerializable<long>)
                {
                    isqLite3Api.BindInt64(stmt, index, Convert.ToInt64(((ISerializable<long>)value).Serialize()));
                }
                else if (value is float || value is double || value is decimal)
                {
                    isqLite3Api.BindDouble(stmt, index, Convert.ToDouble(value));
                }
                else if (value is ISerializable<float>)
                {
                    isqLite3Api.BindDouble(stmt, index, Convert.ToDouble(((ISerializable<float>)value).Serialize()));
                }
                else if (value is ISerializable<double>)
                {
                    isqLite3Api.BindDouble(stmt, index, Convert.ToDouble(((ISerializable<double>)value).Serialize()));
                }
                else if (value is ISerializable<decimal>)
                {
                    isqLite3Api.BindDouble(stmt, index, Convert.ToDouble(((ISerializable<decimal>)value).Serialize()));
                }
                else if (value is TimeSpan)
                {
                    isqLite3Api.BindInt64(stmt, index, ((TimeSpan)value).Ticks);
                }
                else if (value is ISerializable<TimeSpan>)
                {
                    isqLite3Api.BindInt64(stmt, index, ((ISerializable<TimeSpan>)value).Serialize().Ticks);
                }
                else if (value is DateTime)
                {
                    if (storeDateTimeAsTicks)
                    {
                        long ticks = ((DateTime)value).ToUniversalTime().Ticks;
                        isqLite3Api.BindInt64(stmt, index, ticks);
                    }
                    else
                    {
                        string val = ((DateTime)value).ToUniversalTime().ToString(DateTimeFormat, CultureInfo.InvariantCulture);
                        isqLite3Api.BindText16(stmt, index, val, -1, NegativePointer);
                    }
                }
                else if (value is DateTimeOffset)
                {
                    isqLite3Api.BindInt64(stmt, index, ((DateTimeOffset)value).UtcTicks);
                }
                else if (value is ISerializable<DateTime>)
                {
                    if (storeDateTimeAsTicks)
                    {
                        long ticks = ((ISerializable<DateTime>)value).Serialize().ToUniversalTime().Ticks;
                        isqLite3Api.BindInt64(stmt, index, ticks);
                    }
                    else
                    {
                        string val = ((ISerializable<DateTime>)value).Serialize().ToUniversalTime().ToString(DateTimeFormat, CultureInfo.InvariantCulture);
                        isqLite3Api.BindText16(stmt, index, val, -1, NegativePointer);
                    }
                }
                else if (value.GetType().GetTypeInfo().IsEnum)
                {
                    isqLite3Api.BindInt(stmt, index, Convert.ToInt32(value));
                }
                else if (value is byte[])
                {
                    isqLite3Api.BindBlob(stmt, index, (byte[])value, ((byte[])value).Length, NegativePointer);
                }
                else if (value is ISerializable<byte[]>)
                {
                    isqLite3Api.BindBlob(stmt, index, ((ISerializable<byte[]>)value).Serialize(), ((ISerializable<byte[]>)value).Serialize().Length,
                        NegativePointer);
                }
                else if (value is Guid)
                {
                    isqLite3Api.BindText16(stmt, index, ((Guid)value).ToString(), 72, NegativePointer);
                }
                else if (value is ISerializable<Guid>)
                {
                    isqLite3Api.BindText16(stmt, index, ((ISerializable<Guid>)value).Serialize().ToString(), 72, NegativePointer);
                }
                else if (serializer != null && serializer.CanDeserialize(value.GetType()))
                {
                    var bytes = serializer.Serialize(value);
                    isqLite3Api.BindBlob(stmt, index, bytes, bytes.Length, NegativePointer);
                }
                else
                {
                    throw new NotSupportedException("Cannot store type: " + value.GetType());
                }
            }
        }

        internal virtual void CreateTableFromObject(string tableName, IEnumerable<ColumnDefinition> columns)
        {
            ColumnDefinition idColumn = columns.FirstOrDefault(c => c.Name.Equals(MobileServiceSystemColumns.Id));
            var colDefinitions = columns.Where(c => c != idColumn).Select(c => String.Format("{0} {1}", SqlHelpers.FormatMember(c.Name), c.StoreType)).ToList();
            if (idColumn != null)
            {
                colDefinitions.Insert(0, String.Format("{0} {1} PRIMARY KEY", SqlHelpers.FormatMember(idColumn.Name), idColumn.StoreType));
            }

            String tblSql = string.Format("CREATE TABLE IF NOT EXISTS {0} ({1})", SqlHelpers.FormatTableName(tableName), String.Join(", ", colDefinitions));
            this.ExecuteNonQuery(tblSql, parameters: null);

            string infoSql = string.Format("PRAGMA table_info({0});", SqlHelpers.FormatTableName(tableName));
            IDictionary<string, JObject> existingColumns = this.ExecuteQuery((TableDefinition)null, infoSql, parameters: null)
                                                               .ToDictionary(c => c.Value<string>("name"), StringComparer.OrdinalIgnoreCase);

            // new columns that do not exist in existing columns
            var columnsToCreate = columns.Where(c => !existingColumns.ContainsKey(c.Name));

            foreach (ColumnDefinition column in columnsToCreate)
            {
                string createSql = string.Format("ALTER TABLE {0} ADD COLUMN {1} {2}",
                                                 SqlHelpers.FormatTableName(tableName),
                                                 SqlHelpers.FormatMember(column.Name),
                                                 column.StoreType);
                this.ExecuteNonQuery(createSql, parameters: null);
            }

            // NOTE: In SQLite you cannot drop columns, only add them.
        }

        internal virtual async Task SaveSetting(string name, string value)
        {
            var setting = new JObject()
            {
                { "id", name },
                { "value", value }
            };
            await this.UpsertAsyncInternal(MobileServiceLocalSystemTables.Config, new[] { setting }, ignoreMissingColumns: false);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                this.connection.Dispose();
            }
        }

        /// <summary>
        /// Executes a sql statement on a given table in local SQLite database.
        /// </summary>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="parameters">The query parameters.</param>
        protected virtual void ExecuteNonQuery(string sql, IDictionary<string, object> parameters)
        {
            try
            {
                parameters = parameters ?? new Dictionary<string, object>();

                var command = connection.CreateCommand(sql);

                foreach (KeyValuePair<string, object> parameter in parameters)
                {
                    command.Bind(parameter.Key, parameter.Value);
                }

                int result = command.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                // throw new SQLiteException(string.Format(Properties.Resources.SQLiteStore_QueryExecutionFailed, "fail"), ex);
            }

            // throw new SQLiteException(string.Format(Properties.Resources.SQLiteStore_QueryExecutionFailed, result));
        }

        /// <summary>
        /// Executes a sql statement on a given table in local SQLite database.
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

        protected override async Task OnInitialize()
        {
            this.CreateAllTables();
            await this.InitializeConfig();
        }

        private static string AddParameter(JObject item, Dictionary<string, object> parameters, ColumnDefinition column)
        {
            JToken rawValue = item.GetValue(column.Name, StringComparison.OrdinalIgnoreCase);
            object value = SqlHelpers.SerializeValue(rawValue, column.StoreType, column.JsonType);
            string paramName = CreateParameter(parameters, value);
            return paramName;
        }

        private static void AppendInsertValuesSql(StringBuilder sql, Dictionary<string, object> parameters, List<ColumnDefinition> columns, JObject item)
        {
            sql.Append("(");
            int colCount = 0;
            foreach (var column in columns)
            {
                if (colCount > 0)
                    sql.Append(",");

                sql.Append(AddParameter(item, parameters, column));

                colCount++;
            }
            sql.Append(")");
        }

        private static string CreateParameter(Dictionary<string, object> parameters, object value)
        {
            string paramName = "@p" + parameters.Count;
            parameters[paramName] = value;
            return paramName;
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

        /// <summary>
        /// Read column.
        /// </summary>
        /// <param name="connection">Connection to use.</param>
        /// <param name="stmt">Statement to use.</param>
        /// <param name="index">Index to use.</param>
        /// <param name="type">Type to use.</param>
        /// <param name="clrType">Type to use.</param>
        /// <returns>Object read.</returns>
        private static object ReadCol(SQLiteConnection connection, IDbStatement stmt, int index, ColType type, Type clrType)
        {
            var interfaces = clrType.GetTypeInfo().ImplementedInterfaces.ToList();

            if (type == ColType.Null)
            {
                return null;
            }
            if (clrType == typeof(string))
            {
                return connection.Platform.SQLiteApi.ColumnText16(stmt, index);
            }
            if (interfaces.Contains(typeof(ISerializable<string>)))
            {
                var value = connection.Platform.SQLiteApi.ColumnText16(stmt, index);
                return connection.Resolver.CreateObject(clrType, new object[] { value });
            }
            if (clrType == typeof(int))
            {
                return connection.Platform.SQLiteApi.ColumnInt(stmt, index);
            }
            if (interfaces.Contains(typeof(ISerializable<int>)))
            {
                var value = connection.Platform.SQLiteApi.ColumnInt(stmt, index);
                return connection.Resolver.CreateObject(clrType, new object[] { value });
            }
            if (clrType == typeof(bool))
            {
                return connection.Platform.SQLiteApi.ColumnInt(stmt, index) == 1;
            }
            if (interfaces.Contains(typeof(ISerializable<bool>)))
            {
                var value = connection.Platform.SQLiteApi.ColumnInt(stmt, index) == 1;
                return connection.Resolver.CreateObject(clrType, new object[] { value });
            }
            if (clrType == typeof(double))
            {
                return connection.Platform.SQLiteApi.ColumnDouble(stmt, index);
            }
            if (interfaces.Contains(typeof(ISerializable<double>)))
            {
                var value = connection.Platform.SQLiteApi.ColumnDouble(stmt, index);
                return connection.Resolver.CreateObject(clrType, new object[] { value });
            }
            if (clrType == typeof(float))
            {
                return (float)connection.Platform.SQLiteApi.ColumnDouble(stmt, index);
            }
            if (interfaces.Contains(typeof(ISerializable<float>)))
            {
                var value = (float)connection.Platform.SQLiteApi.ColumnDouble(stmt, index);
                return connection.Resolver.CreateObject(clrType, new object[] { value });
            }
            if (clrType == typeof(TimeSpan))
            {
                return new TimeSpan(connection.Platform.SQLiteApi.ColumnInt64(stmt, index));
            }
            if (interfaces.Contains(typeof(ISerializable<TimeSpan>)))
            {
                var value = new TimeSpan(connection.Platform.SQLiteApi.ColumnInt64(stmt, index));
                return connection.Resolver.CreateObject(clrType, new object[] { value });
            }
            if (clrType == typeof(DateTime))
            {
                if (connection.StoreDateTimeAsTicks)
                {
                    return new DateTime(connection.Platform.SQLiteApi.ColumnInt64(stmt, index), DateTimeKind.Utc);
                }
                return DateTime.Parse(connection.Platform.SQLiteApi.ColumnText16(stmt, index), CultureInfo.InvariantCulture);
            }
            if (clrType == typeof(DateTimeOffset))
            {
                return new DateTimeOffset(connection.Platform.SQLiteApi.ColumnInt64(stmt, index), TimeSpan.Zero);
            }
            if (interfaces.Contains(typeof(ISerializable<DateTime>)))
            {
                DateTime value;
                if (connection.StoreDateTimeAsTicks)
                {
                    value = new DateTime(connection.Platform.SQLiteApi.ColumnInt64(stmt, index), DateTimeKind.Utc);
                }
                else
                {
                    value = DateTime.Parse(connection.Platform.SQLiteApi.ColumnText16(stmt, index), CultureInfo.InvariantCulture);
                }
                return Activator.CreateInstance(clrType, value);
            }
            if (clrType.GetTypeInfo().IsEnum)
            {
                return connection.Platform.SQLiteApi.ColumnInt(stmt, index);
            }
            if (clrType == typeof(long))
            {
                return connection.Platform.SQLiteApi.ColumnInt64(stmt, index);
            }
            if (interfaces.Contains(typeof(ISerializable<long>)))
            {
                var value = connection.Platform.SQLiteApi.ColumnInt64(stmt, index);
                return connection.Resolver.CreateObject(clrType, new object[] { value });
            }
            if (clrType == typeof(uint))
            {
                return (uint)connection.Platform.SQLiteApi.ColumnInt64(stmt, index);
            }
            if (interfaces.Contains(typeof(ISerializable<long>)))
            {
                var value = (uint)connection.Platform.SQLiteApi.ColumnInt64(stmt, index);
                return connection.Resolver.CreateObject(clrType, new object[] { value });
            }
            if (clrType == typeof(decimal))
            {
                return (decimal)connection.Platform.SQLiteApi.ColumnDouble(stmt, index);
            }
            if (interfaces.Contains(typeof(ISerializable<decimal>)))
            {
                var value = (decimal)connection.Platform.SQLiteApi.ColumnDouble(stmt, index);
                return connection.Resolver.CreateObject(clrType, new object[] { value });
            }
            if (clrType == typeof(byte))
            {
                return (byte)connection.Platform.SQLiteApi.ColumnInt(stmt, index);
            }
            if (interfaces.Contains(typeof(ISerializable<byte>)))
            {
                var value = (byte)connection.Platform.SQLiteApi.ColumnInt(stmt, index);
                return connection.Resolver.CreateObject(clrType, new object[] { value });
            }
            if (clrType == typeof(ushort))
            {
                return (ushort)connection.Platform.SQLiteApi.ColumnInt(stmt, index);
            }
            if (interfaces.Contains(typeof(ISerializable<ushort>)))
            {
                var value = (ushort)connection.Platform.SQLiteApi.ColumnInt(stmt, index);
                return connection.Resolver.CreateObject(clrType, new object[] { value });
            }
            if (clrType == typeof(short))
            {
                return (short)connection.Platform.SQLiteApi.ColumnInt(stmt, index);
            }
            if (interfaces.Contains(typeof(ISerializable<short>)))
            {
                var value = (short)connection.Platform.SQLiteApi.ColumnInt(stmt, index);
                return connection.Resolver.CreateObject(clrType, new object[] { value });
            }
            if (clrType == typeof(sbyte))
            {
                return (sbyte)connection.Platform.SQLiteApi.ColumnInt(stmt, index);
            }
            if (interfaces.Contains(typeof(ISerializable<sbyte>)))
            {
                var value = (sbyte)connection.Platform.SQLiteApi.ColumnInt(stmt, index);
                return connection.Resolver.CreateObject(clrType, new object[] { value });
            }
            if (clrType == typeof(byte[]))
            {
                return connection.Platform.SQLiteApi.ColumnByteArray(stmt, index);
            }
            if (interfaces.Contains(typeof(ISerializable<byte[]>)))
            {
                var value = connection.Platform.SQLiteApi.ColumnByteArray(stmt, index);
                return connection.Resolver.CreateObject(clrType, new object[] { value });
            }
            if (clrType == typeof(Guid))
            {
                return new Guid(connection.Platform.SQLiteApi.ColumnText16(stmt, index));
            }
            if (interfaces.Contains(typeof(ISerializable<Guid>)))
            {
                var value = new Guid(connection.Platform.SQLiteApi.ColumnText16(stmt, index));
                return connection.Resolver.CreateObject(clrType, new object[] { value });
            }
            if (connection.Serializer != null && connection.Serializer.CanDeserialize(clrType))
            {
                var bytes = connection.Platform.SQLiteApi.ColumnByteArray(stmt, index);
                return connection.Serializer.Deserialize(bytes, clrType);
            }
            throw new NotSupportedException("Don't know how to read " + clrType);
        }

        /// <summary>
        /// Read column.
        /// </summary>
        /// <param name="connection">Connection to use.</param>
        /// <param name="statement">Statement to use.</param>
        /// <param name="index">Index to use.</param>
        /// <returns>Object read.</returns>
        private static object ReadColumn(SQLiteConnection connection, IDbStatement statement, int index)
        {
            object result = null;

            var sqliteapi = connection.Platform.SQLiteApi;

            ColType type = (ColType)sqliteapi.ColumnType(statement, index);

            switch (type)
            {
                case ColType.Integer:
                    result = sqliteapi.ColumnInt64(statement, index);

                    break;

                case ColType.Float:
                    result = sqliteapi.ColumnDouble(statement, index);

                    break;

                case ColType.Text:
                    result = sqliteapi.ColumnText16(statement, index);

                    break;

                case ColType.Blob:
                    result = sqliteapi.ColumnBlob(statement, index);

                    break;

                case ColType.Null:
                    break;
            }

            return result;
        }

        private static JObject ReadRow(SQLiteConnection connection, TableDefinition table, IDbStatement statement)
        {
            var row = new JObject();

            var sqliteApi = connection.Platform.SQLiteApi;
            var totalColumns = sqliteApi.ColumnCount(statement);

            for (int i = 0; i < totalColumns; i++)
            {
                string name = sqliteApi.ColumnName16(statement, i);
                object value = ReadColumn(connection, statement, i);

                ColumnDefinition column;
                if (table.TryGetValue(name, out column))
                {
                    JToken jVal = SqlHelpers.DeserializeValue(value, column.StoreType, column.JsonType);
                    row[name] = jVal;
                }
                else
                {
                    row[name] = value == null ? null : JToken.FromObject(value);
                }
            }
            return row;
        }

        private static int ValidateParameterCount(int parametersCount)
        {
            int batchSize = MaxParametersPerQuery / parametersCount;
            if (batchSize == 0)
            {
                throw new InvalidOperationException(string.Format(Properties.Resources.SQLiteStore_TooManyColumns, MaxParametersPerQuery));
            }
            return batchSize;
        }

        private static void ValidateResult(Result result)
        {
            if (result != Result.Done)
            {
                throw new InvalidOperationException(string.Format(Properties.Resources.SQLiteStore_QueryExecutionFailed, result));
            }
        }

        private void BatchInsert(string tableName, IEnumerable<JObject> items, List<ColumnDefinition> columns)
        {
            if (columns.Count == 0) // we need to have some columns to insert the item
            {
                return;
            }

            // Generate the prepared insert statement
            string sqlBase = String.Format(
                "INSERT OR IGNORE INTO {0} ({1}) VALUES ",
                SqlHelpers.FormatTableName(tableName),
                String.Join(", ", columns.Select(c => c.Name).Select(SqlHelpers.FormatMember))
            );

            // Use int division to calculate how many times this record will fit into our parameter quota
            int batchSize = ValidateParameterCount(columns.Count);

            foreach (var batch in items.Split(maxLength: batchSize))
            {
                var sql = new StringBuilder(sqlBase);
                var parameters = new Dictionary<string, object>();

                foreach (JObject item in batch)
                {
                    AppendInsertValuesSql(sql, parameters, columns, item);
                    sql.Append(",");
                }

                if (parameters.Any())
                {
                    sql.Remove(sql.Length - 1, 1); // remove the trailing comma
                    this.ExecuteNonQuery(sql.ToString(), parameters);
                }
            }
        }

        private void BatchUpdate(string tableName, IEnumerable<JObject> items, List<ColumnDefinition> columns)
        {
            if (columns.Count <= 1)
            {
                return; // For update to work there has to be at least once column besides Id that needs to be updated
            }

            ValidateParameterCount(columns.Count);

            string sqlBase = String.Format("UPDATE {0} SET ", SqlHelpers.FormatTableName(tableName));

            foreach (JObject item in items)
            {
                var sql = new StringBuilder(sqlBase);
                var parameters = new Dictionary<string, object>();

                ColumnDefinition idColumn = columns.FirstOrDefault(c => c.Name.Equals(MobileServiceSystemColumns.Id));
                if (idColumn == null)
                {
                    continue;
                }

                foreach (var column in columns.Where(c => c != idColumn))
                {
                    string paramName = AddParameter(item, parameters, column);

                    sql.AppendFormat("{0} = {1}", SqlHelpers.FormatMember(column.Name), paramName);
                    sql.Append(",");
                }

                if (parameters.Any())
                {
                    sql.Remove(sql.Length - 1, 1); // remove the trailing comma
                }

                sql.AppendFormat(" WHERE {0} = {1}", SqlHelpers.FormatMember(MobileServiceSystemColumns.Id), AddParameter(item, parameters, idColumn));

                this.ExecuteNonQuery(sql.ToString(), parameters);
            }
        }

        private void CreateAllTables()
        {
            foreach (KeyValuePair<string, TableDefinition> table in this.tableMap)
            {
                this.CreateTableFromObject(table.Key, table.Value.Values);
            }
        }

        private IList<JObject> ExecuteQuery(TableDefinition table, string sql, IDictionary<string, object> parameters)
        {
            table = table ?? new TableDefinition();
            parameters = parameters ?? new Dictionary<string, object>();

            var rows = new List<JObject>();

            IDbStatement statement = null;

            try
            {
                statement = this.connection.Platform.SQLiteApi.Prepare2(connection.Handle, sql);

                foreach (KeyValuePair<string, object> parameter in parameters)
                {
                    var index = this.connection.Platform.SQLiteApi.BindParameterIndex(statement, parameter.Key);
                    BindParameter(this.connection.Platform.SQLiteApi, statement, index, parameter.Value, connection.StoreDateTimeAsTicks, connection.Serializer);
                }

                Result result;
                while ((result = this.connection.Platform.SQLiteApi.Step(statement)) == Result.Row)
                {
                    var row = ReadRow(this.connection, table, statement);
                    rows.Add(row);
                }

                ValidateResult(result);
            }
            finally
            {
                if (statement != null)
                {
                    this.connection.Platform.SQLiteApi.Finalize(statement);
                }
            }

            return rows;
        }

        private TableDefinition GetTable(string tableName)
        {
            TableDefinition table;
            if (!this.tableMap.TryGetValue(tableName, out table))
            {
                throw new InvalidOperationException(string.Format(Properties.Resources.SQLiteStore_TableNotDefined, tableName));
            }
            return table;
        }

        private async Task InitializeConfig()
        {
            foreach (KeyValuePair<string, TableDefinition> table in this.tableMap)
            {
                if (!MobileServiceLocalSystemTables.All.Contains(table.Key))
                {
                    // preserve system properties setting for non-system tables
                    string name = String.Format("systemProperties|{0}", table.Key);
                    string value = ((int)table.Value.SystemProperties).ToString();
                    await this.SaveSetting(name, value);
                }
            }
        }

        private Task UpsertAsyncInternal(string tableName, IEnumerable<JObject> items, bool ignoreMissingColumns)
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
                if (!table.TryGetValue(prop.Name, out column) && !ignoreMissingColumns)
                {
                    throw new InvalidOperationException(string.Format(Properties.Resources.SQLiteStore_ColumnNotDefined, prop.Name, tableName));
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

            this.ExecuteNonQuery("BEGIN TRANSACTION", null);

            BatchInsert(tableName, items, columns.Where(c => c.Name.Equals(MobileServiceSystemColumns.Id)).Take(1).ToList());
            BatchUpdate(tableName, items, columns);

            this.ExecuteNonQuery("COMMIT TRANSACTION", null);

            return Task.FromResult(0);
        }
    }
}