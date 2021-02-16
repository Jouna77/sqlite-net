using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO.Pipes;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using SQLBase;
using SQLBase.Sync;
using SQLBase.Sync.Enums;

namespace SQLServer
{
	/// <summary>
	/// An open connection to a SQLite database.
	/// </summary>
	[Preserve (AllMembers = true)]
	public partial class SQLServerConnection : IDisposable
	{
		readonly static Dictionary<string, TableMapping> _mappings = new Dictionary<string, TableMapping> ();
		private System.Diagnostics.Stopwatch _sw;
		private long _elapsedMilliseconds = 0;

		private int _transactionDepth = 0;
		private Random _rand = new Random ();




		/// <summary>
		/// Gets the database path used by this connection.
		/// </summary>
		public SqlConnectionStringBuilder ConnectionString { get; private set; }



		/// <summary>
		/// Whether Trace lines should be written that show the execution time of queries.
		/// </summary>
		public bool TimeExecution { get; set; }

		/// <summary>
		/// Whether to write queries to <see cref="Tracer"/> during execution.
		/// </summary>
		public bool Trace { get; set; }

		/// <summary>
		/// The delegate responsible for writing trace lines.
		/// </summary>
		/// <value>The tracer.</value>
		public Action<string> Tracer { get; set; }

		/// <summary>
		///// Whether to store DateTime properties as ticks (true) or strings (false).
		///// </summary>
		//public bool StoreDateTimeAsTicks { get; private set; }

		///// <summary>
		///// Whether to store TimeSpan properties as ticks (true) or strings (false).
		///// </summary>
		//public bool StoreTimeSpanAsTicks { get; private set; }

		///// <summary>
		///// The format to use when storing DateTime properties as strings. Ignored if StoreDateTimeAsTicks is true.
		///// </summary>
		///// <value>The date time string format.</value>
		//public string DateTimeStringFormat { get; private set; }

		///// <summary>
		///// The DateTimeStyles value to use when parsing a DateTime property string.
		///// </summary>
		///// <value>The date time style.</value>
		//internal System.Globalization.DateTimeStyles DateTimeStyle { get; private set; }

		public SqlConnection Connection { get; private set; }




		public string DataBaseName { get; private set; }
		public string Server { get; private set; }
		public string Port { get; private set; }
		public string User { get; private set; }
		public string Password { get; private set; }

		public SQLServerConnection (string DataBaseName, string Server, string Port = null, string User = null, string Password = null)
			: this (BuildConnectionString (DataBaseName, Server, Port, User, Password))
		{
			this.DataBaseName = DataBaseName;
			this.Server = Server;
			this.Port = Port;
			this.User = User;
			this.Password = Password;
		}

		private static SqlConnectionStringBuilder BuildConnectionString
			(string DataBaseName, string Server, string Port = null, string User = null, string Password = null)
		{
			StringBuilder ConnectionString = new StringBuilder ();
			ConnectionString.Append ("Data Source=TCP:")
				.Append (Server)
				.Append ((!string.IsNullOrEmpty (Port?.Trim ()) ? "," + Port : ""))//no puerto no lo pongo
				.Append (";Initial Catalog=")
				.Append (DataBaseName);
			if (string.IsNullOrEmpty (User?.Trim ()) && string.IsNullOrEmpty (Password?.Trim ()))//no user provided,default authentication 
			{
				ConnectionString.Append (";Integrated Security=True;");
			}
			else {
				ConnectionString.Append (";Integrated Security=False;Persist Security Info=True;User ID=")
					.Append (User)
					.Append (";Password=")
					.Append (Password).Append (";");
			}

			string[] args = ConnectionString.
				Replace (Environment.NewLine, "").
				Replace ('\n', ' ').
				Replace ('\r', ' ').ToString ().
				Split (';');

			return new SqlConnectionStringBuilder (string.Join (";" + Environment.NewLine, args));
		}

		/// <summary>
		/// Constructs a new SQLiteConnection and opens a SQLite database specified by databasePath.
		/// </summary>
		/// <param name="connectionString">
		/// Details on how to find and open the database.
		/// </param>
		public SQLServerConnection (SqlConnectionStringBuilder connectionString)
		{
			if (connectionString == null)
				throw new ArgumentNullException (nameof (connectionString));
			if (connectionString.ConnectionString == null)
				throw new InvalidOperationException ("ConnectionString must be specified");
			ConnectionString = connectionString;
			Tracer = line => Debug.WriteLine (line);
			Connection = new SqlConnection (ConnectionString.ConnectionString);
		}
		internal void RenewConnection ()
		{
			if (IsOpen) {
				Connection.Close ();
			}
			Connection?.Dispose ();
			Connection = new SqlConnection (ConnectionString.ConnectionString);
		}
		public void ChangeCatalog (string newcatalog)
		{
			this.ConnectionString.InitialCatalog = newcatalog;
			this.RenewConnection ();
		}

		/// <summary>
		/// Convert an input string to a quoted SQL string that can be safely used in queries.
		/// </summary>
		/// <returns>The quoted string.</returns>
		/// <param name="unsafeString">The unsafe string to quote.</param>
		static string Quote (string unsafeString)
		{
			if (unsafeString == null)
				return "NULL";
			var safe = unsafeString.Replace ("'", "''");
			return "'" + safe + "'";
		}

		/// <summary>
		/// Sets a busy handler to sleep the specified amount of time when a table is locked.
		/// The handler will sleep multiple times until a total time of <see cref="BusyTimeout"/> has accumulated.
		/// </summary>
		//public TimeSpan BusyTimeout {
		//	get { return _busyTimeout; }
		//	set {
		//		_busyTimeout = value;
		//		if (Handle != NullHandle) {
		//			SQLite3.BusyTimeout (Handle, (int)_busyTimeout.TotalMilliseconds);
		//		}
		//	}
		//}

		/// <summary>
		/// Returns the mappings from types to tables that the connection
		/// currently understands.
		/// </summary>
		public IEnumerable<TableMapping> TableMappings {
			get {
				lock (_mappings) {
					return new List<TableMapping> (_mappings.Values);
				}
			}
		}

		/// <summary>
		/// Retrieves the mapping that is automatically generated for the given type.
		/// </summary>
		/// <param name="type">
		/// The type whose mapping to the database is returned.
		/// </param>
		/// <param name="createFlags">
		/// Optional flags allowing implicit PK and indexes based on naming conventions
		/// </param>
		/// <returns>
		/// The mapping represents the schema of the columns of the database and contains
		/// methods to set and get properties of objects.
		/// </returns>
		public TableMapping GetMapping (Type type, CreateFlags createFlags = CreateFlags.None)//
		{
			TableMapping map;
			var key = type.FullName;
			lock (_mappings) {
				if (_mappings.TryGetValue (key, out map)) {
					map = new TableMapping (type, createFlags);
					_mappings[key] = map;
				}
				else {
					map = new TableMapping (type, createFlags);
					_mappings.Add (key, map);
				}
			}
			return map;
		}

		/// <summary>
		/// Retrieves the mapping that is automatically generated for the given type.
		/// </summary>
		/// <param name="createFlags">
		/// Optional flags allowing implicit PK and indexes based on naming conventions
		/// </param>
		/// <returns>
		/// The mapping represents the schema of the columns of the database and contains
		/// methods to set and get properties of objects.
		/// </returns>
		public TableMapping GetMapping<T> (CreateFlags createFlags = CreateFlags.None)
		{
			return GetMapping (typeof (T), createFlags);
		}

		internal struct IndexedColumn
		{
			public int Order;
			public string ColumnName;
		}

		internal struct IndexInfo
		{
			public string IndexName;
			public string TableName;
			public bool Unique;
			public List<IndexedColumn> Columns;
		}

		/// <summary>
		/// Executes a "drop table" on the database.  This is non-recoverable.
		/// </summary>
		public int DropTable<T> ()
		{
			return DropTable (GetMapping (typeof (T)));
		}

		/// <summary>
		/// Executes a "drop table" on the database.  This is non-recoverable.
		/// </summary>
		/// <param name="map">
		/// The TableMapping used to identify the table.
		/// </param>
		public int DropTable (TableMapping map)
		{
			var query = string.Format ("drop table if exists \"{0}\"", map.TableName);
			return Execute (query);
		}

		public SQLServerConnection CreateDatabase ()
		{
			try {
				if (!DatabaseExists (this.DataBaseName)) {
					this.ChangeCatalog ("master");
					Execute ($"CREATE DATABASE {this.DataBaseName}");
					this.ChangeCatalog (this.DataBaseName);
				}
			}
			catch (Exception e) {

				throw SqlServerException.New (SQLite3.Result.CannotOpen, e.Message);
			}

			return this;
		}

		private bool DatabaseExists (string DbName)
		{
			ChangeCatalog ("master");
			bool exists = Exists ("SELECT 1 WHERE  DB_ID(@DbName) IS NOT NULL", new SqlParameter ("DbName", DbName));
			ChangeCatalog (DbName);
			return exists;
		}


		public bool Exists (string sql, params SqlParameter[] parameters)
		{
			bool result = false;
			using (var reader = Read (sql, parameters)) {
				if (reader != null) {
					result = reader.Read ();
				}
			}
			return result;
		}
		private SqlDataReader Read (string sql, params SqlParameter[] parameters)
		{
			try {
				SqlCommand cmd = new SqlCommand (sql, Connection) { CommandType = CommandType.Text };
				if (parameters != null) {
					cmd.Parameters.AddRange (parameters);
				}
				cmd.Connection.Open ();
				return cmd.ExecuteReader ();
			}
			catch (Exception ex) {
				throw ex;
			}
		}

		/// <summary>
		/// Executes a "create table if not exists" on the database. It also
		/// creates any specified indexes on the columns of the table. It uses
		/// a schema automatically generated from the specified type. You can
		/// later access this schema by calling GetMapping.
		/// </summary>
		/// <returns>
		/// Whether the table was created or migrated.
		/// </returns>
		public CreateTableResult CreateTable<T> ()
		{
			return CreateTable (typeof (T));
		}

		/// <summary>
		/// Executes a "create table if not exists" on the database. It also
		/// creates any specified indexes on the columns of the table. It uses
		/// a schema automatically generated from the specified type. You can
		/// later access this schema by calling GetMapping.
		/// </summary>
		/// <param name="ty">Type to reflect to a database table.</param>
		/// <param name="createFlags">Optional flags allowing implicit PK and indexes based on naming conventions.</param>
		/// <returns>
		/// Whether the table was created or migrated.
		/// </returns>
		public CreateTableResult CreateTable (Type ty, CreateFlags createFlags = CreateFlags.None)
		{
			var map = GetMapping (ty, createFlags);

			// Present a nice error if no columns specified
			if (map.Columns.Length == 0) {
				throw new Exception (string.Format ("Cannot create a table without columns (does '{0}' have public properties?)", ty.FullName));
			}

			// Check if the table exists
			var result = CreateTableResult.Created;
			var existingCols = GetTableInfo (map.TableName);

			// Create or migrate it
			if (existingCols.Count == 0) {

				// Facilitate virtual tables a.k.a. full-text search.



				// Build query.
				var query = "create table \"" + map.TableName + "\" (\n";
				var decls = map.Columns.Select (p => Orm.SqlDecl (p));
				var decl = string.Join (",\n", decls.ToArray ());
				query += decl;
				query += ")";
				if (map.WithoutRowId) {
					query += " without rowid";
				}

				Execute (query);
			}
			else {
				result = CreateTableResult.Migrated;
				MigrateTable (map, existingCols);
			}

			return result;
		}

		/// <summary>
		/// Executes a "create table if not exists" on the database for each type. It also
		/// creates any specified indexes on the columns of the table. It uses
		/// a schema automatically generated from the specified type. You can
		/// later access this schema by calling GetMapping.
		/// </summary>
		/// <returns>
		/// Whether the table was created or migrated for each type.
		/// </returns>
		public CreateTablesResult CreateTables<T, T2> ()
			where T : new()
			where T2 : new()
		{
			return CreateTables (typeof (T), typeof (T2));
		}

		/// <summary>
		/// Executes a "create table if not exists" on the database for each type. It also
		/// creates any specified indexes on the columns of the table. It uses
		/// a schema automatically generated from the specified type. You can
		/// later access this schema by calling GetMapping.
		/// </summary>
		/// <returns>
		/// Whether the table was created or migrated for each type.
		/// </returns>
		public CreateTablesResult CreateTables<T, T2, T3> ()
			where T : new()
			where T2 : new()
			where T3 : new()
		{
			return CreateTables (typeof (T), typeof (T2), typeof (T3));
		}

		/// <summary>
		/// Executes a "create table if not exists" on the database for each type. It also
		/// creates any specified indexes on the columns of the table. It uses
		/// a schema automatically generated from the specified type. You can
		/// later access this schema by calling GetMapping.
		/// </summary>
		/// <returns>
		/// Whether the table was created or migrated for each type.
		/// </returns>
		public CreateTablesResult CreateTables<T, T2, T3, T4> ()
			where T : new()
			where T2 : new()
			where T3 : new()
			where T4 : new()
		{
			return CreateTables (typeof (T), typeof (T2), typeof (T3), typeof (T4));
		}

		/// <summary>
		/// Executes a "create table if not exists" on the database for each type. It also
		/// creates any specified indexes on the columns of the table. It uses
		/// a schema automatically generated from the specified type. You can
		/// later access this schema by calling GetMapping.
		/// </summary>
		/// <returns>
		/// Whether the table was created or migrated for each type.
		/// </returns>
		public CreateTablesResult CreateTables<T, T2, T3, T4, T5> ()
			where T : new()
			where T2 : new()
			where T3 : new()
			where T4 : new()
			where T5 : new()
		{
			return CreateTables (typeof (T), typeof (T2), typeof (T3), typeof (T4), typeof (T5));
		}

		/// <summary>
		/// Executes a "create table if not exists" on the database for each type. It also
		/// creates any specified indexes on the columns of the table. It uses
		/// a schema automatically generated from the specified type. You can
		/// later access this schema by calling GetMapping.
		/// </summary>
		/// <returns>
		/// Whether the table was created or migrated for each type.
		/// </returns>
		public CreateTablesResult CreateTables (params Type[] types)
		{
			var result = new CreateTablesResult ();
			foreach (Type type in types) {
				var aResult = CreateTable (type);
				result.Results[type] = aResult;
			}
			return result;
		}

		/// <summary>
		/// Creates an index for the specified table and columns.
		/// </summary>
		/// <param name="indexName">Name of the index to create</param>
		/// <param name="tableName">Name of the database table</param>
		/// <param name="columnNames">An array of column names to index</param>
		/// <param name="unique">Whether the index should be unique</param>
		/// <returns>Zero on success.</returns>
		public int CreateIndex (string indexName, string tableName, string[] columnNames, bool unique = false)
		{
			const string sqlFormat = "create {2} index if not exists \"{3}\" on \"{0}\"(\"{1}\")";
			var sql = String.Format (sqlFormat, tableName, string.Join ("\", \"", columnNames), unique ? "unique" : "", indexName);
			return Execute (sql);
		}

		/// <summary>
		/// Creates an index for the specified table and column.
		/// </summary>
		/// <param name="indexName">Name of the index to create</param>
		/// <param name="tableName">Name of the database table</param>
		/// <param name="columnName">Name of the column to index</param>
		/// <param name="unique">Whether the index should be unique</param>
		/// <returns>Zero on success.</returns>
		public int CreateIndex (string indexName, string tableName, string columnName, bool unique = false)
		{
			return CreateIndex (indexName, tableName, new string[] { columnName }, unique);
		}

		/// <summary>
		/// Creates an index for the specified table and column.
		/// </summary>
		/// <param name="tableName">Name of the database table</param>
		/// <param name="columnName">Name of the column to index</param>
		/// <param name="unique">Whether the index should be unique</param>
		/// <returns>Zero on success.</returns>
		public int CreateIndex (string tableName, string columnName, bool unique = false)
		{
			return CreateIndex (tableName + "_" + columnName, tableName, columnName, unique);
		}

		/// <summary>
		/// Creates an index for the specified table and columns.
		/// </summary>
		/// <param name="tableName">Name of the database table</param>
		/// <param name="columnNames">An array of column names to index</param>
		/// <param name="unique">Whether the index should be unique</param>
		/// <returns>Zero on success.</returns>
		public int CreateIndex (string tableName, string[] columnNames, bool unique = false)
		{
			return CreateIndex (tableName + "_" + string.Join ("_", columnNames), tableName, columnNames, unique);
		}

		/// <summary>
		/// Creates an index for the specified object property.
		/// e.g. CreateIndex&lt;Client&gt;(c => c.Name);
		/// </summary>
		/// <typeparam name="T">Type to reflect to a database table.</typeparam>
		/// <param name="property">Property to index</param>
		/// <param name="unique">Whether the index should be unique</param>
		/// <returns>Zero on success.</returns>
		public int CreateIndex<T> (Expression<Func<T, object>> property, bool unique = false)
		{
			MemberExpression mx;
			if (property.Body.NodeType == ExpressionType.Convert) {
				mx = ((UnaryExpression)property.Body).Operand as MemberExpression;
			}
			else {
				mx = (property.Body as MemberExpression);
			}
			var propertyInfo = mx.Member as PropertyInfo;
			if (propertyInfo == null) {
				throw new ArgumentException ("The lambda expression 'property' should point to a valid Property");
			}

			var propName = propertyInfo.Name;

			var map = GetMapping<T> ();
			var colName = map.FindColumnWithPropertyName (propName).Name;

			return CreateIndex (map.TableName, colName, unique);
		}

		[Preserve (AllMembers = true)]
		public class ColumnInfo
		{
			//			public int cid { get; set; }

			[Column ("name")]
			public string Name { get; set; }

			//			[Column ("type")]
			//			public string ColumnType { get; set; }

			public int notnull { get; set; }

			//			public string dflt_value { get; set; }

			//			public int pk { get; set; }

			public override string ToString ()
			{
				return Name;
			}
		}

		/// <summary>
		/// Query the built-in sqlite table_info table for a specific tables columns.
		/// </summary>
		/// <returns>The columns contains in the table.</returns>
		/// <param name="tableName">Table name.</param>
		public List<ColumnInfo> GetTableInfo (string tableName)
		{
			var query = @"SELECT
		c.ORDINAL_POSITION as cid,
	c.COLUMN_NAME as name,
	c.DATA_TYPE as type,
	IIF(c.IS_NULLABLE='YES',0,1) AS notnull,
	c.COLUMN_DEFAULT as dflt_value,
    CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS pk
FROM INFORMATION_SCHEMA.COLUMNS c
LEFT JOIN (
            SELECT ku.TABLE_CATALOG,ku.TABLE_SCHEMA,ku.TABLE_NAME,ku.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ku
                ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY' 
                AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
         )   pk 
ON  c.TABLE_CATALOG = pk.TABLE_CATALOG
            AND c.TABLE_SCHEMA = pk.TABLE_SCHEMA
            AND c.TABLE_NAME = pk.TABLE_NAME
            AND c.COLUMN_NAME = pk.COLUMN_NAME
WHERE
	c.TABLE_NAME = @tablename
	ORDER BY c.TABLE_SCHEMA,c.TABLE_NAME, c.ORDINAL_POSITION ";
			return Query<ColumnInfo> (query, new SqlParameter ("tablename", tableName));
		}

		void MigrateTable (TableMapping map, List<ColumnInfo> existingCols)
		{
			var toBeAdded = new List<TableMapping.Column> ();

			foreach (var p in map.Columns) {
				var found = false;
				foreach (var c in existingCols) {
					found = (string.Compare (p.Name, c.Name, StringComparison.OrdinalIgnoreCase) == 0);
					if (found)
						break;
				}
				if (!found) {
					toBeAdded.Add (p);
				}
			}

			foreach (var p in toBeAdded) {
				var addCol = "alter table \"" + map.TableName + "\" add column " + Orm.SqlDecl (p);
				Execute (addCol);
			}
		}

		/// <summary>
		/// Creates a new SQLiteCommand. Can be overridden to provide a sub-class.
		/// </summary>
		/// <seealso cref="SQLiteCommand.OnInstanceCreated"/>
		protected virtual SQLServerCommand NewCommand (string CommandText, params SqlParameter[] parameters)
		{
			return new SQLServerCommand (this, CommandText, parameters);
		}

		/// <summary>
		/// Creates a new SQLiteCommand given the command text with arguments. Place a '?'
		/// in the command text for each of the arguments.
		/// </summary>
		/// <param name="cmdText">
		/// The fully escaped SQL.
		/// </param>
		/// <param name="ps">
		/// Arguments to substitute for the occurences of '?' in the command text.
		/// </param>
		/// <returns>
		/// A <see cref="SQLiteCommand"/>
		/// </returns>
		public SQLServerCommand CreateCommand (string cmdText, params SqlParameter[] ps)
		{
			return NewCommand (cmdText, ps);
		}

		/// <summary>
		/// Creates a new SQLiteCommand given the command text with named arguments. Place a "[@:$]VVV"
		/// in the command text for each of the arguments. VVV represents an alphanumeric identifier.
		/// For example, @name :name and $name can all be used in the query.
		/// </summary>
		/// <param name="cmdText">
		/// The fully escaped SQL.
		/// </param>
		/// <param name="args">
		/// Arguments to substitute for the occurences of "[@:$]VVV" in the command text.
		/// </param>
		/// <returns>
		/// A <see cref="SQLiteCommand" />
		/// </returns>
		public SQLServerCommand CreateCommand (string cmdText, Dictionary<string, object> args)
		{
			if (!IsOpen)
				throw SqlServerException.New (SQLite3.Result.Error, "Cannot create commands from unopened database");

			var cmd = NewCommand (cmdText);
			foreach (var kv in args) {
				cmd.Parameters.Add (new SqlParameter (kv.Key, kv.Value));
			}
			return cmd;
		}

		/// <summary>
		/// WARNING: Changes made through this method will not be tracked on history.
		/// Creates a SQLiteCommand given the command text (SQL) with arguments. Place a '?'
		/// in the command text for each of the arguments and then executes that command.
		/// Use this method instead of Query when you don't expect rows back. Such cases include
		/// INSERTs, UPDATEs, and DELETEs.
		/// You can set the Trace or TimeExecution properties of the connection
		/// to profile execution.
		/// </summary>
		/// <param name="query">
		/// The fully escaped SQL.
		/// </param>
		/// <param name="args">
		/// Arguments to substitute for the occurences of '?' in the query.
		/// </param>
		/// <returns>
		/// The number of rows modified in the database as a result of this execution.
		/// </returns>
		public int Execute (string query, params SqlParameter[] args)
		{
			var cmd = CreateCommand (query, args);

			if (TimeExecution) {
				if (_sw == null) {
					_sw = new Stopwatch ();
				}
				_sw.Reset ();
				_sw.Start ();
			}

			var r = cmd.ExecuteNonQuery ();

			if (TimeExecution) {
				_sw.Stop ();
				_elapsedMilliseconds += _sw.ElapsedMilliseconds;
				Tracer?.Invoke (string.Format ("Finished in {0} ms ({1:0.0} s total)", _sw.ElapsedMilliseconds, _elapsedMilliseconds / 1000.0));
			}

			return r;
		}

		/// <summary>
		/// WARNING: Changes made through this method will not be tracked on history.
		/// Creates a SQLiteCommand given the command text (SQL) with arguments. Place a '?'
		/// in the command text for each of the arguments and then executes that command.
		/// Use this method when return primitive values.
		/// You can set the Trace or TimeExecution properties of the connection
		/// to profile execution.
		/// </summary>
		/// <param name="query">
		/// The fully escaped SQL.
		/// </param>
		/// <param name="args">
		/// Arguments to substitute for the occurences of '?' in the query.
		/// </param>
		/// <returns>
		/// The number of rows modified in the database as a result of this execution.
		/// </returns>
		public T ExecuteScalar<T> (string query, params SqlParameter[] args)
		{
			var cmd = CreateCommand (query, args);

			if (TimeExecution) {
				if (_sw == null) {
					_sw = new Stopwatch ();
				}
				_sw.Reset ();
				_sw.Start ();
			}

			var r = cmd.ExecuteScalar<T> ();

			if (TimeExecution) {
				_sw.Stop ();
				_elapsedMilliseconds += _sw.ElapsedMilliseconds;
				Tracer?.Invoke (string.Format ("Finished in {0} ms ({1:0.0} s total)", _sw.ElapsedMilliseconds, _elapsedMilliseconds / 1000.0));
			}

			return r;
		}

		/// <summary>
		/// WARNING: Changes made through this method will not be tracked on history.
		/// Creates a SQLiteCommand given the command text (SQL) with arguments. Place a '?'
		/// in the command text for each of the arguments and then executes that command.
		/// It returns each row of the result using the mapping automatically generated for
		/// the given type.
		/// </summary>
		/// <param name="query">
		/// The fully escaped SQL.
		/// </param>
		/// <param name="args">
		/// Arguments to substitute for the occurences of '?' in the query.
		/// </param>
		/// <returns>
		/// An enumerable with one result for each row returned by the query.
		/// </returns>
		public List<T> Query<T> (string query, params SqlParameter[] args) where T : new()
		{
			var cmd = CreateCommand (query, args);
			return cmd.ExecuteQuery<T> ();
		}

		/// <summary>
		/// WARNING: Changes made through this method will not be tracked on history.
		/// Creates a SQLiteCommand given the command text (SQL) with arguments. Place a '?'
		/// in the command text for each of the arguments and then executes that command.
		/// It returns the first column of each row of the result.
		/// </summary>
		/// <param name="query">
		/// The fully escaped SQL.
		/// </param>
		/// <param name="args">
		/// Arguments to substitute for the occurences of '?' in the query.
		/// </param>
		/// <returns>
		/// An enumerable with one result for the first column of each row returned by the query.
		/// </returns>
		public List<T> QueryScalars<T> (string query, params SqlParameter[] args)
		{
			var cmd = CreateCommand (query, args);
			return cmd.ExecuteQueryScalars<T> ().ToList ();
		}

		/// <summary>
		/// /// WARNING: Changes made through this method will not be tracked on history.
		/// Creates a SQLiteCommand given the command text (SQL) with arguments. Place a '?'
		/// in the command text for each of the arguments and then executes that command.
		/// It returns each row of the result using the mapping automatically generated for
		/// the given type.
		/// </summary>
		/// <param name="query">
		/// The fully escaped SQL.
		/// </param>
		/// <param name="args">
		/// Arguments to substitute for the occurences of '?' in the query.
		/// </param>
		/// <returns>
		/// An enumerable with one result for each row returned by the query.
		/// The enumerator (retrieved by calling GetEnumerator() on the result of this method)
		/// will call sqlite3_step on each call to MoveNext, so the database
		/// connection must remain open for the lifetime of the enumerator.
		/// </returns>
		public IEnumerable<T> DeferredQuery<T> (string query, params SqlParameter[] args) where T : new()
		{
			var cmd = CreateCommand (query, args);
			return cmd.ExecuteDeferredQuery<T> ();
		}

		/// <summary>
		/// WARNING: Changes made through this method will not be tracked on history.
		/// Creates a SQLiteCommand given the command text (SQL) with arguments. Place a '?'
		/// in the command text for each of the arguments and then executes that command.
		/// It returns each row of the result using the specified mapping. This function is
		/// only used by libraries in order to query the database via introspection. It is
		/// normally not used.
		/// </summary>
		/// <param name="map">
		/// A <see cref="TableMapping"/> to use to convert the resulting rows
		/// into objects.
		/// </param>
		/// <param name="query">
		/// The fully escaped SQL.
		/// </param>
		/// <param name="args">
		/// Arguments to substitute for the occurences of '?' in the query.
		/// </param>
		/// <returns>
		/// An enumerable with one result for each row returned by the query.
		/// </returns>
		public List<object> Query (TableMapping map, string query, params SqlParameter[] args)
		{
			var cmd = CreateCommand (query, args);
			return cmd.ExecuteQuery<object> (map);
		}

		/// <summary>
		/// WARNING: Changes made through this method will not be tracked on history.
		/// Creates a SQLiteCommand given the command text (SQL) with arguments. Place a '?'
		/// in the command text for each of the arguments and then executes that command.
		/// It returns each row of the result using the specified mapping. This function is
		/// only used by libraries in order to query the database via introspection. It is
		/// normally not used.
		/// </summary>
		/// <param name="map">
		/// A <see cref="TableMapping"/> to use to convert the resulting rows
		/// into objects.
		/// </param>
		/// <param name="query">
		/// The fully escaped SQL.
		/// </param>
		/// <param name="args">
		/// Arguments to substitute for the occurences of '?' in the query.
		/// </param>
		/// <returns>
		/// An enumerable with one result for each row returned by the query.
		/// The enumerator (retrieved by calling GetEnumerator() on the result of this method)
		/// will call sqlite3_step on each call to MoveNext, so the database
		/// connection must remain open for the lifetime of the enumerator.
		/// </returns>
		public IEnumerable<object> DeferredQuery (TableMapping map, string query, params SqlParameter[] args)
		{
			var cmd = CreateCommand (query, args);
			return cmd.ExecuteDeferredQuery<object> (map);
		}

		/// <summary>
		/// Returns a queryable interface to the table represented by the given type.
		/// </summary>
		/// <returns>
		/// A queryable object that is able to translate Where, OrderBy, and Take
		/// queries into native SQL.
		/// </returns>
		public TableQuery<T> Table<T> () where T : new()
		{
			return new TableQuery<T> (this);
		}

		/// <summary>
		/// Attempts to retrieve an object with the given primary key from the table
		/// associated with the specified type. Use of this method requires that
		/// the given type have a designated PrimaryKey (using the PrimaryKeyAttribute).
		/// </summary>
		/// <param name="pk">
		/// The primary key.
		/// </param>
		/// <returns>
		/// The object with the given primary key. Throws a not found exception
		/// if the object is not found.
		/// </returns>
		public T Get<T> (object pk) where T : new()
		{
			var map = GetMapping (typeof (T));
			return Query<T> (map.GetByPrimaryKeySql, new SqlParameter (map.PK.Name, pk)).First ();
		}

		/// <summary>
		/// Attempts to retrieve an object with the given primary key from the table
		/// associated with the specified type. Use of this method requires that
		/// the given type have a designated PrimaryKey (using the PrimaryKeyAttribute).
		/// </summary>
		/// <param name="pk">
		/// The primary key.
		/// </param>
		/// <param name="map">
		/// The TableMapping used to identify the table.
		/// </param>
		/// <returns>
		/// The object with the given primary key. Throws a not found exception
		/// if the object is not found.
		/// </returns>
		public object Get (object pk, TableMapping map)
		{
			return Query (map, map.GetByPrimaryKeySql, new SqlParameter (map.PK.Name, pk)).First ();
		}

		/// <summary>
		/// Attempts to retrieve the first object that matches the predicate from the table
		/// associated with the specified type.
		/// </summary>
		/// <param name="predicate">
		/// A predicate for which object to find.
		/// </param>
		/// <returns>
		/// The object that matches the given predicate. Throws a not found exception
		/// if the object is not found.
		/// </returns>
		public T Get<T> (Expression<Func<T, bool>> predicate) where T : new()
		{
			return Table<T> ().Where (predicate).First ();
		}

		/// <summary>
		/// Attempts to retrieve an object with the given primary key from the table
		/// associated with the specified type. Use of this method requires that
		/// the given type have a designated PrimaryKey (using the PrimaryKeyAttribute).
		/// </summary>
		/// <param name="pk">
		/// The primary key.
		/// </param>
		/// <returns>
		/// The object with the given primary key or null
		/// if the object is not found.
		/// </returns>
		public T Find<T> (object pk) where T : new()
		{
			var map = GetMapping (typeof (T));
			return Query<T> (map.GetByPrimaryKeySql, new SqlParameter (map.PK.Name, pk)).FirstOrDefault ();
		}

		/// <summary>
		/// Attempts to retrieve an object with the given primary key from the table
		/// associated with the specified type. Use of this method requires that
		/// the given type have a designated PrimaryKey (using the PrimaryKeyAttribute).
		/// </summary>
		/// <param name="pk">
		/// The primary key.
		/// </param>
		/// <param name="map">
		/// The TableMapping used to identify the table.
		/// </param>
		/// <returns>
		/// The object with the given primary key or null
		/// if the object is not found.
		/// </returns>
		public object Find (object pk, TableMapping map)
		{
			return Query (map, map.GetByPrimaryKeySql,
				new SqlParameter (map.PK.Name, pk)).FirstOrDefault ();
		}

		/// <summary>
		/// Attempts to retrieve the first object that matches the predicate from the table
		/// associated with the specified type.
		/// </summary>
		/// <param name="predicate">
		/// A predicate for which object to find.
		/// </param>
		/// <returns>
		/// The object that matches the given predicate or null
		/// if the object is not found.
		/// </returns>
		public T Find<T> (Expression<Func<T, bool>> predicate) where T : new()
		{
			return Table<T> ().Where (predicate).FirstOrDefault ();
		}

		/// <summary>
		/// Attempts to retrieve the first object that matches the query from the table
		/// associated with the specified type.
		/// </summary>
		/// <param name="query">
		/// The fully escaped SQL.
		/// </param>
		/// <param name="args">
		/// Arguments to substitute for the occurences of '?' in the query.
		/// </param>
		/// <returns>
		/// The object that matches the given predicate or null
		/// if the object is not found.
		/// </returns>
		public T FindWithQuery<T> (string query, params SqlParameter[] args) where T : new()
		{
			return Query<T> (query, args).FirstOrDefault ();
		}

		/// <summary>
		/// Attempts to retrieve the first object that matches the query from the table
		/// associated with the specified type.
		/// </summary>
		/// <param name="map">
		/// The TableMapping used to identify the table.
		/// </param>
		/// <param name="query">
		/// The fully escaped SQL.
		/// </param>
		/// <param name="args">
		/// Arguments to substitute for the occurences of '?' in the query.
		/// </param>
		/// <returns>
		/// The object that matches the given predicate or null
		/// if the object is not found.
		/// </returns>
		public object FindWithQuery (TableMapping map, string query, params SqlParameter[] args)
		{
			return Query (map, query, args).FirstOrDefault ();
		}

		/// <summary>
		/// Whether <see cref="BeginTransaction"/> has been called and the database is waiting for a <see cref="Commit"/>.
		/// </summary>
		public bool IsInTransaction {
			get { return _transactionDepth > 0; }
		}

		/// <summary>
		/// Whether <see cref="SQLServerConnection"/> has been disposed and the database is closed.
		/// </summary>
		public bool IsClosed {
			get {
				bool closed = (Connection.State == ConnectionState.Closed || Connection.State == ConnectionState.Broken);
				return closed;
			}
		}

		public bool IsOpen => !IsClosed;

		/// <summary>
		/// Begins a new transaction. Call <see cref="Commit"/> to end the transaction.
		/// </summary>
		/// <example cref="System.InvalidOperationException">Throws if a transaction has already begun.</example>
		public void BeginTransaction ()
		{
			// The BEGIN command only works if the transaction stack is empty,
			//    or in other words if there are no pending transactions.
			// If the transaction stack is not empty when the BEGIN command is invoked,
			//    then the command fails with an error.
			// Rather than crash with an error, we will just ignore calls to BeginTransaction
			//    that would result in an error.
			if (Interlocked.CompareExchange (ref _transactionDepth, 1, 0) == 0) {
				try {
					Execute ("begin transaction");
				}
				catch (Exception ex) {
					var sqlExp = ex as SqlServerException;
					if (sqlExp != null) {
						// It is recommended that applications respond to the errors listed below
						//    by explicitly issuing a ROLLBACK command.
						// TODO: This rollback failsafe should be localized to all throw sites.
						switch (sqlExp.Result) {
							case SQLite3.Result.IOError:
							case SQLite3.Result.Full:
							case SQLite3.Result.Busy:
							case SQLite3.Result.NoMem:
							case SQLite3.Result.Interrupt:
								RollbackTo (null, true);
								break;
						}
					}
					else {
						// Call decrement and not VolatileWrite in case we've already
						//    created a transaction point in SaveTransactionPoint since the catch.
						Interlocked.Decrement (ref _transactionDepth);
					}

					throw;
				}
			}
			else {
				// Calling BeginTransaction on an already open transaction is invalid
				throw new InvalidOperationException ("Cannot begin a transaction while already in a transaction.");
			}
		}

		/// <summary>
		/// Creates a savepoint in the database at the current point in the transaction timeline.
		/// Begins a new transaction if one is not in progress.
		///
		/// Call <see cref="RollbackTo(string)"/> to undo transactions since the returned savepoint.
		/// Call <see cref="Release"/> to commit transactions after the savepoint returned here.
		/// Call <see cref="Commit"/> to end the transaction, committing all changes.
		/// </summary>
		/// <returns>A string naming the savepoint.</returns>
		public string SaveTransactionPoint ()
		{
			int depth = Interlocked.Increment (ref _transactionDepth) - 1;
			string retVal = "S" + _rand.Next (short.MaxValue) + "D" + depth;

			try {
				Execute ("savepoint " + retVal);
			}
			catch (Exception ex) {
				var sqlExp = ex as SqlServerException;
				if (sqlExp != null) {
					// It is recommended that applications respond to the errors listed below
					//    by explicitly issuing a ROLLBACK command.
					// TODO: This rollback failsafe should be localized to all throw sites.
					switch (sqlExp.Result) {
						case SQLite3.Result.IOError:
						case SQLite3.Result.Full:
						case SQLite3.Result.Busy:
						case SQLite3.Result.NoMem:
						case SQLite3.Result.Interrupt:
							RollbackTo (null, true);
							break;
					}
				}
				else {
					Interlocked.Decrement (ref _transactionDepth);
				}

				throw;
			}

			return retVal;
		}

		/// <summary>
		/// Rolls back the transaction that was begun by <see cref="BeginTransaction"/> or <see cref="SaveTransactionPoint"/>.
		/// </summary>
		public void Rollback ()
		{
			RollbackTo (null, false);
		}

		/// <summary>
		/// Rolls back the savepoint created by <see cref="BeginTransaction"/> or SaveTransactionPoint.
		/// </summary>
		/// <param name="savepoint">The name of the savepoint to roll back to, as returned by <see cref="SaveTransactionPoint"/>.  If savepoint is null or empty, this method is equivalent to a call to <see cref="Rollback"/></param>
		public void RollbackTo (string savepoint)
		{
			RollbackTo (savepoint, false);
		}

		/// <summary>
		/// Rolls back the transaction that was begun by <see cref="BeginTransaction"/>.
		/// </summary>
		/// <param name="savepoint">The name of the savepoint to roll back to, as returned by <see cref="SaveTransactionPoint"/>.  If savepoint is null or empty, this method is equivalent to a call to <see cref="Rollback"/></param>
		/// <param name="noThrow">true to avoid throwing exceptions, false otherwise</param>
		void RollbackTo (string savepoint, bool noThrow)
		{
			// Rolling back without a TO clause rolls backs all transactions
			//    and leaves the transaction stack empty.
			try {
				if (String.IsNullOrEmpty (savepoint)) {
					if (Interlocked.Exchange (ref _transactionDepth, 0) > 0) {
						Execute ("rollback");
					}
				}
				else {
					DoSavePointExecute (savepoint, "rollback to ");
				}
			}
			catch (SqlServerException) {
				if (!noThrow)
					throw;

			}
			// No need to rollback if there are no transactions open.
		}

		/// <summary>
		/// Releases a savepoint returned from <see cref="SaveTransactionPoint"/>.  Releasing a savepoint
		///    makes changes since that savepoint permanent if the savepoint began the transaction,
		///    or otherwise the changes are permanent pending a call to <see cref="Commit"/>.
		///
		/// The RELEASE command is like a COMMIT for a SAVEPOINT.
		/// </summary>
		/// <param name="savepoint">The name of the savepoint to release.  The string should be the result of a call to <see cref="SaveTransactionPoint"/></param>
		public void Release (string savepoint)
		{
			try {
				DoSavePointExecute (savepoint, "release ");
			}
			catch (SqlServerException ex) {
				if (ex.Result == SQLite3.Result.Busy) {
					// Force a rollback since most people don't know this function can fail
					// Don't call Rollback() since the _transactionDepth is 0 and it won't try
					// Calling rollback makes our _transactionDepth variable correct.
					// Writes to the database only happen at depth=0, so this failure will only happen then.
					try {
						Execute ("rollback");
					}
					catch {
						// rollback can fail in all sorts of wonderful version-dependent ways. Let's just hope for the best
					}
				}
				throw;
			}
		}

		void DoSavePointExecute (string savepoint, string cmd)
		{
			// Validate the savepoint
			int firstLen = savepoint.IndexOf ('D');
			if (firstLen >= 2 && savepoint.Length > firstLen + 1) {
				int depth;
				if (Int32.TryParse (savepoint.Substring (firstLen + 1), out depth)) {
					// TODO: Mild race here, but inescapable without locking almost everywhere.
					if (0 <= depth && depth < _transactionDepth) {
#if NETFX_CORE || USE_SQLITEPCL_RAW || NETCORE
						Volatile.Write (ref _transactionDepth, depth);
#elif SILVERLIGHT
						_transactionDepth = depth;
#else
						Thread.VolatileWrite (ref _transactionDepth, depth);
#endif
						Execute (cmd + savepoint);
						return;
					}
				}
			}

			throw new ArgumentException ("savePoint is not valid, and should be the result of a call to SaveTransactionPoint.", "savePoint");
		}

		/// <summary>
		/// Commits the transaction that was begun by <see cref="BeginTransaction"/>.
		/// </summary>
		public void Commit ()
		{
			if (Interlocked.Exchange (ref _transactionDepth, 0) != 0) {
				try {
					Execute ("commit");
				}
				catch {
					// Force a rollback since most people don't know this function can fail
					// Don't call Rollback() since the _transactionDepth is 0 and it won't try
					// Calling rollback makes our _transactionDepth variable correct.
					try {
						Execute ("rollback");
					}
					catch {
						// rollback can fail in all sorts of wonderful version-dependent ways. Let's just hope for the best
					}
					throw;
				}
			}
			// Do nothing on a commit with no open transaction
		}

		/// <summary>
		/// Executes <paramref name="action"/> within a (possibly nested) transaction by wrapping it in a SAVEPOINT. If an
		/// exception occurs the whole transaction is rolled back, not just the current savepoint. The exception
		/// is rethrown.
		/// </summary>
		/// <param name="action">
		/// The <see cref="Action"/> to perform within a transaction. <paramref name="action"/> can contain any number
		/// of operations on the connection but should never call <see cref="BeginTransaction"/> or
		/// <see cref="Commit"/>.
		/// </param>
		public void RunInTransaction (Action action)
		{
			try {
				var savePoint = SaveTransactionPoint ();
				action ();
				Release (savePoint);
			}
			catch (Exception) {
				Rollback ();
				throw;
			}
		}

		/// <summary>
		/// Inserts all specified objects.
		/// </summary>
		/// <param name="objects">
		/// An <see cref="IEnumerable"/> of the objects to insert.
		/// <param name="runInTransaction"/>
		/// A boolean indicating if the inserts should be wrapped in a transaction.
		/// </param>
		/// <returns>
		/// The number of rows added to the table.
		/// </returns>
		public int InsertAll (System.Collections.IEnumerable objects, bool runInTransaction = true)
		{
			var c = 0;
			if (runInTransaction) {
				RunInTransaction (() => {
					foreach (var r in objects) {
						c += Insert (r);
					}
				});
			}
			else {
				foreach (var r in objects) {
					c += Insert (r);
				}
			}
			return c;
		}

		/// <summary>
		/// Inserts all specified objects.
		/// </summary>
		/// <param name="objects">
		/// An <see cref="IEnumerable"/> of the objects to insert.
		/// </param>
		/// <param name="extra">
		/// Literal SQL code that gets placed into the command. INSERT {extra} INTO ...
		/// </param>
		/// <param name="runInTransaction">
		/// A boolean indicating if the inserts should be wrapped in a transaction.
		/// </param>
		/// <returns>
		/// The number of rows added to the table.
		/// </returns>
		public int InsertAll (System.Collections.IEnumerable objects, string extra, bool runInTransaction = true)
		{
			var c = 0;
			if (runInTransaction) {
				RunInTransaction (() => {
					foreach (var r in objects) {
						c += Insert (r, extra);
					}
				});
			}
			else {
				foreach (var r in objects) {
					c += Insert (r, extra);
				}
			}
			return c;
		}

		/// <summary>
		/// Inserts all specified objects.
		/// </summary>
		/// <param name="objects">
		/// An <see cref="IEnumerable"/> of the objects to insert.
		/// </param>
		/// <param name="objType">
		/// The type of object to insert.
		/// </param>
		/// <param name="runInTransaction">
		/// A boolean indicating if the inserts should be wrapped in a transaction.
		/// </param>
		/// <returns>
		/// The number of rows added to the table.
		/// </returns>
		public int InsertAll (System.Collections.IEnumerable objects, Type objType, bool runInTransaction = true)
		{
			var c = 0;
			if (runInTransaction) {
				RunInTransaction (() => {
					foreach (var r in objects) {
						c += Insert (r, objType);
					}
				});
			}
			else {
				foreach (var r in objects) {
					c += Insert (r, objType);
				}
			}
			return c;
		}

		/// <summary>
		/// Inserts the given object (and updates its
		/// auto incremented primary key if it has one).
		/// The return value is the number of rows added to the table.
		/// </summary>
		/// <param name="obj">
		/// The object to insert.
		/// </param>
		/// <returns>
		/// The number of rows added to the table.
		/// </returns>
		public int Insert (object obj)
		{
			if (obj == null) {
				return 0;
			}
			return Insert (obj, "", Orm.GetType (obj));
		}

		/// <summary>
		/// Inserts the given object (and updates its
		/// auto incremented primary key if it has one).
		/// The return value is the number of rows added to the table.
		/// If a UNIQUE constraint violation occurs with
		/// some pre-existing object, this function deletes
		/// the old object.
		/// </summary>
		/// <param name="obj">
		/// The object to insert.
		/// </param>
		/// <returns>
		/// The number of rows modified.
		/// </returns>
		public int InsertOrReplace (object obj)
		{
			if (obj == null) {
				return 0;
			}
			return Insert (obj, "OR REPLACE", Orm.GetType (obj));
		}

		/// <summary>
		/// Inserts the given object (and updates its
		/// auto incremented primary key if it has one).
		/// The return value is the number of rows added to the table.
		/// </summary>
		/// <param name="obj">
		/// The object to insert.
		/// </param>
		/// <param name="objType">
		/// The type of object to insert.
		/// </param>
		/// <returns>
		/// The number of rows added to the table.
		/// </returns>
		public int Insert (object obj, Type objType)
		{
			return Insert (obj, "", objType);
		}

		/// <summary>
		/// Inserts the given object (and updates its
		/// auto incremented primary key if it has one).
		/// The return value is the number of rows added to the table.
		/// If a UNIQUE constraint violation occurs with
		/// some pre-existing object, this function deletes
		/// the old object.
		/// </summary>
		/// <param name="obj">
		/// The object to insert.
		/// </param>
		/// <param name="objType">
		/// The type of object to insert.
		/// </param>
		/// <returns>
		/// The number of rows modified.
		/// </returns>
		public int InsertOrReplace (object obj, Type objType)
		{
			return Insert (obj, "OR REPLACE", objType);
		}

		/// <summary>
		/// Inserts the given object (and updates its
		/// auto incremented primary key if it has one).
		/// The return value is the number of rows added to the table.
		/// </summary>
		/// <param name="obj">
		/// The object to insert.
		/// </param>
		/// <param name="extra">
		/// Literal SQL code that gets placed into the command. INSERT {extra} INTO ...
		/// </param>
		/// <returns>
		/// The number of rows added to the table.
		/// </returns>
		public int Insert (object obj, string extra)
		{
			if (obj == null) {
				return 0;
			}
			return Insert (obj, extra, Orm.GetType (obj));
		}

		/// <summary>
		/// Inserts the given object (and updates its
		/// auto incremented primary key if it has one).
		/// The return value is the number of rows added to the table.
		/// </summary>
		/// <param name="obj">
		/// The object to insert.
		/// </param>
		/// <param name="extra">
		/// Literal SQL code that gets placed into the command. INSERT {extra} INTO ...
		/// </param>
		/// <param name="objType">
		/// The type of object to insert.
		/// </param>
		/// <returns>
		/// The number of rows added to the table.
		/// </returns>
		public int Insert (object obj, string extra, Type objType)
		{
			if (obj == null || objType == null) {
				return 0;
			}

			var map = GetMapping (objType);

			if (map.PK != null && map.PK.IsAutoGuid) {
				if (map.PK.GetValue (obj).Equals (Guid.Empty)) {
					map.PK.SetValue (obj, Guid.NewGuid ());
				}
			}

			if (map.SyncGuid is TableMapping.GuidColumn SyncGuid) {
				SyncGuid.SetValue (null, null);
			}


			var replacing = string.Compare (extra, "OR REPLACE", StringComparison.OrdinalIgnoreCase) == 0;
			if (replacing) {

			}


			var cols = replacing ? map.InsertOrReplaceColumns : map.InsertColumns;
			var vals = new object[cols.Length];
			SqlParameter[] parameters = new SqlParameter[vals.Length];
			for (var i = 0; i < vals.Length; i++) {
				vals[i] = cols[i].GetValue (obj);
				parameters[i] = new SqlParameter (cols[i].Name, vals[i]);
			}

			var insertCmd = GetInsertCommand (map, extra);
			int count;

			lock (insertCmd) {
				// We lock here to protect the prepared statement returned via GetInsertCommand.
				// A SQLite prepared statement can be bound for only one operation at a time.
				try {
					if (map.HasAutoIncPK) {
						count = 0;
						long pk = insertCmd.ExecuteNonQueryAndRecoverLastScopeIdentity (parameters);
						if (pk > 0)
							count = 1;
						map.SetAutoIncPK (obj, pk);
					}
					else {
						count = insertCmd.ExecuteNonQuery (parameters);
					}
				}
				catch (SqlServerException ex) {
					//if (SQLite3.ExtendedErrCode (this.Handle) == SQLite3.ExtendedResult.ConstraintNotNull) {
					//	throw NotNullConstraintViolationException.New (ex.Result, ex.Message, map, obj);
					//}
					throw ex;
				}
			}
			if (count > 0)
				OnTableChanged (map, NotifyTableChangedAction.Insert);

			return count;
		}



		readonly Dictionary<Tuple<string, string>, PreparedSqlServerInsertCommand> _insertCommandMap = new Dictionary<Tuple<string, string>, PreparedSqlServerInsertCommand> ();

		PreparedSqlServerInsertCommand GetInsertCommand (TableMapping map, string extra)
		{
			PreparedSqlServerInsertCommand prepCmd;

			var key = Tuple.Create (map.MappedType.FullName, extra);

			lock (_insertCommandMap) {
				if (_insertCommandMap.TryGetValue (key, out prepCmd)) {
					return prepCmd;
				}
			}

			prepCmd = CreateInsertCommand (map, extra);

			lock (_insertCommandMap) {
				if (_insertCommandMap.TryGetValue (key, out var existing)) {
					prepCmd.Dispose ();
					return existing;
				}

				_insertCommandMap.Add (key, prepCmd);
			}

			return prepCmd;
		}

		PreparedSqlServerInsertCommand CreateInsertCommand (TableMapping map, string extra)
		{
			var cols = map.InsertColumns;
			string insertSql;
			if (cols.Length == 0 && map.Columns.Length == 1 && map.Columns[0].IsAutoInc) {
				insertSql = string.Format ("insert {1} into \"{0}\" default values", map.TableName, extra);
			}
			else {
				var replacing = string.Compare (extra, "OR REPLACE", StringComparison.OrdinalIgnoreCase) == 0;

				if (replacing) {
					cols = map.InsertOrReplaceColumns;
				}

				insertSql = string.Format ("insert {3} into \"{0}\"({1}) values ({2})"
					, map.TableName
					, string.Join (",", (from c in cols
										 select "\"" + c.Name + "\"").ToArray ())
					, string.Join (",", (from c in cols
										 select "@" + c.Name).ToArray ()), extra);

			}

			var insertCommand = new PreparedSqlServerInsertCommand (this, insertSql);
			return insertCommand;
		}

		/// <summary>
		/// Updates all of the columns of a table using the specified object
		/// except for its primary key.
		/// The object is required to have a primary key.
		/// </summary>
		/// <param name="obj">
		/// The object to update. It must have a primary key designated using the PrimaryKeyAttribute.
		/// </param>
		/// <returns>
		/// The number of rows updated.
		/// </returns>
		public int Update (object obj)
		{
			if (obj == null) {
				return 0;
			}
			return Update (obj, Orm.GetType (obj));
		}

		/// <summary>
		/// Updates all of the columns of a table using the specified object
		/// except for its primary key.
		/// The object is required to have a primary key.
		/// </summary>
		/// <param name="obj">
		/// The object to update. It must have a primary key designated using the PrimaryKeyAttribute.
		/// </param>
		/// <param name="objType">
		/// The type of object to insert.
		/// </param>
		/// <returns>
		/// The number of rows updated.
		/// </returns>
		public int Update (object obj, Type objType)
		{
			int rowsAffected = 0;
			if (obj == null || objType == null) {
				return 0;
			}

			var map = GetMapping (objType);

			var pk = map.PK;

			if (pk == null) {
				throw new NotSupportedException ("Cannot update " + map.TableName + ": it has no PK");
			}


			var cols = (from p in map.Columns
						where p != pk && !(p is TableMapping.GuidColumn)
						select p);
			var vals = (from c in cols
						select c.GetValue (obj));

			var ps = new List<object> (vals);

			if (ps.Count == 0) {
				// There is a PK but no accompanying data,
				// so reset the PK to make the UPDATE work.
				cols = map.Columns;
				vals = from c in cols
					   select c.GetValue (obj);
				ps = new List<object> (vals);
			}
			ps.Add (pk.GetValue (obj));

			List<SqlParameter> parmeters = new List<SqlParameter> (ps.Count);
			for (var i = 0; i < cols.Count (); i++) {
				parmeters.Add (new SqlParameter (cols.ElementAt (i).Name, ps[i]));
			}
			parmeters.Add (new SqlParameter (pk.Name, pk.GetValue (obj)));

			var q = string.Format ("update \"{0}\" set {1} where {2} = @{2} ", map.TableName
				, string.Join (",", (from c in cols select "\"" + c.Name + "\" = @" + c.Name + " ").ToArray ()), pk.Name);

			try {
				rowsAffected = Execute (q, parmeters.ToArray ());
			}
			catch (SqlServerException ex) {

				//if (ex.Result == SQLite3.Result.Constraint && SQLite3.ExtendedErrCode (this.Handle) == SQLite3.ExtendedResult.ConstraintNotNull) {
				//	throw NotNullConstraintViolationException.New (ex, map, obj);
				//}

				throw ex;
			}

			if (rowsAffected > 0) {
				map.SyncGuid.SetValue (obj, ExecuteScalar<Guid> (
					$"SELECT SyncGuid from {map.TableName} where {map.PK.Name}=@{map.PK.Name}",
					new SqlParameter (map.PK.Name, map.PK.GetValue (obj))));
				OnTableChanged (map, NotifyTableChangedAction.Update);
			}

			return rowsAffected;
		}

		/// <summary>
		/// Updates all specified objects.
		/// </summary>
		/// <param name="objects">
		/// An <see cref="IEnumerable"/> of the objects to insert.
		/// </param>
		/// <param name="runInTransaction">
		/// A boolean indicating if the inserts should be wrapped in a transaction
		/// </param>
		/// <returns>
		/// The number of rows modified.
		/// </returns>
		public int UpdateAll (System.Collections.IEnumerable objects, bool runInTransaction = true)
		{
			var c = 0;
			if (runInTransaction) {
				RunInTransaction (() => {
					foreach (var r in objects) {
						c += Update (r);
					}
				});
			}
			else {
				foreach (var r in objects) {
					c += Update (r);
				}
			}
			return c;
		}

		/// <summary>
		/// Deletes the given object from the database using its primary key.
		/// </summary>
		/// <param name="objectToDelete">
		/// The object to delete. It must have a primary key designated using the PrimaryKeyAttribute.
		/// </param>
		/// <returns>
		/// The number of rows deleted.
		/// </returns>
		public int Delete (object objectToDelete)
		{
			var map = GetMapping (Orm.GetType (objectToDelete));
			var pk = map.PK;
			if (pk == null) {
				throw new NotSupportedException ("Cannot delete " + map.TableName + ": it has no PK");
			}
			var q = string.Format ("delete from \"{0}\" where \"{1}\" = @{1}", map.TableName, pk.Name);

			map.SyncGuid.SetValue (objectToDelete, ExecuteScalar<Guid> (
				$"SELECT SyncGuid from {map.TableName} where {map.PK.Name}=@{map.PK.Name}",
				new SqlParameter (map.PK.Name, map.PK.GetValue (objectToDelete))));

			var count = Execute (q, new SqlParameter (map.PK.Name, map.PK.GetValue (objectToDelete)));
			if (count > 0)
				OnTableChanged (map, NotifyTableChangedAction.Delete);
			return count;
		}

		/// <summary>
		/// Deletes the object with the specified primary key.
		/// </summary>
		/// <param name="primaryKey">
		/// The primary key of the object to delete.
		/// </param>
		/// <returns>
		/// The number of objects deleted.
		/// </returns>
		/// <typeparam name='T'>
		/// The type of object.
		/// </typeparam>
		public int Delete<T> (object primaryKey)
		{
			return Delete (primaryKey, GetMapping (typeof (T)));
		}

		/// <summary>
		/// Deletes the object with the specified primary key.
		/// </summary>
		/// <param name="primaryKey">
		/// The primary key of the object to delete.
		/// </param>
		/// <param name="map">
		/// The TableMapping used to identify the table.
		/// </param>
		/// <returns>
		/// The number of objects deleted.
		/// </returns>
		public int Delete (object primaryKey, TableMapping map)
		{
			var pk = map.PK;
			if (pk == null) {
				throw new NotSupportedException ("Cannot delete " + map.TableName + ": it has no PK");
			}
			var q = string.Format ("delete from \"{0}\" where \"{1}\" = @{1}", map.TableName, pk.Name);

			map.SyncGuid.SetValue (null, ExecuteScalar<Guid> (
				$"SELECT SyncGuid from {map.TableName} where {map.PK.Name}=@{map.PK.Name}",
				new SqlParameter (map.PK.Name, primaryKey)));

			var count = Execute (q, new SqlParameter (map.PK.Name, primaryKey));
			if (count > 0)
				OnTableChanged (map, NotifyTableChangedAction.Delete);
			return count;
		}

		/// <summary>
		/// Deletes all the objects from the specified table.
		/// WARNING WARNING: Let me repeat. It deletes ALL the objects from the
		/// specified table. Do you really want to do that?
		/// </summary>
		/// <returns>
		/// The number of objects deleted.
		/// </returns>
		/// <typeparam name='T'>
		/// The type of objects to delete.
		/// </typeparam>
		public int DeleteAll<T> ()
		{
			var map = GetMapping (typeof (T));
			return DeleteAll (map);
		}

		/// <summary>
		/// Deletes all the objects from the specified table.
		/// WARNING WARNING: Let me repeat. It deletes ALL the objects from the
		/// specified table. Do you really want to do that?
		/// </summary>
		/// <param name="map">
		/// The TableMapping used to identify the table.
		/// </param>
		/// <returns>
		/// The number of objects deleted.
		/// </returns>
		public int DeleteAll (TableMapping map)
		{
			var query = string.Format ("delete from \"{0}\"", map.TableName);
			OnTableDeleteAll (map);
			var count = Execute (query);
			if (count > 0)
				OnTableChanged (map, NotifyTableChangedAction.Delete);
			return count;
		}

		/// <summary>
		/// Backup the entire database to the specified path.
		/// </summary>
		/// <param name="destinationDatabasePath">Path to backup file.</param>
		/// <param name="databaseName">The name of the database to backup (usually "main").</param>
		//public void Backup (string destinationDatabasePath, string databaseName = "main")
		//{
		//	// Open the destination
		//	var r = SQLite3.Open (destinationDatabasePath, out var destHandle);
		//	if (r != SQLite3.Result.OK) {
		//		throw SqlServerException.New (r, "Failed to open destination database");
		//	}

		//	// Init the backup
		//	var backup = SQLite3.BackupInit (destHandle, databaseName, Handle, databaseName);
		//	if (backup == NullBackupHandle) {
		//		SQLite3.Close (destHandle);
		//		throw new Exception ("Failed to create backup");
		//	}

		//	// Perform it
		//	SQLite3.BackupStep (backup, -1);
		//	SQLite3.BackupFinish (backup);

		//	// Check for errors
		//	r = SQLite3.GetResult (destHandle);
		//	string msg = "";
		//	if (r != SQLite3.Result.OK) {
		//		msg = SQLite3.GetErrmsg (destHandle);
		//	}

		//	// Close everything and report errors
		//	SQLite3.Close (destHandle);
		//	if (r != SQLite3.Result.OK) {
		//		throw SqlServerException.New (r, msg);
		//	}
		//}

		~SQLServerConnection ()
		{
			Dispose (false);
		}

		public void Dispose ()
		{
			Dispose (true);
			GC.SuppressFinalize (this);
		}

		public void Close ()
		{
			Dispose (true);
		}

		protected virtual void Dispose (bool disposing)
		{
			if (IsOpen) {
				try {
					if (disposing) {
						lock (_insertCommandMap) {
							foreach (var sqlInsertCommand in _insertCommandMap.Values) {
								sqlInsertCommand.Dispose ();
							}

							_insertCommandMap.Clear ();
						}

						Connection.Close ();
						Connection.Dispose ();
					}
					else {
						Connection.Close ();
					}
				}
				catch (Exception ex) {
					throw ex;
				}
			}
		}
		public event EventHandler<NotifyTableChangedEventArgs> TableChanged;

		void OnTableDeleteAll (TableMapping table)
		{
			Table<ChangesHistory> ().Delete (x => x.TableName == table.TableName);
			QueryScalars<Guid> ($"SELECT SyncGuid FROM {table.TableName}")
				.ForEach (x => Insert (new ChangesHistory (table.TableName, x, NotifyTableChangedAction.Delete)));
		}
		void OnTableChanged (TableMapping table, NotifyTableChangedAction action)
		{
			if (table.TableName == nameof (ChangesHistory)) {
				return;
			}

			UpdateVersionControl (new ChangesHistory (
				table.TableName
				, (Guid)table.SyncGuid.GetValue (null)
				, action));
			var ev = TableChanged;
			if (ev != null)
				ev (this, new NotifyTableChangedEventArgs (table, action));
		}

		void UpdateVersionControl (ChangesHistory VersionControl)
		{
			Table<ChangesHistory> ()
				.Delete (x => x.SyncGuid == VersionControl.SyncGuid);
			Insert (VersionControl);
		}

	}
}
