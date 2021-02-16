using System;
using SQLBase;
using SQLBase.Sync.Enums;

namespace SQLServer
{

#if WINDOWS_PHONE && !USE_WP8_NATIVE_SQLITE
#define USE_CSHARP_SQLITE
#endif

	using System;
#if !USE_SQLITEPCL_RAW
	using System.Runtime.InteropServices;
#endif
	using System.Collections.Generic;
	using System.Reflection;
	using System.Linq;
	using System.Linq.Expressions;
	using System.Text;
#if USE_CSHARP_SQLITE
using Sqlite3 = Community.CsharpSqlite.Sqlite3;
using Sqlite3DatabaseHandle = Community.CsharpSqlite.Sqlite3.sqlite3;
using Sqlite3Statement = Community.CsharpSqlite.Sqlite3.Vdbe;
#elif USE_WP8_NATIVE_SQLITE
using Sqlite3 = Sqlite.Sqlite3;
using Sqlite3DatabaseHandle = Sqlite.Database;
using Sqlite3Statement = Sqlite.Statement;
#elif USE_SQLITEPCL_RAW
using Sqlite3DatabaseHandle = SQLitePCL.sqlite3;
using Sqlite3BackupHandle = SQLitePCL.sqlite3_backup;
using Sqlite3Statement = SQLitePCL.sqlite3_stmt;
using Sqlite3 = SQLitePCL.raw;
#else
	using Sqlite3DatabaseHandle = System.IntPtr;
	using Sqlite3BackupHandle = System.IntPtr;
	using Sqlite3Statement = System.IntPtr;
	using System.Data.SqlClient;
#endif

#pragma warning disable 1591 // XML Doc Comments


	public class SqlServerException : Exception
	{
		public SQLite3.Result Result { get; private set; }

		protected SqlServerException (SQLite3.Result r, string message) : base (message)
		{
			Result = r;
		}

		public static SqlServerException New (SQLite3.Result r, string message)
		{
			return new SqlServerException (r, message);
		}
	}

	public class NotNullConstraintViolationException : SqlServerException
	{
		public IEnumerable<TableMapping.Column> Columns { get; protected set; }

		protected NotNullConstraintViolationException (SQLite3.Result r, string message)
			: this (r, message, null, null)
		{

		}

		protected NotNullConstraintViolationException (SQLite3.Result r, string message, TableMapping mapping, object obj)
			: base (r, message)
		{
			if (mapping != null && obj != null) {
				this.Columns = from c in mapping.Columns
							   where c.IsNullable == false && c.GetValue (obj) == null
							   select c;
			}
		}

		public static new NotNullConstraintViolationException New (SQLite3.Result r, string message)
		{
			return new NotNullConstraintViolationException (r, message);
		}

		public static NotNullConstraintViolationException New (SQLite3.Result r, string message, TableMapping mapping, object obj)
		{
			return new NotNullConstraintViolationException (r, message, mapping, obj);
		}

		public static NotNullConstraintViolationException New (SqlServerException exception, TableMapping mapping, object obj)
		{
			return new NotNullConstraintViolationException (exception.Result, exception.Message, mapping, obj);
		}
	}

	//[Flags]
	//public enum SQLiteOpenFlags
	//{
	//	ReadOnly = 1, ReadWrite = 2, Create = 4,
	//	NoMutex = 0x8000, FullMutex = 0x10000,
	//	SharedCache = 0x20000, PrivateCache = 0x40000,
	//	ProtectionComplete = 0x00100000,
	//	ProtectionCompleteUnlessOpen = 0x00200000,
	//	ProtectionCompleteUntilFirstUserAuthentication = 0x00300000,
	//	ProtectionNone = 0x00400000
	//}

	[Flags]
	public enum CreateFlags
	{
		/// <summary>
		/// Use the default creation options
		/// </summary>
		None = 0x000,
		/// <summary>
		/// Create a primary key index for a property called 'Id' (case-insensitive).
		/// This avoids the need for the [PrimaryKey] attribute.
		/// </summary>
		ImplicitPK = 0x001,
		/// <summary>
		/// Create indices for properties ending in 'Id' (case-insensitive).
		/// </summary>
		ImplicitIndex = 0x002,
		/// <summary>
		/// Create a primary key for a property called 'Id' and
		/// create an indices for properties ending in 'Id' (case-insensitive).
		/// </summary>
		AllImplicit = 0x003,
		/// <summary>
		/// Force the primary key property to be auto incrementing.
		/// This avoids the need for the [AutoIncrement] attribute.
		/// The primary key property on the class should have type int or long.
		/// </summary>
		AutoIncPK = 0x004,
		/// <summary>
		/// Create virtual table using FTS3
		/// </summary>
		FullTextSearch3 = 0x100,
		/// <summary>
		/// Create virtual table using FTS4
		/// </summary>
		FullTextSearch4 = 0x200
	}

	public class NotifyTableChangedEventArgs : EventArgs
	{
		public TableMapping Table { get; private set; }
		public NotifyTableChangedAction Action { get; private set; }

		public NotifyTableChangedEventArgs (TableMapping table, NotifyTableChangedAction action)
		{
			Table = table;
			Action = action;
		}
	}

	public class TableMapping
	{
		public Type MappedType { get; private set; }

		public string TableName { get; private set; }

		public bool WithoutRowId { get; private set; }

		public Column[] Columns { get; private set; }

		public Column PK { get; private set; }
		public Column SyncGuid { get; private set; }

		public string GetByPrimaryKeySql { get; private set; }


		readonly Column _autoPk;
		readonly Column[] _insertColumns;
		readonly Column[] _insertOrReplaceColumns;

		public TableMapping (Type type, CreateFlags createFlags = CreateFlags.None)
		{
			MappedType = type;

			var typeInfo = type.GetTypeInfo ();
#if ENABLE_IL2CPP
			var tableAttr = typeInfo.GetCustomAttribute<TableAttribute> ();
#else
			var tableAttr =
				typeInfo.CustomAttributes
						.Where (x => x.AttributeType == typeof (TableAttribute))
						.Select (x => (TableAttribute)Orm.InflateAttribute (x))
						.FirstOrDefault ();
#endif

			TableName = (tableAttr != null && !string.IsNullOrEmpty (tableAttr.Name)) ? tableAttr.Name : MappedType.Name;
			WithoutRowId = tableAttr != null ? tableAttr.WithoutRowId : false;

			var props = new List<PropertyInfo> ();
			var baseType = type;
			var propNames = new HashSet<string> ();
			while (baseType != typeof (object)) {
				var ti = baseType.GetTypeInfo ();
				var newProps = (
					from p in ti.DeclaredProperties
					where
						!propNames.Contains (p.Name) &&
						p.CanRead && p.CanWrite &&
						(p.GetMethod != null) && (p.SetMethod != null) &&
						(p.GetMethod.IsPublic && p.SetMethod.IsPublic) &&
						(!p.GetMethod.IsStatic) && (!p.SetMethod.IsStatic)
					select p).ToList ();
				foreach (var p in newProps) {
					propNames.Add (p.Name);
				}
				props.AddRange (newProps);
				baseType = ti.BaseType;
			}

			var cols = new List<Column> ();
			foreach (var p in props) {
				var ignore = p.IsDefined (typeof (IgnoreAttribute), true);
				if (!ignore) {
					cols.Add (new Column (p, createFlags));
				}
			}

			foreach (var c in cols) {
				if (c.IsAutoInc && c.IsPK) {
					_autoPk = c;
				}
				if (c.IsPK) {
					PK = c;
				}
			}

			HasAutoIncPK = _autoPk != null;

			if (PK != null) {
				GetByPrimaryKeySql = string.Format ("select * from \"{0}\" where \"{1}\" = @{1}", TableName, PK.Name);
			}
			else {
				// People should not be calling Get/Find without a PK
				GetByPrimaryKeySql = string.Format ("select * from \"{0}\" limit 1", TableName);
			}

			if (cols.FirstOrDefault (x => x.Name == "SyncGuid") is Column syncguidcol) {
				this.SyncGuid = new GuidColumn (syncguidcol);
			}
			else {
				this.SyncGuid = new GuidColumn ();
				cols.Add (this.SyncGuid);
			}

			Columns = cols.ToArray ();

			_insertColumns = Columns.Where (c => !c.IsAutoInc).ToArray ();
			_insertOrReplaceColumns = Columns.ToArray ();
		}

		public bool HasAutoIncPK { get; private set; }

		public void SetAutoIncPK (object obj, long id)
		{
			if (_autoPk != null) {
				_autoPk.SetValue (obj, Convert.ChangeType (id, _autoPk.ColumnType, null));
			}
		}

		public Column[] InsertColumns {
			get {
				return _insertColumns;
			}
		}

		public Column[] InsertOrReplaceColumns {
			get {
				return _insertOrReplaceColumns;
			}
		}

		public Column FindColumnWithPropertyName (string propertyName)
		{
			var exact = Columns.FirstOrDefault (c => c.PropertyName == propertyName);
			return exact;
		}

		public Column FindColumn (string columnName)
		{
			var exact = Columns.FirstOrDefault (c => c.Name.ToLower () == columnName.ToLower ());
			return exact;
		}

		public class Column
		{
			protected PropertyInfo _prop;

			public string Name { get; protected set; }

			public PropertyInfo PropertyInfo => _prop;

			public string PropertyName { get { return _prop.Name; } }

			public Type ColumnType { get; protected set; }

			public string Collation { get; protected set; }

			public bool IsAutoInc { get; private set; }
			public bool IsAutoGuid { get; protected set; }

			public bool IsPK { get; protected set; }

			public IEnumerable<IndexedAttribute> Indices { get; set; }

			public bool IsNullable { get; protected set; }

			public int? MaxStringLength { get; protected set; }

			public bool StoreAsText { get; protected set; }

			protected Column () { }
			public Column (PropertyInfo prop, CreateFlags createFlags = CreateFlags.None)
			{
				var colAttr = prop.CustomAttributes.FirstOrDefault (x => x.AttributeType == typeof (ColumnAttribute));

				_prop = prop;
#if ENABLE_IL2CPP
                var ca = prop.GetCustomAttribute(typeof(ColumnAttribute)) as ColumnAttribute;
				Name = ca == null ? prop.Name : ca.Name;
#else
				Name = (colAttr != null && colAttr.ConstructorArguments.Count > 0) ?
						colAttr.ConstructorArguments[0].Value?.ToString () :
						prop.Name;
#endif
				//If this type is Nullable<T> then Nullable.GetUnderlyingType returns the T, otherwise it returns null, so get the actual type instead
				ColumnType = Nullable.GetUnderlyingType (prop.PropertyType) ?? prop.PropertyType;
				Collation = Orm.Collation (prop);

				IsPK = Orm.IsPK (prop) ||
					(((createFlags & CreateFlags.ImplicitPK) == CreateFlags.ImplicitPK) &&
						 string.Compare (prop.Name, Orm.ImplicitPkName, StringComparison.OrdinalIgnoreCase) == 0);

				var isAuto = Orm.IsAutoInc (prop) || (IsPK && ((createFlags & CreateFlags.AutoIncPK) == CreateFlags.AutoIncPK));
				IsAutoGuid = isAuto && ColumnType == typeof (Guid);
				IsAutoInc = isAuto && !IsAutoGuid;

				Indices = Orm.GetIndices (prop);
				if (!Indices.Any ()
					&& !IsPK
					&& ((createFlags & CreateFlags.ImplicitIndex) == CreateFlags.ImplicitIndex)
					&& Name.EndsWith (Orm.ImplicitIndexSuffix, StringComparison.OrdinalIgnoreCase)
					) {
					Indices = new IndexedAttribute[] { new IndexedAttribute () };
				}
				IsNullable = !(IsPK || Orm.IsMarkedNotNull (prop));
				MaxStringLength = Orm.MaxStringLength (prop);

				StoreAsText = prop.PropertyType.GetTypeInfo ().CustomAttributes.Any (x => x.AttributeType == typeof (StoreAsTextAttribute));
			}

			public virtual void SetValue (object obj, object val)
			{
				if (val != null && ColumnType.GetTypeInfo ().IsEnum) {
					_prop.SetValue (obj, Enum.ToObject (ColumnType, val));
				}
				else {
					_prop.SetValue (obj, val, null);
				}
			}

			public virtual object GetValue (object obj)
			{
				return _prop.GetValue (obj, null);
			}
		}

		public class GuidColumn : Column
		{
			private Guid SyncGuid;

			public GuidColumn (Column syncguidcol) : base ()
			{
				Name = syncguidcol.Name;
				ColumnType = syncguidcol.ColumnType;
				Collation = syncguidcol.Collation;
				IsPK = syncguidcol.IsPK;
				IsAutoGuid = true;
				IsNullable = false;
				MaxStringLength = null;
				StoreAsText = syncguidcol.StoreAsText;
				Indices = new IndexedAttribute[] { new IndexedAttribute (Name, -1) { Unique = true } };
				_prop = syncguidcol.PropertyInfo;
			}
			public GuidColumn () : base ()
			{
				Name = "SyncGuid";
				//If this type is Nullable<T> then Nullable.GetUnderlyingType returns the T, otherwise it returns null, so get the actual type instead
				ColumnType = typeof (Guid);
				Collation = "";
				IsPK = false;
				IsAutoGuid = true;
				IsNullable = false;
				MaxStringLength = null;
				StoreAsText = ColumnType.CustomAttributes.Any (x => x.AttributeType == typeof (StoreAsTextAttribute));
				Indices = new IndexedAttribute[] { new IndexedAttribute (Name, -1) { Unique = true } };

			}

			public void SetValue ()
			{
				SyncGuid = Guid.NewGuid ();
			}
			public override void SetValue (object obj, object val)
			{
				if (val is Guid guid) {
					SyncGuid = guid;
					return;
				}
				SetValue ();
			}

			public Guid GetValue ()
			{
				return SyncGuid;
			}
			public override object GetValue (object obj)
			{
				return GetValue ();
			}
		}
	}

	class EnumCacheInfo
	{
		public EnumCacheInfo (Type type)
		{
			var typeInfo = type.GetTypeInfo ();

			IsEnum = typeInfo.IsEnum;

			if (IsEnum) {
				StoreAsText = typeInfo.CustomAttributes.Any (x => x.AttributeType == typeof (StoreAsTextAttribute));

				if (StoreAsText) {
					EnumValues = new Dictionary<int, string> ();
					foreach (object e in Enum.GetValues (type)) {
						EnumValues[Convert.ToInt32 (e)] = e.ToString ();
					}
				}
			}
		}

		public bool IsEnum { get; private set; }

		public bool StoreAsText { get; private set; }

		public Dictionary<int, string> EnumValues { get; private set; }
	}

	static class EnumCache
	{
		static readonly Dictionary<Type, EnumCacheInfo> Cache = new Dictionary<Type, EnumCacheInfo> ();

		public static EnumCacheInfo GetInfo<T> ()
		{
			return GetInfo (typeof (T));
		}

		public static EnumCacheInfo GetInfo (Type type)
		{
			lock (Cache) {
				EnumCacheInfo info = null;
				if (!Cache.TryGetValue (type, out info)) {
					info = new EnumCacheInfo (type);
					Cache[type] = info;
				}

				return info;
			}
		}
	}

	public static class Orm
	{
		public const int DefaultMaxStringLength = 140;
		public const string ImplicitPkName = "Id";
		public const string ImplicitIndexSuffix = "Id";

		public static Type GetType (object obj)
		{
			if (obj == null)
				return typeof (object);
			var rt = obj as IReflectableType;
			if (rt != null)
				return rt.GetTypeInfo ().AsType ();
			return obj.GetType ();
		}

		public static string SqlDecl (TableMapping.Column p)
		{
			string decl = "\"" + p.Name + "\" " + SqlType (p) + " ";

			if (p.IsPK) {
				decl += "primary key ";
			}
			if (p.IsAutoInc) {
				decl += "IDENTITY(1,1) ";
			}
			if (!p.IsNullable) {
				decl += "not null ";
			}

			if (p.IsAutoGuid) {
				decl += "DEFAULT NEWID() ";
			}

			if (p.Indices.Any ()) {
				foreach (var i in p.Indices) {
					if (i.Unique) {
						decl += "UNIQUE ";
					}
				}
				//var indexes = new Dictionary<string, SQLServerConnection.IndexInfo> ();

				//foreach (var i in p.Indices) {
				//	//var iname = i.Name ?? map.TableName + "_" + c.Name;
				//	SQLServerConnection.IndexInfo iinfo;
				//	if (!indexes.TryGetValue (iname, out iinfo)) {
				//		iinfo = new SQLServerConnection.IndexInfo {
				//			IndexName = iname,
				//			TableName = map.TableName,
				//			Unique = i.Unique,
				//			Columns = new List<SQLServerConnection.IndexedColumn> ()
				//		};
				//		indexes.Add (iname, iinfo);
				//	}

				//	if (i.Unique != iinfo.Unique)
				//		throw new Exception ("All the columns in an index must have the same value for their Unique property");

				//	iinfo.Columns.Add (new SQLServerConnection.IndexedColumn {
				//		Order = i.Order,
				//		ColumnName = p.Name
				//	});
				//}


				//foreach (var indexName in indexes.Keys) {
				//	var index = indexes[indexName];
				//	var columns = index.Columns.OrderBy (i => i.Order).Select (i => i.ColumnName).ToArray ();
				//	CreateIndex (indexName, index.TableName, columns, index.Unique);
				//}
			}

			if (!string.IsNullOrEmpty (p.Collation)) {
				decl += "collate " + p.Collation + " ";
			}

			return decl;
		}

		public static string SqlType (TableMapping.Column p)
		{
			var clrType = p.ColumnType;
			if (clrType == typeof (Boolean) || clrType == typeof (Byte) || clrType == typeof (UInt16) || clrType == typeof (SByte) || clrType == typeof (Int16) || clrType == typeof (Int32) || clrType == typeof (UInt32) || clrType == typeof (Int64)) {
				return "integer";
			}
			else if (clrType == typeof (Single) || clrType == typeof (Double) || clrType == typeof (Decimal)) {
				return "float";
			}
			else if (clrType == typeof (String) || clrType == typeof (StringBuilder) || clrType == typeof (Uri) || clrType == typeof (UriBuilder)) {
				int? len = p.MaxStringLength;

				if (len.HasValue)
					return "varchar(" + len.Value + ")";

				return "varchar(MAX)";
			}
			else if (clrType == typeof (TimeSpan)) {
				return "time";
			}
			else if (clrType == typeof (DateTime)) {
				return "datetime";
			}
			else if (clrType == typeof (DateTimeOffset)) {
				return "bigint";
			}
			else if (clrType.GetTypeInfo ().IsEnum) {
				if (p.StoreAsText)
					return "varchar(MAX)";
				else
					return "integer";
			}
			else if (clrType == typeof (byte[])) {
				return "blob";
			}
			else if (clrType == typeof (Guid)) {
				return "UNIQUEIDENTIFIER";
			}
			else {
				throw new NotSupportedException ("Don't know about " + clrType);
			}
		}

		public static bool IsPK (MemberInfo p)
		{
			return p.CustomAttributes.Any (x => x.AttributeType == typeof (PrimaryKeyAttribute));
		}

		public static string Collation (MemberInfo p)
		{
#if ENABLE_IL2CPP
			return (p.GetCustomAttribute<CollationAttribute> ()?.Value) ?? "";
#else
			return
				(p.CustomAttributes
				 .Where (x => typeof (CollationAttribute) == x.AttributeType)
				 .Select (x => {
					 var args = x.ConstructorArguments;
					 return args.Count > 0 ? ((args[0].Value as string) ?? "") : "";
				 })
				 .FirstOrDefault ()) ?? "";
#endif
		}

		public static bool IsAutoInc (MemberInfo p)
		{
			return p.CustomAttributes.Any (x => x.AttributeType == typeof (AutoIncrementAttribute));
		}

		public static FieldInfo GetField (TypeInfo t, string name)
		{
			var f = t.GetDeclaredField (name);
			if (f != null)
				return f;
			return GetField (t.BaseType.GetTypeInfo (), name);
		}

		public static PropertyInfo GetProperty (TypeInfo t, string name)
		{
			var f = t.GetDeclaredProperty (name);
			if (f != null)
				return f;
			return GetProperty (t.BaseType.GetTypeInfo (), name);
		}

		public static object InflateAttribute (CustomAttributeData x)
		{
			var atype = x.AttributeType;
			var typeInfo = atype.GetTypeInfo ();
#if ENABLE_IL2CPP
			var r = Activator.CreateInstance (x.AttributeType);
#else
			var args = x.ConstructorArguments.Select (a => a.Value).ToArray ();
			var r = Activator.CreateInstance (x.AttributeType, args);
			foreach (var arg in x.NamedArguments) {
				if (arg.IsField) {
					GetField (typeInfo, arg.MemberName).SetValue (r, arg.TypedValue.Value);
				}
				else {
					GetProperty (typeInfo, arg.MemberName).SetValue (r, arg.TypedValue.Value);
				}
			}
#endif
			return r;
		}

		public static IEnumerable<IndexedAttribute> GetIndices (MemberInfo p)
		{
#if ENABLE_IL2CPP
			return p.GetCustomAttributes<IndexedAttribute> ();
#else
			var indexedInfo = typeof (IndexedAttribute).GetTypeInfo ();
			return
				p.CustomAttributes
				 .Where (x => indexedInfo.IsAssignableFrom (x.AttributeType.GetTypeInfo ()))
				 .Select (x => (IndexedAttribute)InflateAttribute (x));
#endif
		}

		public static int? MaxStringLength (PropertyInfo p)
		{
#if ENABLE_IL2CPP
			return p.GetCustomAttribute<MaxLengthAttribute> ()?.Value;
#else
			var attr = p.CustomAttributes.FirstOrDefault (x => x.AttributeType == typeof (MaxLengthAttribute));
			if (attr != null) {
				var attrv = (MaxLengthAttribute)InflateAttribute (attr);
				return attrv.Value;
			}
			return null;
#endif
		}

		public static bool IsMarkedNotNull (MemberInfo p)
		{
			return p.CustomAttributes.Any (x => x.AttributeType == typeof (NotNullAttribute));
		}
	}

	public partial class SQLServerCommand
	{
		SQLServerConnection _conn;
		private List<Binding> _bindings;
		public string CommandText { get; set; }
		public List<SqlParameter> Parameters { get; internal set; }

		public SQLServerCommand (SQLServerConnection conn, string cmd, params SqlParameter[] parameters)
		{
			this.Parameters = new List<SqlParameter> (parameters);
			CommandText = cmd;
			_conn = conn;
		}
		public SQLServerCommand (SQLServerConnection conn)
		{
			_conn = conn;
			_bindings = new List<Binding> ();
			CommandText = "";
		}

		public int ExecuteNonQuery ()
		{
			if (_conn.Trace) {
				_conn.Tracer?.Invoke ("Executing: " + this);
			}

			if (_conn.IsClosed) {
				_conn.RenewConnection ();
			}
			using (var con = _conn.Connection) {
				con.Open ();
				using (SqlCommand cmd = new SqlCommand (CommandText, _conn.Connection)) {
					if (this.Parameters.Any ()) {
						cmd.Parameters.AddRange (this.Parameters.ToArray ());
					}
					return cmd.ExecuteNonQuery ();
				}
			}
			return 0;
		}

		public IEnumerable<T> ExecuteDeferredQuery<T> ()
		{
			return ExecuteDeferredQuery<T> (_conn.GetMapping (typeof (T)));
		}

		public List<T> ExecuteQuery<T> ()
		{
			return ExecuteDeferredQuery<T> (_conn.GetMapping (typeof (T))).ToList ();
		}

		public List<T> ExecuteQuery<T> (TableMapping map)
		{
			return ExecuteDeferredQuery<T> (map).ToList ();
		}

		/// <summary>
		/// Invoked every time an instance is loaded from the database.
		/// </summary>
		/// <param name='obj'>
		/// The newly created object.
		/// </param>
		/// <remarks>
		/// This can be overridden in combination with the <see cref="SQLServerConnection.NewCommand"/>
		/// method to hook into the life-cycle of objects.
		/// </remarks>
		protected virtual void OnInstanceCreated (object obj)
		{
			// Can be overridden.
		}

		public IEnumerable<T> ExecuteDeferredQuery<T> (TableMapping map)
		{
			if (_conn.Trace) {
				_conn.Tracer?.Invoke ("Executing Query: " + this);
			}

			if (_conn.IsClosed) {
				_conn.RenewConnection ();
			}
			using (var con = _conn.Connection) {
				con.Open ();
				using (var cmd = new SqlCommand (this.CommandText, con)) {
					if (this.Parameters.Any ()) {
						cmd.Parameters.AddRange (Parameters.ToArray ());
					}
					using (var reader = cmd.ExecuteReader ()) {
						if (reader.Read ()) {
							var cols = new TableMapping.Column[reader.FieldCount];
							var fastColumnSetters = new Action<T, SqlDataReader, int>[reader.FieldCount];
							for (int i = 0; i < cols.Length; i++) {
								var name = reader.GetName (i);
								cols[i] = map.FindColumn (name);
								if (cols[i] != null)
									fastColumnSetters[i] = FastColumnSetter.GetFastSetter<T> (_conn, cols[i]);
							}

							do {
								var obj = Activator.CreateInstance (map.MappedType);
								for (int i = 0; i < cols.Length; i++) {
									if (cols[i] == null)
										continue;

									if (fastColumnSetters[i] != null) {
										fastColumnSetters[i].Invoke ((T)obj, reader, i);
									}
									else {
										var colType = reader.GetFieldType (i);
										var val = ReadCol (reader, i, colType, cols[i].ColumnType);
										cols[i].SetValue (obj, val);
									}
								}
								OnInstanceCreated (obj);
								yield return (T)obj;

							} while ((reader.Read ()));
						}
					}

				}
			}
		}

		public T ExecuteScalar<T> ()
		{
			if (_conn.Trace) {
				_conn.Tracer?.Invoke ("Executing Query: " + this);
			}

			T val = default (T);

			if (_conn.IsClosed) {
				_conn.RenewConnection ();
			}
			using (var con = _conn.Connection) {
				con.Open ();
				using (var cmd = new SqlCommand (this.CommandText + " select SCOPE_IDENTITY();", con)) {
					if (Parameters?.Any () ?? false) {
						cmd.Parameters.AddRange (Parameters.ToArray ());
					}
					using (var reader = cmd.ExecuteReader ()) {
						if (reader.Read ()) {

							var colval = ReadCol (reader, 0, reader.GetFieldType (0), typeof (T));
							if (colval != null) {
								val = (T)colval;
							}
						}
					}
				}
			}

			//var stmt = Prepare ();

			//try {
			//	var r = SQLite3.Step (stmt);
			//	if (r == SQLite3.Result.Row) {
			//		var colType = SQLite3.ColumnType (stmt, 0);
			//		var colval = ReadCol (stmt, 0, colType, typeof (T));
			//		if (colval != null) {
			//			val = (T)colval;
			//		}
			//	}
			//	else if (r == SQLite3.Result.Done) {
			//	}
			//	else {
			//		throw SQLiteException.New (r, SQLite3.GetErrmsg (_conn.Handle));
			//	}
			//}
			//finally {
			//	Finalize (stmt);
			//}

			return val;
		}

		public IEnumerable<T> ExecuteQueryScalars<T> ()
		{
			if (_conn.Trace) {
				_conn.Tracer?.Invoke ("Executing Query: " + this);
			}
			//var stmt = Prepare ();
			//try {
			//	if (SQLite3.ColumnCount (stmt) < 1) {
			//		throw new InvalidOperationException ("QueryScalars should return at least one column");
			//	}
			//	while (SQLite3.Step (stmt) == SQLite3.Result.Row) {
			//		var colType = SQLite3.ColumnType (stmt, 0);
			//		var val = ReadCol (stmt, 0, colType, typeof (T));
			//		if (val == null) {
			//			yield return default (T);
			//		}
			//		else {
			//			yield return (T)val;
			//		}
			//	}
			//}
			//finally {
			//	Finalize (stmt);
			//}
			return null;
		}

		//public void Bind (string name, object val)
		//{
		//	_bindings.Add (new Binding {
		//		Name = name,
		//		Value = val
		//	});
		//}

		//public void Bind (object val)
		//{
		//	Bind (null, val);
		//}

		public override string ToString ()
		{
			var parts = new string[1 + _bindings.Count];
			parts[0] = CommandText;
			var i = 1;
			foreach (var b in _bindings) {
				parts[i] = string.Format ("  {0}: {1}", i - 1, b.Value);
				i++;
			}
			return string.Join (Environment.NewLine, parts);
		}

		SqlCommand Prepare (SqlCommand cmd)
		{
			cmd.Prepare ();
			return cmd;
			//var stmt = SQLite3.Prepare2 (_conn.Handle, CommandText);
			//BindAll (stmt);
			//return stmt;
		}

		void Finalize (SqlCommand cmd)
		{
			cmd.Dispose ();
			//SQLite3.Finalize (stmt);
		}

		void BindAll (Sqlite3Statement stmt)
		{
			int nextIdx = 1;
			foreach (var b in _bindings) {
				if (b.Name != null) {
					b.Index = SQLite3.BindParameterIndex (stmt, b.Name);
				}
				else {
					b.Index = nextIdx++;
				}

				BindParameter (stmt, b.Index, b.Value);
			}
		}

		static IntPtr NegativePointer = new IntPtr (-1);

		internal static void BindParameter (Sqlite3Statement stmt, int index, object value)
		{
			if (value == null) {
				SQLite3.BindNull (stmt, index);
			}
			else {
				if (value is Int32) {
					SQLite3.BindInt (stmt, index, (int)value);
				}
				else if (value is String) {
					SQLite3.BindText (stmt, index, (string)value, -1, NegativePointer);
				}
				else if (value is Byte || value is UInt16 || value is SByte || value is Int16) {
					SQLite3.BindInt (stmt, index, Convert.ToInt32 (value));
				}
				else if (value is Boolean) {
					SQLite3.BindInt (stmt, index, (bool)value ? 1 : 0);
				}
				else if (value is UInt32 || value is Int64) {
					SQLite3.BindInt64 (stmt, index, Convert.ToInt64 (value));
				}
				else if (value is Single || value is Double || value is Decimal) {
					SQLite3.BindDouble (stmt, index, Convert.ToDouble (value));
				}
				else if (value is TimeSpan) {
					SQLite3.BindText (stmt, index, ((TimeSpan)value).ToString (), -1, NegativePointer);
				}
				else if (value is DateTime) {
					SQLite3.BindText (stmt, index, ((DateTime)value).ToString (System.Globalization.CultureInfo.InvariantCulture), -1, NegativePointer);
				}
				else if (value is DateTimeOffset) {
					SQLite3.BindInt64 (stmt, index, ((DateTimeOffset)value).UtcTicks);
				}
				else if (value is byte[]) {
					SQLite3.BindBlob (stmt, index, (byte[])value, ((byte[])value).Length, NegativePointer);
				}
				else if (value is Guid) {
					SQLite3.BindText (stmt, index, ((Guid)value).ToString (), 72, NegativePointer);
				}
				else if (value is Uri) {
					SQLite3.BindText (stmt, index, ((Uri)value).ToString (), -1, NegativePointer);
				}
				else if (value is StringBuilder) {
					SQLite3.BindText (stmt, index, ((StringBuilder)value).ToString (), -1, NegativePointer);
				}
				else if (value is UriBuilder) {
					SQLite3.BindText (stmt, index, ((UriBuilder)value).ToString (), -1, NegativePointer);
				}
				else {
					// Now we could possibly get an enum, retrieve cached info
					var valueType = value.GetType ();
					var enumInfo = EnumCache.GetInfo (valueType);
					if (enumInfo.IsEnum) {
						var enumIntValue = Convert.ToInt32 (value);
						if (enumInfo.StoreAsText)
							SQLite3.BindText (stmt, index, enumInfo.EnumValues[enumIntValue], -1, NegativePointer);
						else
							SQLite3.BindInt (stmt, index, enumIntValue);
					}
					else {
						throw new NotSupportedException ("Cannot store type: " + Orm.GetType (value));
					}
				}
			}
		}

		class Binding
		{
			public string Name { get; set; }

			public object Value { get; set; }

			public int Index { get; set; }
		}

		object ReadCol (SqlDataReader reader, int index, Type coltype, Type clrType)
		{
			if (coltype == typeof (DBNull)) {
				return null;
			}
			else {
				var clrTypeInfo = clrType.GetTypeInfo ();
				if (clrTypeInfo.IsGenericType && clrTypeInfo.GetGenericTypeDefinition () == typeof (Nullable<>)) {
					clrType = clrTypeInfo.GenericTypeArguments[0];
					clrTypeInfo = clrType.GetTypeInfo ();
				}

				if (clrType == typeof (String)) {
					return Convert.ToString (reader[index]);
				}
				else if (clrType == typeof (Int32)) {
					return (int)Convert.ToInt64 (reader[index]);
				}
				else if (clrType == typeof (Boolean)) {
					return Convert.ToBoolean (reader[index]);
				}
				else if (clrType == typeof (double)) {
					return Convert.ToDouble (reader[index]);
				}
				else if (clrType == typeof (float)) {
					return (float)Convert.ToDouble (reader[index]);
				}
				else if (clrType == typeof (TimeSpan)) {

					var text = Convert.ToString (reader[index]);
					TimeSpan resultTime;
					if (!TimeSpan.TryParseExact (text, "c", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.TimeSpanStyles.None, out resultTime)) {
						resultTime = TimeSpan.Parse (text);
					}
					return resultTime;

				}
				else if (clrType == typeof (DateTime)) {
					var text = Convert.ToString (reader[index]);
					DateTime resultDate = DateTime.Now;

					//if (!DateTime.TryParseExact (text, System.Globalization.CultureInfo.InvariantCulture, out resultDate)) {
					//	resultDate = DateTime.Parse (text);
					//}
					return resultDate;

				}
				else if (clrType == typeof (DateTimeOffset)) {
					return new DateTimeOffset (Convert.ToInt64 (reader[index]), TimeSpan.Zero);
				}
				else if (clrTypeInfo.IsEnum) {
					if (coltype == typeof (String)) {
						var value = Convert.ToString (reader[index]);
						return Enum.Parse (clrType, value.ToString (), true);
					}
					else
						return Convert.ToInt32 (reader[index]);
				}
				else if (clrType == typeof (Int64)) {
					return Convert.ToInt64 (reader[index]);
				}
				else if (clrType == typeof (UInt32)) {
					return (uint)Convert.ToInt32 (reader[index]);
				}
				else if (clrType == typeof (decimal)) {
					return (decimal)Convert.ToDouble (reader[index]);
				}
				else if (clrType == typeof (Byte)) {
					return (byte)Convert.ToInt32 (reader[index]);
				}
				else if (clrType == typeof (UInt16)) {
					return (ushort)Convert.ToInt32 (reader[index]);
				}
				else if (clrType == typeof (Int16)) {
					return (short)Convert.ToInt32 (reader[index]);
				}
				else if (clrType == typeof (sbyte)) {
					return (sbyte)Convert.ToInt32 (reader[index]);
				}
				else if (clrType == typeof (byte[])) {
					return (byte[])(reader[index]);
				}
				else if (clrType == typeof (Guid)) {
					var text = Convert.ToString (reader[index]);
					return new Guid (text);
				}
				else if (clrType == typeof (Uri)) {
					var text = Convert.ToString (reader[index]);
					return new Uri (text);
				}
				else if (clrType == typeof (StringBuilder)) {
					var text = Convert.ToString (reader[index]);
					return new StringBuilder (text);
				}
				else if (clrType == typeof (UriBuilder)) {
					var text = Convert.ToString (reader[index]);
					return new UriBuilder (text);
				}
				else {
					throw new NotSupportedException ("Don't know how to read " + clrType);
				}
			}
		}
	}

	internal class FastColumnSetter
	{
		/// <summary>
		/// Creates a delegate that can be used to quickly set object members from query columns.
		///
		/// Note that this frontloads the slow reflection-based type checking for columns to only happen once at the beginning of a query,
		/// and then afterwards each row of the query can invoke the delegate returned by this function to get much better performance (up to 10x speed boost, depending on query size and platform).
		/// </summary>
		/// <typeparam name="T">The type of the destination object that the query will read into</typeparam>
		/// <param name="conn">The active connection.  Note that this is primarily needed in order to read preferences regarding how certain data types (such as TimeSpan / DateTime) should be encoded in the database.</param>
		/// <param name="column">The table mapping used to map the statement column to a member of the destination object type</param>
		/// <returns>
		/// A delegate for fast-setting of object members from statement columns.
		///
		/// If no fast setter is available for the requested column (enums in particular cause headache), then this function returns null.
		/// </returns>
		internal static Action<T, SqlDataReader, int> GetFastSetter<T> (SQLServerConnection conn, TableMapping.Column column)
		{
			Action<T, SqlDataReader, int> fastSetter = null;
			if (column.PropertyInfo is null) {
				return null;
			}
			Type clrType = column.PropertyInfo.PropertyType;

			var clrTypeInfo = clrType.GetTypeInfo ();
			if (clrTypeInfo.IsGenericType && clrTypeInfo.GetGenericTypeDefinition () == typeof (Nullable<>)) {
				clrType = clrTypeInfo.GenericTypeArguments[0];
				clrTypeInfo = clrType.GetTypeInfo ();
			}

			if (clrType == typeof (String)) {
				fastSetter = CreateTypedSetterDelegate<T, string> (column, (reader, index) => {
					return Convert.ToString (reader[index]);
				});
			}
			else if (clrType == typeof (Int32)) {
				fastSetter = CreateNullableTypedSetterDelegate<T, int> (column, (reader, index) => {
					return Convert.ToInt32 (reader[index]);
				});
			}
			else if (clrType == typeof (Boolean)) {
				fastSetter = CreateNullableTypedSetterDelegate<T, bool> (column, (reader, index) => {
					return Convert.ToBoolean (reader[index]);
				});
			}
			else if (clrType == typeof (double)) {
				fastSetter = CreateNullableTypedSetterDelegate<T, double> (column, (reader, index) => {
					return Convert.ToDouble (reader[index]);
				});
			}
			else if (clrType == typeof (float)) {
				fastSetter = CreateNullableTypedSetterDelegate<T, float> (column, (reader, index) => {
					return (float)Convert.ToDouble (reader[index]);
				});
			}
			else if (clrType == typeof (TimeSpan)) {

				fastSetter = CreateNullableTypedSetterDelegate<T, TimeSpan> (column, (reader, index) => {
					var text = Convert.ToString (reader[index]);
					TimeSpan resultTime;
					if (!TimeSpan.TryParseExact (text, "c", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.TimeSpanStyles.None, out resultTime)) {
						resultTime = TimeSpan.Parse (text);
					}
					return resultTime;
				});

			}
			else if (clrType == typeof (DateTime)) {

				fastSetter = CreateNullableTypedSetterDelegate<T, DateTime> (column, (reader, index) => {
					var text = Convert.ToString (reader[index]);
					DateTime resultDate = DateTime.Now;
					//if (!DateTime.TryParseExact (text, "", System.Globalization.CultureInfo.InvariantCulture, out resultDate)) {
					//	resultDate = DateTime.Parse (text);
					//}
					return resultDate;
				});

			}
			else if (clrType == typeof (DateTimeOffset)) {
				fastSetter = CreateNullableTypedSetterDelegate<T, DateTimeOffset> (column, (reader, index) => {
					return new DateTimeOffset (Convert.ToInt64 (reader[index]), TimeSpan.Zero);
				});
			}
			else if (clrTypeInfo.IsEnum) {
				// NOTE: Not sure of a good way (if any?) to do a strongly-typed fast setter like this for enumerated types -- for now, return null and column sets will revert back to the safe (but slow) Reflection-based method of column prop.Set()
			}
			else if (clrType == typeof (Int64)) {
				fastSetter = CreateNullableTypedSetterDelegate<T, Int64> (column, (reader, index) => {
					return Convert.ToInt64 (reader[index]);
				});
			}
			else if (clrType == typeof (UInt32)) {
				fastSetter = CreateNullableTypedSetterDelegate<T, UInt32> (column, (reader, index) => {
					return (uint)Convert.ToInt64 (reader[index]);
				});
			}
			else if (clrType == typeof (decimal)) {
				fastSetter = CreateNullableTypedSetterDelegate<T, decimal> (column, (reader, index) => {
					return (decimal)reader[index];
				});
			}
			else if (clrType == typeof (Byte)) {
				fastSetter = CreateNullableTypedSetterDelegate<T, Byte> (column, (reader, index) => {
					return (byte)reader[index];
				});
			}
			else if (clrType == typeof (UInt16)) {
				fastSetter = CreateNullableTypedSetterDelegate<T, UInt16> (column, (reader, index) => {
					return (ushort)reader[index];
				});
			}
			else if (clrType == typeof (Int16)) {
				fastSetter = CreateNullableTypedSetterDelegate<T, Int16> (column, (reader, index) => {
					return (short)reader[index];
				});
			}
			else if (clrType == typeof (sbyte)) {
				fastSetter = CreateNullableTypedSetterDelegate<T, sbyte> (column, (reader, index) => {
					return (sbyte)reader[index];
				});
			}
			else if (clrType == typeof (byte[])) {
				fastSetter = CreateTypedSetterDelegate<T, byte[]> (column, (reader, index) => {
					return (byte[])reader[index];
				});
			}
			else if (clrType == typeof (Guid)) {
				fastSetter = CreateNullableTypedSetterDelegate<T, Guid> (column, (reader, index) => {
					var text = Convert.ToString (reader[index]);
					return new Guid (text);
				});
			}
			else if (clrType == typeof (Uri)) {
				fastSetter = CreateTypedSetterDelegate<T, Uri> (column, (reader, index) => {
					var text = Convert.ToString (reader[index]);
					return new Uri (text);
				});
			}
			else if (clrType == typeof (StringBuilder)) {
				fastSetter = CreateTypedSetterDelegate<T, StringBuilder> (column, (reader, index) => {
					var text = Convert.ToString (reader[index]);
					return new StringBuilder (text);
				});
			}
			else if (clrType == typeof (UriBuilder)) {
				fastSetter = CreateTypedSetterDelegate<T, UriBuilder> (column, (reader, index) => {
					var text = Convert.ToString (reader[index]);
					return new UriBuilder (text);
				});
			}
			else {
				// NOTE: Will fall back to the slow setter method in the event that we are unable to create a fast setter delegate for a particular column type
			}
			return fastSetter;
		}

		/// <summary>
		/// This creates a strongly typed delegate that will permit fast setting of column values given a Sqlite3Statement and a column index.
		///
		/// Note that this is identical to CreateTypedSetterDelegate(), but has an extra check to see if it should create a nullable version of the delegate.
		/// </summary>
		/// <typeparam name="ObjectType">The type of the object whose member column is being set</typeparam>
		/// <typeparam name="ColumnMemberType">The CLR type of the member in the object which corresponds to the given SQLite columnn</typeparam>
		/// <param name="column">The column mapping that identifies the target member of the destination object</param>
		/// <param name="getColumnValue">A lambda that can be used to retrieve the column value at query-time</param>
		/// <returns>A strongly-typed delegate</returns>
		private static Action<ObjectType, SqlDataReader, int> CreateNullableTypedSetterDelegate<ObjectType, ColumnMemberType> (TableMapping.Column column, Func<SqlDataReader, int, ColumnMemberType> getColumnValue) where ColumnMemberType : struct
		{
			var clrTypeInfo = column.PropertyInfo.PropertyType.GetTypeInfo ();
			bool isNullable = false;

			if (clrTypeInfo.IsGenericType && clrTypeInfo.GetGenericTypeDefinition () == typeof (Nullable<>)) {
				isNullable = true;
			}

			if (isNullable) {
				var setProperty = (Action<ObjectType, ColumnMemberType?>)Delegate.CreateDelegate (
						typeof (Action<ObjectType, ColumnMemberType?>), null,
						column.PropertyInfo.GetSetMethod ());

				return (o, reader, i) => {
					var colType = reader.GetFieldType (i);
					//if (colType != SQLite3.ColType.Null)
					//	setProperty.Invoke (o, getColumnValue.Invoke (stmt, i));
				};
			}

			return CreateTypedSetterDelegate<ObjectType, ColumnMemberType> (column, getColumnValue);
		}

		/// <summary>
		/// This creates a strongly typed delegate that will permit fast setting of column values given a Sqlite3Statement and a column index.
		/// </summary>
		/// <typeparam name="ObjectType">The type of the object whose member column is being set</typeparam>
		/// <typeparam name="ColumnMemberType">The CLR type of the member in the object which corresponds to the given SQLite columnn</typeparam>
		/// <param name="column">The column mapping that identifies the target member of the destination object</param>
		/// <param name="getColumnValue">A lambda that can be used to retrieve the column value at query-time</param>
		/// <returns>A strongly-typed delegate</returns>
		private static Action<ObjectType, SqlDataReader, int> CreateTypedSetterDelegate<ObjectType, ColumnMemberType> (TableMapping.Column column, Func<SqlDataReader, int, ColumnMemberType> getColumnValue)
		{
			var setProperty = (Action<ObjectType, ColumnMemberType>)Delegate.CreateDelegate (
					typeof (Action<ObjectType, ColumnMemberType>), null,
					column.PropertyInfo.GetSetMethod ());

			return (o, reader, i) => {
				var colType = reader.GetFieldType (i);
				if (colType != typeof (DBNull))
					setProperty.Invoke (o, getColumnValue.Invoke (reader, i));
			};
		}
	}

	/// <summary>
	/// Since the insert never changed, we only need to prepare once.
	/// </summary>
	class PreparedSqlServerInsertCommand : IDisposable
	{
		bool Initialized;

		SQLServerConnection Connection;

		string CommandText;

		Sqlite3Statement Statement;
		static readonly Sqlite3Statement NullStatement = default (Sqlite3Statement);

		public PreparedSqlServerInsertCommand (SQLServerConnection conn, string commandText)
		{
			Connection = conn;
			CommandText = commandText;
		}

		public long ExecuteNonQueryAndRecoverLastScopeIdentity (SqlParameter[] source)
		{
			int execute_result = -1;
			long last_scope_identity = -1;

			if (Initialized && Statement == NullStatement) {
				throw new ObjectDisposedException (nameof (PreparedSqlServerInsertCommand));
			}

			if (Connection.Trace) {
				Connection.Tracer?.Invoke ("Executing: " + CommandText);
			}

			if (Connection.IsClosed) {
				Connection.RenewConnection ();
			}
			using (var con = Connection.Connection) {
				con.Open ();
				using (var cmd = new SqlCommand (this.CommandText + " select SCOPE_IDENTITY();", con)) {
					if (source?.Any () ?? false) {
						cmd.Parameters.AddRange (source);
					}

					using (var reader = cmd.ExecuteReader ()) {
						if (reader.Read ()) {
							return Convert.ToInt64 (reader[0]);
						}
					}
				}
			}

			return 0;
		}

		private long LastInsertRowid (SqlConnection con)
		{
			using (SqlCommand cmd = new SqlCommand ("select SCOPE_IDENTITY()", con)) {
				using (SqlDataReader reader = cmd.ExecuteReader ()) {
					if (reader.Read ()) {
						return Convert.ToInt64 (reader[0]);
					}
				}

				return 0;
			}
		}

		public int ExecuteNonQuery (SqlParameter[] source)
		{
			if (Initialized && Statement == NullStatement) {
				throw new ObjectDisposedException (nameof (PreparedSqlServerInsertCommand));
			}

			if (Connection.Trace) {
				Connection.Tracer?.Invoke ("Executing: " + CommandText);
			}

			if (Connection.IsClosed) {
				Connection.RenewConnection ();
			}

			using (var con = Connection.Connection) {
				con.Open ();
				using (var cmd = new SqlCommand (this.CommandText, con)) {
					if (source?.Any () ?? false) {
						cmd.Parameters.AddRange (source);
					}
					return cmd.ExecuteNonQuery ();
				}
			}
		}

		public void Dispose ()
		{
			Dispose (true);
			GC.SuppressFinalize (this);
		}

		void Dispose (bool disposing)
		{
			var s = Statement;
			Statement = NullStatement;
			Connection = null;
			if (s != NullStatement) {
				SQLite3.Finalize (s);
			}
		}

		~PreparedSqlServerInsertCommand ()
		{
			Dispose (false);
		}
	}

	public enum CreateTableResult
	{
		Created,
		Migrated,
	}

	public class CreateTablesResult
	{
		public Dictionary<Type, CreateTableResult> Results { get; private set; }

		public CreateTablesResult ()
		{
			Results = new Dictionary<Type, CreateTableResult> ();
		}
	}

	public abstract class BaseTableQuery
	{
		protected class Ordering
		{
			public string ColumnName { get; set; }
			public bool Ascending { get; set; }
		}
	}

	public class TableQuery<T> : BaseTableQuery, IEnumerable<T>
	{
		public SQLServerConnection Connection { get; private set; }

		public TableMapping Table { get; private set; }

		Expression _where;
		List<Ordering> _orderBys;
		int? _limit;
		int? _offset;

		BaseTableQuery _joinInner;
		Expression _joinInnerKeySelector;
		BaseTableQuery _joinOuter;
		Expression _joinOuterKeySelector;
		Expression _joinSelector;

		Expression _selector;

		TableQuery (SQLServerConnection conn, TableMapping table)
		{
			Connection = conn;
			Table = table;
		}

		public TableQuery (SQLServerConnection conn)
		{
			Connection = conn;
			Table = Connection.GetMapping (typeof (T));
		}

		public TableQuery<U> Clone<U> ()
		{
			var q = new TableQuery<U> (Connection, Table);
			q._where = _where;
			q._deferred = _deferred;
			if (_orderBys != null) {
				q._orderBys = new List<Ordering> (_orderBys);
			}
			q._limit = _limit;
			q._offset = _offset;
			q._joinInner = _joinInner;
			q._joinInnerKeySelector = _joinInnerKeySelector;
			q._joinOuter = _joinOuter;
			q._joinOuterKeySelector = _joinOuterKeySelector;
			q._joinSelector = _joinSelector;
			q._selector = _selector;
			return q;
		}

		/// <summary>
		/// Filters the query based on a predicate.
		/// </summary>
		public TableQuery<T> Where (Expression<Func<T, bool>> predExpr)
		{
			if (predExpr.NodeType == ExpressionType.Lambda) {
				var lambda = (LambdaExpression)predExpr;
				var pred = lambda.Body;
				var q = Clone<T> ();
				q.AddWhere (pred);
				return q;
			}
			else {
				throw new NotSupportedException ("Must be a predicate");
			}
		}

		/// <summary>
		/// Delete all the rows that match this query.
		/// </summary>
		public int Delete ()
		{
			return Delete (null);
		}

		/// <summary>
		/// Delete all the rows that match this query and the given predicate.
		/// </summary>
		public int Delete (Expression<Func<T, bool>> predExpr)
		{
			if (_limit.HasValue || _offset.HasValue)
				throw new InvalidOperationException ("Cannot delete with limits or offsets");

			if (_where == null && predExpr == null)
				throw new InvalidOperationException ("No condition specified");

			var pred = _where;

			if (predExpr != null && predExpr.NodeType == ExpressionType.Lambda) {
				var lambda = (LambdaExpression)predExpr;
				pred = pred != null ? Expression.AndAlso (pred, lambda.Body) : lambda.Body;
			}

			var args = new List<SqlParameter> ();
			var cmdText = "delete from \"" + Table.TableName + "\"";
			var w = CompileExpr (pred, args, null);
			cmdText += " where " + w.CommandText;

			var command = Connection.CreateCommand (cmdText, args.ToArray ());

			int result = command.ExecuteNonQuery ();
			return result;
		}

		/// <summary>
		/// Yields a given number of elements from the query and then skips the remainder.
		/// </summary>
		public TableQuery<T> Take (int n)
		{
			var q = Clone<T> ();
			q._limit = n;
			return q;
		}

		/// <summary>
		/// Skips a given number of elements from the query and then yields the remainder.
		/// </summary>
		public TableQuery<T> Skip (int n)
		{
			var q = Clone<T> ();
			q._offset = n;
			return q;
		}

		/// <summary>
		/// Returns the element at a given index
		/// </summary>
		public T ElementAt (int index)
		{
			return Skip (index).Take (1).First ();
		}

		bool _deferred;
		public TableQuery<T> Deferred ()
		{
			var q = Clone<T> ();
			q._deferred = true;
			return q;
		}

		/// <summary>
		/// Order the query results according to a key.
		/// </summary>
		public TableQuery<T> OrderBy<U> (Expression<Func<T, U>> orderExpr)
		{
			return AddOrderBy<U> (orderExpr, true);
		}

		/// <summary>
		/// Order the query results according to a key.
		/// </summary>
		public TableQuery<T> OrderByDescending<U> (Expression<Func<T, U>> orderExpr)
		{
			return AddOrderBy<U> (orderExpr, false);
		}

		/// <summary>
		/// Order the query results according to a key.
		/// </summary>
		public TableQuery<T> ThenBy<U> (Expression<Func<T, U>> orderExpr)
		{
			return AddOrderBy<U> (orderExpr, true);
		}

		/// <summary>
		/// Order the query results according to a key.
		/// </summary>
		public TableQuery<T> ThenByDescending<U> (Expression<Func<T, U>> orderExpr)
		{
			return AddOrderBy<U> (orderExpr, false);
		}

		TableQuery<T> AddOrderBy<U> (Expression<Func<T, U>> orderExpr, bool asc)
		{
			if (orderExpr.NodeType == ExpressionType.Lambda) {
				var lambda = (LambdaExpression)orderExpr;

				MemberExpression mem = null;

				var unary = lambda.Body as UnaryExpression;
				if (unary != null && unary.NodeType == ExpressionType.Convert) {
					mem = unary.Operand as MemberExpression;
				}
				else {
					mem = lambda.Body as MemberExpression;
				}

				if (mem != null && (mem.Expression.NodeType == ExpressionType.Parameter)) {
					var q = Clone<T> ();
					if (q._orderBys == null) {
						q._orderBys = new List<Ordering> ();
					}
					q._orderBys.Add (new Ordering {
						ColumnName = Table.FindColumnWithPropertyName (mem.Member.Name).Name,
						Ascending = asc
					});
					return q;
				}
				else {
					throw new NotSupportedException ("Order By does not support: " + orderExpr);
				}
			}
			else {
				throw new NotSupportedException ("Must be a predicate");
			}
		}

		private void AddWhere (Expression pred)
		{
			if (_where == null) {
				_where = pred;
			}
			else {
				_where = Expression.AndAlso (_where, pred);
			}
		}

		///// <summary>
		///// Performs an inner join of two queries based on matching keys extracted from the elements.
		///// </summary>
		//public TableQuery<TResult> Join<TInner, TKey, TResult> (
		//	TableQuery<TInner> inner,
		//	Expression<Func<T, TKey>> outerKeySelector,
		//	Expression<Func<TInner, TKey>> innerKeySelector,
		//	Expression<Func<T, TInner, TResult>> resultSelector)
		//{
		//	var q = new TableQuery<TResult> (Connection, Connection.GetMapping (typeof (TResult))) {
		//		_joinOuter = this,
		//		_joinOuterKeySelector = outerKeySelector,
		//		_joinInner = inner,
		//		_joinInnerKeySelector = innerKeySelector,
		//		_joinSelector = resultSelector,
		//	};
		//	return q;
		//}

		// Not needed until Joins are supported
		// Keeping this commented out forces the default Linq to objects processor to run
		//public TableQuery<TResult> Select<TResult> (Expression<Func<T, TResult>> selector)
		//{
		//	var q = Clone<TResult> ();
		//	q._selector = selector;
		//	return q;
		//}

		private SQLServerCommand GenerateCommand (string selectionList)
		{
			if (_joinInner != null && _joinOuter != null) {
				throw new NotSupportedException ("Joins are not supported.");
			}
			else {
				var cmdText = "select " + selectionList + " from \"" + Table.TableName + "\"";
				var args = new List<SqlParameter> ();
				if (_where != null) {
					var w = CompileExpr (_where, args, null);
					cmdText += " where " + w.CommandText;
				}
				if ((_orderBys != null) && (_orderBys.Count > 0)) {
					var t = string.Join (", ", _orderBys.Select (o => "\"" + o.ColumnName + "\"" + (o.Ascending ? "" : " desc")).ToArray ());
					cmdText += " order by " + t;
				}
				if (_limit.HasValue) {
					cmdText += " limit " + _limit.Value;
				}
				if (_offset.HasValue) {
					if (!_limit.HasValue) {
						cmdText += " limit -1 ";
					}
					cmdText += " offset " + _offset.Value;
				}
				return Connection.CreateCommand (cmdText, args.ToArray ());
			}
		}

		class CompileResult
		{
			public string CommandText { get; set; }

			public object Value { get; set; }
		}

		private CompileResult CompileExpr (Expression expr, List<SqlParameter> queryArgs, string leftrname)
		{
			if (expr == null) {
				throw new NotSupportedException ("Expression is NULL");
			}
			else if (expr is BinaryExpression) {
				var bin = (BinaryExpression)expr;

				// VB turns 'x=="foo"' into 'CompareString(x,"foo",true/false)==0', so we need to unwrap it
				// http://blogs.msdn.com/b/vbteam/archive/2007/09/18/vb-expression-trees-string-comparisons.aspx
				if (bin.Left.NodeType == ExpressionType.Call) {
					var call = (MethodCallExpression)bin.Left;
					if (call.Method.DeclaringType.FullName == "Microsoft.VisualBasic.CompilerServices.Operators"
						&& call.Method.Name == "CompareString")
						bin = Expression.MakeBinary (bin.NodeType, call.Arguments[0], call.Arguments[1]);
				}


				var leftr = CompileExpr (bin.Left, queryArgs, null);
				var rightr = CompileExpr (bin.Right, queryArgs, leftr.CommandText);

				//If either side is a parameter and is null, then handle the other side specially (for "is null"/"is not null")
				string text;
				if (leftr.CommandText == "?" && leftr.Value == null)
					text = CompileNullBinaryExpression (bin, rightr);
				else if (rightr.CommandText == "?" && rightr.Value == null)
					text = CompileNullBinaryExpression (bin, leftr);
				else
					text = "(" + leftr.CommandText + " " + GetSqlName (bin) + " " + rightr.CommandText + ")";
				return new CompileResult { CommandText = text };
			}
			else if (expr.NodeType == ExpressionType.Not) {
				var operandExpr = ((UnaryExpression)expr).Operand;
				var opr = CompileExpr (operandExpr, queryArgs, leftrname);
				object val = opr.Value;
				if (val is bool)
					val = !((bool)val);
				return new CompileResult {
					CommandText = "NOT(" + opr.CommandText + ")",
					Value = val
				};
			}
			else if (expr.NodeType == ExpressionType.Call) {

				var call = (MethodCallExpression)expr;
				var args = new CompileResult[call.Arguments.Count];
				var obj = call.Object != null ? CompileExpr (call.Object, queryArgs, leftrname) : null;

				for (var i = 0; i < args.Length; i++) {
					args[i] = CompileExpr (call.Arguments[i], queryArgs, leftrname);
				}

				var sqlCall = "";

				if (call.Method.Name == "Like" && args.Length == 2) {
					sqlCall = "(" + args[0].CommandText + " like " + args[1].CommandText + ")";
				}
				else if (call.Method.Name == "Contains" && args.Length == 2) {
					sqlCall = "(" + args[1].CommandText + " in " + args[0].CommandText + ")";
				}
				else if (call.Method.Name == "Contains" && args.Length == 1) {
					if (call.Object != null && call.Object.Type == typeof (string)) {
						sqlCall = "( instr(" + obj.CommandText + "," + args[0].CommandText + ") >0 )";
					}
					else {
						sqlCall = "(" + args[0].CommandText + " in " + obj.CommandText + ")";
					}
				}
				else if (call.Method.Name == "StartsWith" && args.Length >= 1) {
					var startsWithCmpOp = StringComparison.CurrentCulture;
					if (args.Length == 2) {
						startsWithCmpOp = (StringComparison)args[1].Value;
					}
					switch (startsWithCmpOp) {
						case StringComparison.Ordinal:
						case StringComparison.CurrentCulture:
							sqlCall = "( substr(" + obj.CommandText + ", 1, " + args[0].Value.ToString ().Length + ") =  " + args[0].CommandText + ")";
							break;
						case StringComparison.OrdinalIgnoreCase:
						case StringComparison.CurrentCultureIgnoreCase:
							sqlCall = "(" + obj.CommandText + " like (" + args[0].CommandText + " || '%'))";
							break;
					}

				}
				else if (call.Method.Name == "EndsWith" && args.Length >= 1) {
					var endsWithCmpOp = StringComparison.CurrentCulture;
					if (args.Length == 2) {
						endsWithCmpOp = (StringComparison)args[1].Value;
					}
					switch (endsWithCmpOp) {
						case StringComparison.Ordinal:
						case StringComparison.CurrentCulture:
							sqlCall = "( substr(" + obj.CommandText + ", length(" + obj.CommandText + ") - " + args[0].Value.ToString ().Length + "+1, " + args[0].Value.ToString ().Length + ") =  " + args[0].CommandText + ")";
							break;
						case StringComparison.OrdinalIgnoreCase:
						case StringComparison.CurrentCultureIgnoreCase:
							sqlCall = "(" + obj.CommandText + " like ('%' || " + args[0].CommandText + "))";
							break;
					}
				}
				else if (call.Method.Name == "Equals" && args.Length == 1) {
					sqlCall = "(" + obj.CommandText + " = (" + args[0].CommandText + "))";
				}
				else if (call.Method.Name == "ToLower") {
					sqlCall = "(lower(" + obj.CommandText + "))";
				}
				else if (call.Method.Name == "ToUpper") {
					sqlCall = "(upper(" + obj.CommandText + "))";
				}
				else if (call.Method.Name == "Replace" && args.Length == 2) {
					sqlCall = "(replace(" + obj.CommandText + "," + args[0].CommandText + "," + args[1].CommandText + "))";
				}
				else if (call.Method.Name == "IsNullOrEmpty" && args.Length == 1) {
					sqlCall = "(" + args[0].CommandText + " is null or" + args[0].CommandText + " ='' )";
				}
				else {
					sqlCall = call.Method.Name.ToLower () + "(" + string.Join (",", args.Select (a => a.CommandText).ToArray ()) + ")";
				}
				return new CompileResult { CommandText = sqlCall };

			}
			else if (expr.NodeType == ExpressionType.Constant) {
				var c = (ConstantExpression)expr;
				queryArgs.Add (new SqlParameter ("@" + leftrname, c.Value));
				return new CompileResult {
					CommandText = "@" + leftrname,
					Value = c.Value
				};
			}
			else if (expr.NodeType == ExpressionType.Convert) {
				var u = (UnaryExpression)expr;
				var ty = u.Type;
				var valr = CompileExpr (u.Operand, queryArgs, leftrname);
				return new CompileResult {
					CommandText = valr.CommandText,
					Value = valr.Value != null ? ConvertTo (valr.Value, ty) : null
				};
			}
			else if (expr.NodeType == ExpressionType.MemberAccess) {
				var mem = (MemberExpression)expr;

				var paramExpr = mem.Expression as ParameterExpression;
				if (paramExpr == null) {
					var convert = mem.Expression as UnaryExpression;
					if (convert != null && convert.NodeType == ExpressionType.Convert) {
						paramExpr = convert.Operand as ParameterExpression;
					}
				}

				if (paramExpr != null) {
					//
					// This is a column of our table, output just the column name
					// Need to translate it if that column name is mapped
					//
					var columnName = Table.FindColumnWithPropertyName (mem.Member.Name).Name;
					//return new CompileResult { CommandText = "\"" + columnName + "\"" };
					return new CompileResult { CommandText = columnName };
				}
				else {
					object obj = null;
					if (mem.Expression != null) {
						var r = CompileExpr (mem.Expression, queryArgs, leftrname);
						if (r.Value == null) {
							throw new NotSupportedException ("Member access failed to compile expression");
						}
						if (r.CommandText.StartsWith ("@")) {
							queryArgs.RemoveAt (queryArgs.Count - 1);
						}
						obj = r.Value;
					}

					//
					// Get the member value
					//
					object val = null;

					if (mem.Member is PropertyInfo) {
						var m = (PropertyInfo)mem.Member;
						val = m.GetValue (obj, null);
					}
					else if (mem.Member is FieldInfo) {
						var m = (FieldInfo)mem.Member;
						val = m.GetValue (obj);
					}
					else {
						throw new NotSupportedException ("MemberExpr: " + mem.Member.GetType ());
					}

					//
					// Work special magic for enumerables
					//
					if (val != null && val is System.Collections.IEnumerable && !(val is string) && !(val is System.Collections.Generic.IEnumerable<byte>)) {
						var sb = new System.Text.StringBuilder ();
						sb.Append ("(");
						var head = "";
						foreach (object a in (System.Collections.IEnumerable)val) {
							queryArgs.Add (new SqlParameter ("?what", a));
							sb.Append (head);
							sb.Append ("?");
							head = ",";
						}
						sb.Append (")");
						return new CompileResult {
							CommandText = sb.ToString (),
							Value = val
						};
					}
					else {
						queryArgs.Add (new SqlParameter ("@" + leftrname, val));
						return new CompileResult {
							CommandText = "@" + leftrname,
							Value = val
						};
					}
				}
			}
			throw new NotSupportedException ("Cannot compile: " + expr.NodeType.ToString ());
		}

		static object ConvertTo (object obj, Type t)
		{
			Type nut = Nullable.GetUnderlyingType (t);

			if (nut != null) {
				if (obj == null)
					return null;
				return Convert.ChangeType (obj, nut);
			}
			else {
				return Convert.ChangeType (obj, t);
			}
		}

		/// <summary>
		/// Compiles a BinaryExpression where one of the parameters is null.
		/// </summary>
		/// <param name="expression">The expression to compile</param>
		/// <param name="parameter">The non-null parameter</param>
		private string CompileNullBinaryExpression (BinaryExpression expression, CompileResult parameter)
		{
			if (expression.NodeType == ExpressionType.Equal)
				return "(" + parameter.CommandText + " is ?)";
			else if (expression.NodeType == ExpressionType.NotEqual)
				return "(" + parameter.CommandText + " is not ?)";
			else if (expression.NodeType == ExpressionType.GreaterThan
				|| expression.NodeType == ExpressionType.GreaterThanOrEqual
				|| expression.NodeType == ExpressionType.LessThan
				|| expression.NodeType == ExpressionType.LessThanOrEqual)
				return "(" + parameter.CommandText + " < ?)"; // always false
			else
				throw new NotSupportedException ("Cannot compile Null-BinaryExpression with type " + expression.NodeType.ToString ());
		}

		string GetSqlName (Expression expr)
		{
			var n = expr.NodeType;
			if (n == ExpressionType.GreaterThan)
				return ">";
			else if (n == ExpressionType.GreaterThanOrEqual) {
				return ">=";
			}
			else if (n == ExpressionType.LessThan) {
				return "<";
			}
			else if (n == ExpressionType.LessThanOrEqual) {
				return "<=";
			}
			else if (n == ExpressionType.And) {
				return "&";
			}
			else if (n == ExpressionType.AndAlso) {
				return "and";
			}
			else if (n == ExpressionType.Or) {
				return "|";
			}
			else if (n == ExpressionType.OrElse) {
				return "or";
			}
			else if (n == ExpressionType.Equal) {
				return "=";
			}
			else if (n == ExpressionType.NotEqual) {
				return "!=";
			}
			else {
				throw new NotSupportedException ("Cannot get SQL for: " + n);
			}
		}

		/// <summary>
		/// Execute SELECT COUNT(*) on the query
		/// </summary>
		public int Count ()
		{
			return GenerateCommand ("count(*)").ExecuteScalar<int> ();
		}

		/// <summary>
		/// Execute SELECT COUNT(*) on the query with an additional WHERE clause.
		/// </summary>
		public int Count (Expression<Func<T, bool>> predExpr)
		{
			return Where (predExpr).Count ();
		}

		public IEnumerator<T> GetEnumerator ()
		{
			if (!_deferred)
				return GenerateCommand ("*").ExecuteQuery<T> ().GetEnumerator ();

			return GenerateCommand ("*").ExecuteDeferredQuery<T> ().GetEnumerator ();
		}

		System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator ()
		{
			return GetEnumerator ();
		}

		/// <summary>
		/// Queries the database and returns the results as a List.
		/// </summary>
		public List<T> ToList ()
		{
			return GenerateCommand ("*").ExecuteQuery<T> ();
		}

		/// <summary>
		/// Queries the database and returns the results as an array.
		/// </summary>
		public T[] ToArray ()
		{
			return GenerateCommand ("*").ExecuteQuery<T> ().ToArray ();
		}

		/// <summary>
		/// Returns the first element of this query.
		/// </summary>
		public T First ()
		{
			var query = Take (1);
			return query.ToList ().First ();
		}

		/// <summary>
		/// Returns the first element of this query, or null if no element is found.
		/// </summary>
		public T FirstOrDefault ()
		{
			var query = Take (1);
			return query.ToList ().FirstOrDefault ();
		}

		/// <summary>
		/// Returns the first element of this query that matches the predicate.
		/// </summary>
		public T First (Expression<Func<T, bool>> predExpr)
		{
			return Where (predExpr).First ();
		}

		/// <summary>
		/// Returns the first element of this query that matches the predicate, or null
		/// if no element is found.
		/// </summary>
		public T FirstOrDefault (Expression<Func<T, bool>> predExpr)
		{
			return Where (predExpr).FirstOrDefault ();
		}
	}

	public static class SQLite3
	{
		public enum Result : int
		{
			OK = 0,
			Error = 1,
			Internal = 2,
			Perm = 3,
			Abort = 4,
			Busy = 5,
			Locked = 6,
			NoMem = 7,
			ReadOnly = 8,
			Interrupt = 9,
			IOError = 10,
			Corrupt = 11,
			NotFound = 12,
			Full = 13,
			CannotOpen = 14,
			LockErr = 15,
			Empty = 16,
			SchemaChngd = 17,
			TooBig = 18,
			Constraint = 19,
			Mismatch = 20,
			Misuse = 21,
			NotImplementedLFS = 22,
			AccessDenied = 23,
			Format = 24,
			Range = 25,
			NonDBFile = 26,
			Notice = 27,
			Warning = 28,
			Row = 100,
			Done = 101
		}

		public enum ExtendedResult : int
		{
			IOErrorRead = (Result.IOError | (1 << 8)),
			IOErrorShortRead = (Result.IOError | (2 << 8)),
			IOErrorWrite = (Result.IOError | (3 << 8)),
			IOErrorFsync = (Result.IOError | (4 << 8)),
			IOErrorDirFSync = (Result.IOError | (5 << 8)),
			IOErrorTruncate = (Result.IOError | (6 << 8)),
			IOErrorFStat = (Result.IOError | (7 << 8)),
			IOErrorUnlock = (Result.IOError | (8 << 8)),
			IOErrorRdlock = (Result.IOError | (9 << 8)),
			IOErrorDelete = (Result.IOError | (10 << 8)),
			IOErrorBlocked = (Result.IOError | (11 << 8)),
			IOErrorNoMem = (Result.IOError | (12 << 8)),
			IOErrorAccess = (Result.IOError | (13 << 8)),
			IOErrorCheckReservedLock = (Result.IOError | (14 << 8)),
			IOErrorLock = (Result.IOError | (15 << 8)),
			IOErrorClose = (Result.IOError | (16 << 8)),
			IOErrorDirClose = (Result.IOError | (17 << 8)),
			IOErrorSHMOpen = (Result.IOError | (18 << 8)),
			IOErrorSHMSize = (Result.IOError | (19 << 8)),
			IOErrorSHMLock = (Result.IOError | (20 << 8)),
			IOErrorSHMMap = (Result.IOError | (21 << 8)),
			IOErrorSeek = (Result.IOError | (22 << 8)),
			IOErrorDeleteNoEnt = (Result.IOError | (23 << 8)),
			IOErrorMMap = (Result.IOError | (24 << 8)),
			LockedSharedcache = (Result.Locked | (1 << 8)),
			BusyRecovery = (Result.Busy | (1 << 8)),
			CannottOpenNoTempDir = (Result.CannotOpen | (1 << 8)),
			CannotOpenIsDir = (Result.CannotOpen | (2 << 8)),
			CannotOpenFullPath = (Result.CannotOpen | (3 << 8)),
			CorruptVTab = (Result.Corrupt | (1 << 8)),
			ReadonlyRecovery = (Result.ReadOnly | (1 << 8)),
			ReadonlyCannotLock = (Result.ReadOnly | (2 << 8)),
			ReadonlyRollback = (Result.ReadOnly | (3 << 8)),
			AbortRollback = (Result.Abort | (2 << 8)),
			ConstraintCheck = (Result.Constraint | (1 << 8)),
			ConstraintCommitHook = (Result.Constraint | (2 << 8)),
			ConstraintForeignKey = (Result.Constraint | (3 << 8)),
			ConstraintFunction = (Result.Constraint | (4 << 8)),
			ConstraintNotNull = (Result.Constraint | (5 << 8)),
			ConstraintPrimaryKey = (Result.Constraint | (6 << 8)),
			ConstraintTrigger = (Result.Constraint | (7 << 8)),
			ConstraintUnique = (Result.Constraint | (8 << 8)),
			ConstraintVTab = (Result.Constraint | (9 << 8)),
			NoticeRecoverWAL = (Result.Notice | (1 << 8)),
			NoticeRecoverRollback = (Result.Notice | (2 << 8))
		}


		public enum ConfigOption : int
		{
			SingleThread = 1,
			MultiThread = 2,
			Serialized = 3
		}

		const string LibraryPath = "sqlite3";

#if !USE_CSHARP_SQLITE && !USE_WP8_NATIVE_SQLITE && !USE_SQLITEPCL_RAW
		[DllImport (LibraryPath, EntryPoint = "sqlite3_threadsafe", CallingConvention = CallingConvention.Cdecl)]
		public static extern int Threadsafe ();

		[DllImport (LibraryPath, EntryPoint = "sqlite3_open", CallingConvention = CallingConvention.Cdecl)]
		public static extern Result Open ([MarshalAs (UnmanagedType.LPStr)] string filename, out IntPtr db);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_open_v2", CallingConvention = CallingConvention.Cdecl)]
		public static extern Result Open ([MarshalAs (UnmanagedType.LPStr)] string filename, out IntPtr db, int flags, [MarshalAs (UnmanagedType.LPStr)] string zvfs);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_open_v2", CallingConvention = CallingConvention.Cdecl)]
		public static extern Result Open (byte[] filename, out IntPtr db, int flags, [MarshalAs (UnmanagedType.LPStr)] string zvfs);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_open16", CallingConvention = CallingConvention.Cdecl)]
		public static extern Result Open16 ([MarshalAs (UnmanagedType.LPWStr)] string filename, out IntPtr db);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_enable_load_extension", CallingConvention = CallingConvention.Cdecl)]
		public static extern Result EnableLoadExtension (IntPtr db, int onoff);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_close", CallingConvention = CallingConvention.Cdecl)]
		public static extern Result Close (IntPtr db);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_close_v2", CallingConvention = CallingConvention.Cdecl)]
		public static extern Result Close2 (IntPtr db);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_initialize", CallingConvention = CallingConvention.Cdecl)]
		public static extern Result Initialize ();

		[DllImport (LibraryPath, EntryPoint = "sqlite3_shutdown", CallingConvention = CallingConvention.Cdecl)]
		public static extern Result Shutdown ();

		[DllImport (LibraryPath, EntryPoint = "sqlite3_config", CallingConvention = CallingConvention.Cdecl)]
		public static extern Result Config (ConfigOption option);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_win32_set_directory", CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Unicode)]
		public static extern int SetDirectory (uint directoryType, string directoryPath);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_busy_timeout", CallingConvention = CallingConvention.Cdecl)]
		public static extern Result BusyTimeout (IntPtr db, int milliseconds);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_changes", CallingConvention = CallingConvention.Cdecl)]
		public static extern int Changes (IntPtr db);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_prepare_v2", CallingConvention = CallingConvention.Cdecl)]
		public static extern Result Prepare2 (IntPtr db, [MarshalAs (UnmanagedType.LPStr)] string sql, int numBytes, out IntPtr stmt, IntPtr pzTail);

#if NETFX_CORE
		[DllImport (LibraryPath, EntryPoint = "sqlite3_prepare_v2", CallingConvention = CallingConvention.Cdecl)]
		public static extern Result Prepare2 (IntPtr db, byte[] queryBytes, int numBytes, out IntPtr stmt, IntPtr pzTail);
#endif

		public static IntPtr Prepare2 (IntPtr db, string query)
		{
			IntPtr stmt;
#if NETFX_CORE
            byte[] queryBytes = System.Text.UTF8Encoding.UTF8.GetBytes (query);
            var r = Prepare2 (db, queryBytes, queryBytes.Length, out stmt, IntPtr.Zero);
#else
			var r = Prepare2 (db, query, System.Text.UTF8Encoding.UTF8.GetByteCount (query), out stmt, IntPtr.Zero);
#endif
			if (r != Result.OK) {
				throw SqlServerException.New (r, GetErrmsg (db));
			}
			return stmt;
		}

		[DllImport (LibraryPath, EntryPoint = "sqlite3_step", CallingConvention = CallingConvention.Cdecl)]
		public static extern Result Step (IntPtr stmt);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_reset", CallingConvention = CallingConvention.Cdecl)]
		public static extern Result Reset (IntPtr stmt);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_finalize", CallingConvention = CallingConvention.Cdecl)]
		public static extern Result Finalize (IntPtr stmt);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_last_insert_rowid", CallingConvention = CallingConvention.Cdecl)]
		public static extern long LastInsertRowid (IntPtr db);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_errmsg16", CallingConvention = CallingConvention.Cdecl)]
		public static extern IntPtr Errmsg (IntPtr db);

		public static string GetErrmsg (IntPtr db)
		{
			return Marshal.PtrToStringUni (Errmsg (db));
		}

		[DllImport (LibraryPath, EntryPoint = "sqlite3_bind_parameter_index", CallingConvention = CallingConvention.Cdecl)]
		public static extern int BindParameterIndex (IntPtr stmt, [MarshalAs (UnmanagedType.LPStr)] string name);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_bind_null", CallingConvention = CallingConvention.Cdecl)]
		public static extern int BindNull (IntPtr stmt, int index);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_bind_int", CallingConvention = CallingConvention.Cdecl)]
		public static extern int BindInt (IntPtr stmt, int index, int val);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_bind_int64", CallingConvention = CallingConvention.Cdecl)]
		public static extern int BindInt64 (IntPtr stmt, int index, long val);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_bind_double", CallingConvention = CallingConvention.Cdecl)]
		public static extern int BindDouble (IntPtr stmt, int index, double val);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_bind_text16", CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Unicode)]
		public static extern int BindText (IntPtr stmt, int index, [MarshalAs (UnmanagedType.LPWStr)] string val, int n, IntPtr free);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_bind_blob", CallingConvention = CallingConvention.Cdecl)]
		public static extern int BindBlob (IntPtr stmt, int index, byte[] val, int n, IntPtr free);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_column_count", CallingConvention = CallingConvention.Cdecl)]
		public static extern int ColumnCount (IntPtr stmt);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_column_name", CallingConvention = CallingConvention.Cdecl)]
		public static extern IntPtr ColumnName (IntPtr stmt, int index);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_column_name16", CallingConvention = CallingConvention.Cdecl)]
		static extern IntPtr ColumnName16Internal (IntPtr stmt, int index);
		public static string ColumnName16 (IntPtr stmt, int index)
		{
			return Marshal.PtrToStringUni (ColumnName16Internal (stmt, index));
		}

		[DllImport (LibraryPath, EntryPoint = "sqlite3_column_type", CallingConvention = CallingConvention.Cdecl)]
		public static extern ColType ColumnType (IntPtr stmt, int index);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_column_int", CallingConvention = CallingConvention.Cdecl)]
		public static extern int ColumnInt (IntPtr stmt, int index);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_column_int64", CallingConvention = CallingConvention.Cdecl)]
		public static extern long ColumnInt64 (IntPtr stmt, int index);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_column_double", CallingConvention = CallingConvention.Cdecl)]
		public static extern double ColumnDouble (IntPtr stmt, int index);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_column_text", CallingConvention = CallingConvention.Cdecl)]
		public static extern IntPtr ColumnText (IntPtr stmt, int index);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_column_text16", CallingConvention = CallingConvention.Cdecl)]
		public static extern IntPtr ColumnText16 (IntPtr stmt, int index);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_column_blob", CallingConvention = CallingConvention.Cdecl)]
		public static extern IntPtr ColumnBlob (IntPtr stmt, int index);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_column_bytes", CallingConvention = CallingConvention.Cdecl)]
		public static extern int ColumnBytes (IntPtr stmt, int index);

		public static string ColumnString (IntPtr stmt, int index)
		{
			return Marshal.PtrToStringUni (SQLite3.ColumnText16 (stmt, index));
		}

		public static byte[] ColumnByteArray (IntPtr stmt, int index)
		{
			int length = ColumnBytes (stmt, index);
			var result = new byte[length];
			if (length > 0)
				Marshal.Copy (ColumnBlob (stmt, index), result, 0, length);
			return result;
		}

		[DllImport (LibraryPath, EntryPoint = "sqlite3_errcode", CallingConvention = CallingConvention.Cdecl)]
		public static extern Result GetResult (Sqlite3DatabaseHandle db);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_extended_errcode", CallingConvention = CallingConvention.Cdecl)]
		public static extern ExtendedResult ExtendedErrCode (IntPtr db);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_libversion_number", CallingConvention = CallingConvention.Cdecl)]
		public static extern int LibVersionNumber ();

		[DllImport (LibraryPath, EntryPoint = "sqlite3_backup_init", CallingConvention = CallingConvention.Cdecl)]
		public static extern Sqlite3BackupHandle BackupInit (Sqlite3DatabaseHandle destDb, [MarshalAs (UnmanagedType.LPStr)] string destName, Sqlite3DatabaseHandle sourceDb, [MarshalAs (UnmanagedType.LPStr)] string sourceName);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_backup_step", CallingConvention = CallingConvention.Cdecl)]
		public static extern Result BackupStep (Sqlite3BackupHandle backup, int numPages);

		[DllImport (LibraryPath, EntryPoint = "sqlite3_backup_finish", CallingConvention = CallingConvention.Cdecl)]
		public static extern Result BackupFinish (Sqlite3BackupHandle backup);
#else
		public static Result Open (string filename, out Sqlite3DatabaseHandle db)
		{
			return (Result)Sqlite3.sqlite3_open (filename, out db);
		}

		public static Result Open (string filename, out Sqlite3DatabaseHandle db, int flags, string vfsName)
		{
#if USE_WP8_NATIVE_SQLITE
			return (Result)Sqlite3.sqlite3_open_v2(filename, out db, flags, vfsName ?? "");
#else
			return (Result)Sqlite3.sqlite3_open_v2 (filename, out db, flags, vfsName);
#endif
		}

		public static Result Close (Sqlite3DatabaseHandle db)
		{
			return (Result)Sqlite3.sqlite3_close (db);
		}

		public static Result Close2 (Sqlite3DatabaseHandle db)
		{
			return (Result)Sqlite3.sqlite3_close_v2 (db);
		}

		public static Result BusyTimeout (Sqlite3DatabaseHandle db, int milliseconds)
		{
			return (Result)Sqlite3.sqlite3_busy_timeout (db, milliseconds);
		}

		public static int Changes (Sqlite3DatabaseHandle db)
		{
			return Sqlite3.sqlite3_changes (db);
		}

		public static Sqlite3Statement Prepare2 (Sqlite3DatabaseHandle db, string query)
		{
			Sqlite3Statement stmt = default (Sqlite3Statement);
#if USE_WP8_NATIVE_SQLITE || USE_SQLITEPCL_RAW
			var r = Sqlite3.sqlite3_prepare_v2 (db, query, out stmt);
#else
			stmt = new Sqlite3Statement();
			var r = Sqlite3.sqlite3_prepare_v2(db, query, -1, ref stmt, 0);
#endif
			if (r != 0) {
				throw SQLiteException.New ((Result)r, GetErrmsg (db));
			}
			return stmt;
		}

		public static Result Step (Sqlite3Statement stmt)
		{
			return (Result)Sqlite3.sqlite3_step (stmt);
		}

		public static Result Reset (Sqlite3Statement stmt)
		{
			return (Result)Sqlite3.sqlite3_reset (stmt);
		}

		public static Result Finalize (Sqlite3Statement stmt)
		{
			return (Result)Sqlite3.sqlite3_finalize (stmt);
		}

		public static long LastInsertRowid (Sqlite3DatabaseHandle db)
		{
			return Sqlite3.sqlite3_last_insert_rowid (db);
		}

		public static string GetErrmsg (Sqlite3DatabaseHandle db)
		{
			return Sqlite3.sqlite3_errmsg (db).utf8_to_string ();
		}

		public static int BindParameterIndex (Sqlite3Statement stmt, string name)
		{
			return Sqlite3.sqlite3_bind_parameter_index (stmt, name);
		}

		public static int BindNull (Sqlite3Statement stmt, int index)
		{
			return Sqlite3.sqlite3_bind_null (stmt, index);
		}

		public static int BindInt (Sqlite3Statement stmt, int index, int val)
		{
			return Sqlite3.sqlite3_bind_int (stmt, index, val);
		}

		public static int BindInt64 (Sqlite3Statement stmt, int index, long val)
		{
			return Sqlite3.sqlite3_bind_int64 (stmt, index, val);
		}

		public static int BindDouble (Sqlite3Statement stmt, int index, double val)
		{
			return Sqlite3.sqlite3_bind_double (stmt, index, val);
		}

		public static int BindText (Sqlite3Statement stmt, int index, string val, int n, IntPtr free)
		{
#if USE_WP8_NATIVE_SQLITE
			return Sqlite3.sqlite3_bind_text(stmt, index, val, n);
#elif USE_SQLITEPCL_RAW
			return Sqlite3.sqlite3_bind_text (stmt, index, val);
#else
			return Sqlite3.sqlite3_bind_text(stmt, index, val, n, null);
#endif
		}

		public static int BindBlob (Sqlite3Statement stmt, int index, byte[] val, int n, IntPtr free)
		{
#if USE_WP8_NATIVE_SQLITE
			return Sqlite3.sqlite3_bind_blob(stmt, index, val, n);
#elif USE_SQLITEPCL_RAW
			return Sqlite3.sqlite3_bind_blob (stmt, index, val);
#else
			return Sqlite3.sqlite3_bind_blob(stmt, index, val, n, null);
#endif
		}

		public static int ColumnCount (Sqlite3Statement stmt)
		{
			return Sqlite3.sqlite3_column_count (stmt);
		}

		public static string ColumnName (Sqlite3Statement stmt, int index)
		{
			return Sqlite3.sqlite3_column_name (stmt, index).utf8_to_string ();
		}

		public static string ColumnName16 (Sqlite3Statement stmt, int index)
		{
			return Sqlite3.sqlite3_column_name (stmt, index).utf8_to_string ();
		}

		public static ColType ColumnType (Sqlite3Statement stmt, int index)
		{
			return (ColType)Sqlite3.sqlite3_column_type (stmt, index);
		}

		public static int ColumnInt (Sqlite3Statement stmt, int index)
		{
			return Sqlite3.sqlite3_column_int (stmt, index);
		}

		public static long ColumnInt64 (Sqlite3Statement stmt, int index)
		{
			return Sqlite3.sqlite3_column_int64 (stmt, index);
		}

		public static double ColumnDouble (Sqlite3Statement stmt, int index)
		{
			return Sqlite3.sqlite3_column_double (stmt, index);
		}

		public static string ColumnText (Sqlite3Statement stmt, int index)
		{
			return Sqlite3.sqlite3_column_text (stmt, index).utf8_to_string ();
		}

		public static string ColumnText16 (Sqlite3Statement stmt, int index)
		{
			return Sqlite3.sqlite3_column_text (stmt, index).utf8_to_string ();
		}

		public static byte[] ColumnBlob (Sqlite3Statement stmt, int index)
		{
			return Sqlite3.sqlite3_column_blob (stmt, index).ToArray ();
		}

		public static int ColumnBytes (Sqlite3Statement stmt, int index)
		{
			return Sqlite3.sqlite3_column_bytes (stmt, index);
		}

		public static string ColumnString (Sqlite3Statement stmt, int index)
		{
			return Sqlite3.sqlite3_column_text (stmt, index).utf8_to_string ();
		}

		public static byte[] ColumnByteArray (Sqlite3Statement stmt, int index)
		{
			int length = ColumnBytes (stmt, index);
			if (length > 0) {
				return ColumnBlob (stmt, index);
			}
			return new byte[0];
		}

		public static Result EnableLoadExtension (Sqlite3DatabaseHandle db, int onoff)
		{
			return (Result)Sqlite3.sqlite3_enable_load_extension (db, onoff);
		}

		public static int LibVersionNumber ()
		{
			return Sqlite3.sqlite3_libversion_number ();
		}

		public static Result GetResult (Sqlite3DatabaseHandle db)
		{
			return (Result)Sqlite3.sqlite3_errcode (db);
		}

		public static ExtendedResult ExtendedErrCode (Sqlite3DatabaseHandle db)
		{
			return (ExtendedResult)Sqlite3.sqlite3_extended_errcode (db);
		}

		public static Sqlite3BackupHandle BackupInit (Sqlite3DatabaseHandle destDb, string destName, Sqlite3DatabaseHandle sourceDb, string sourceName)
		{
			return Sqlite3.sqlite3_backup_init (destDb, destName, sourceDb, sourceName);
		}

		public static Result BackupStep (Sqlite3BackupHandle backup, int numPages)
		{
			return (Result)Sqlite3.sqlite3_backup_step (backup, numPages);
		}

		public static Result BackupFinish (Sqlite3BackupHandle backup)
		{
			return (Result)Sqlite3.sqlite3_backup_finish (backup);
		}
#endif

		public enum ColType : int
		{
			Integer = 1,
			Float = 2,
			Text = 3,
			Blob = 4,
			Null = 5
		}
	}
}


