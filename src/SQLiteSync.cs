using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using SQLBase.Sync;
using SQLite;

namespace SQLite.Sync
{
#pragma warning disable 1591 // XML Doc Comments
	public class SQLiteSync
	{
		public static SQLiteSync Instance { get; private set; }
		private readonly SQLiteConnectionString ConnectionString;
		private SQLiteSync (SQLiteConnectionString ConnectionString)
		{
			this.ConnectionString = ConnectionString;
		}
		public static SQLiteSync Init (string DatabasePath)
		{
			Instance = new SQLiteSync (new SQLiteConnectionString (DatabasePath));
			CreateVersionControlTable ();
			return Instance;
		}
		public static SQLiteSync Init (SQLiteConnectionString ConnectionString)
		{
			Instance = new SQLiteSync (ConnectionString);
			CreateVersionControlTable ();
			return Instance;
		}

		private static void CreateVersionControlTable ()
		{
			using (SQLiteConnection con = Instance.Connection ()) {
				con.CreateTable<ChangesHistory> ();
			}
		}

		private SQLiteConnection _Connection;
		private SQLiteAsyncConnection _AsyncConnection;


		public SQLiteConnection Connection ()
		{
			if (_AsyncConnection != null) {
				if (_AsyncConnection.GetConnection ().IsInTransaction) {
					throw SQLiteException.New (SQLite3.Result.CannotOpen,
						"SQLiteConnection is in transaction please wait until it finish to start an async connection");
				}

				_AsyncConnection.GetConnection ().Close ();
				_AsyncConnection.GetConnection ().Dispose ();
				_AsyncConnection = null;
			}

			if (_Connection is null || _Connection.IsClosed) {
				_Connection = new SQLiteConnection (ConnectionString);
			}
			return _Connection;
		}

		private async void CloseAsync ()
		{
			await _AsyncConnection.CloseAsync ();
			_AsyncConnection = null;
		}

		public SQLiteAsyncConnection AsyncConnection ()
		{
			if (_Connection != null) {
				if (_Connection.IsInTransaction) {
					throw SQLiteException.New (SQLite3.Result.CannotOpen,
						"SQLiteConnection is in transaction please wait until it finish to start an async connection");
				}

				_Connection.Close ();
				_Connection.Dispose ();
				_Connection = null;
			}

			if (_AsyncConnection is null) {
				_AsyncConnection = new SQLiteAsyncConnection (ConnectionString);
			}
			return _AsyncConnection;
		}


	}
}
