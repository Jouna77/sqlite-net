using System;
using SQLBase.Sync;
using SQLite;
using SQLServer;
using SyncTest.Models;

namespace SyncTest
{
	class Program
	{
		static void Main (string[] args)
		{
			using (SQLServerConnection con =
				new SQLServerConnection ("TestDb", "192.168.0.21\\SQLEXPRESS", "1433", "sa", "12345678")) {
				con.CreateDatabase ()
					.CreateTable<ChangesHistory> ();


				InsertTest insertTest = new InsertTest ();
				insertTest.Insert (con);

				UpdateTest update = new UpdateTest ();
				update.Update (con);

				DeleteTest delete = new DeleteTest ();
				delete.Delete (con);


			}

			Console.ReadKey ();
		}


	}
}
