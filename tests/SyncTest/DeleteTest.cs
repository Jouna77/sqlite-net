using System;
using System.Collections.Generic;
using System.Text;
using SQLServer;
using SyncTest.Models;

namespace SyncTest
{
	public class DeleteTest
	{
		public void Delete (SQLServerConnection con)
		{
			var products = con.Table<Product> ().ToList ();
			foreach (Product product in products) {
				con.Delete (product);
			}

		}
	}
}
