using System;
using System.Collections.Generic;
using System.Text;
using SQLServer;
using SyncTest.Models;

namespace SyncTest
{
	public class UpdateTest
	{
		public void Update (SQLServerConnection con)
		{
			var products = con.Table<Product> ().ToList ();
			foreach (Product product in products) {
				product.Name += " Updated";
				con.Update (product);
			}

		}
	}
}
