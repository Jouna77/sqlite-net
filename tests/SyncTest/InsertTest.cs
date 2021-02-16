using System;
using System.Collections.Generic;
using SQLServer;
using SyncTest.Models;

namespace SyncTest
{
	public class InsertTest
	{
		public void Insert (SQLServerConnection con)
		{
			Product product = new Product () { Name = "Orange Juice" };
			con.CreateTable<Product> ();
			con.Insert (product);
			product = new Product () { Name = "Apple Juice" };
			con.Insert (product);
		}
	}
}
