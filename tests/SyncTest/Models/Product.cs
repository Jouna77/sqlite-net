using System;
using System.Collections.Generic;
using System.Text;
using SQLBase;
using SQLite;

namespace SyncTest.Models
{
	public class Product 
	{
		[PrimaryKey, AutoIncrement]
		public int Id { get; set; }
		public string Name { get; set; }

		public Product ()
		{

		}
	}
}
