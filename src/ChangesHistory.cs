using System;
using System.Collections.Generic;
using System.Text;
using SQLBase.Sync.Enums;

namespace SQLBase.Sync
{
	/// <summary>
	/// A table that keeps track of every change made on sqlite databate
	/// </summary>
	public class ChangesHistory
	{
		/// <summary>
		/// Consecutive id
		/// </summary>
		[PrimaryKey, AutoIncrement]
		public long Id { get; set; }
		/// <summary>
		/// Name of the table where te change has been made
		/// </summary>
		public string TableName { get; set; }
		/// <summary>
		/// This guid identifies the row where the change is made
		/// </summary>
		[Unique, NotNull]
		public Guid SyncGuid { get; set; }
		/// <summary>
		/// Type of change
		/// </summary>
		public NotifyTableChangedAction Action { get; set; }
		public ChangesHistory () { }
		public ChangesHistory (string TableName, Guid SyncGuid, NotifyTableChangedAction Action)
		{
			this.TableName = TableName;
			this.SyncGuid = SyncGuid;
			this.Action = Action;
		}
	}
}
