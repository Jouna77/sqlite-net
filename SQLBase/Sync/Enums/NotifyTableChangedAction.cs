using System;
using System.Collections.Generic;
using System.Text;

namespace SQLBase.Sync.Enums
{

	[StoreAsText]
	public enum NotifyTableChangedAction
	{
		Insert,
		Update,
		Delete,
	}
}
