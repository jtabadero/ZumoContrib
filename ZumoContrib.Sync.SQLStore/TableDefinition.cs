using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Microsoft.WindowsAzure.MobileServices;

namespace ZumoContrib.Sync.SQLStore
{ /// <summary>
    /// A class that represents the structure of table on local store
    /// </summary>
    [SuppressMessage("Microsoft.Interoperability", "CA1405:ComVisibleTypeBaseTypesShouldBeComVisible")]
    internal class TableDefinition : Dictionary<string, ColumnDefinition>
    {
        public MobileServiceSystemProperties SystemProperties { get; private set; }

        public TableDefinition()
        {
        }

        public TableDefinition(IDictionary<string, ColumnDefinition> definition, MobileServiceSystemProperties systemProperties)
            : base(definition, StringComparer.OrdinalIgnoreCase)
        {
            this.SystemProperties = systemProperties;
        }
    }
}
