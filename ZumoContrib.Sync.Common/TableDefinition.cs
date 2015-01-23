using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Microsoft.WindowsAzure.MobileServices;

namespace ZumoContrib.Sync.Common
{
    [SuppressMessage("Microsoft.Interoperability", "CA1405:ComVisibleTypeBaseTypesShouldBeComVisible")]
    public class TableDefinition : Dictionary<string, ColumnDefinition>
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
