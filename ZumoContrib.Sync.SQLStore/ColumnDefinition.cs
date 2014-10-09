using System;
using Newtonsoft.Json.Linq;

namespace ZumoContrib.Sync.SQLStore
{
    internal class ColumnDefinition
    {
        public string Name { get; private set; }

        public JTokenType JsonType { get; private set; }

        public string StoreType { get; private set; }


        public ColumnDefinition(string name, JTokenType jsonType, string storeType)
        {
            this.Name = name;
            this.JsonType = jsonType;
            this.StoreType = storeType;
        }

        public override int GetHashCode()
        {
            return Tuple.Create(this.Name, this.JsonType, this.StoreType).GetHashCode();
        }

        public override bool Equals(object obj)
        {
            var other = obj as ColumnDefinition;
            if (other == null)
            {
                return base.Equals(obj);
            }

            return this.Name.Equals(other.Name) &&
                   this.JsonType.Equals(other.JsonType) &&
                   this.StoreType.Equals(other.StoreType);
        }
    }
}
