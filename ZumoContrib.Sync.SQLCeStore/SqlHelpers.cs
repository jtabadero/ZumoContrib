using System;
using System.Globalization;
using Newtonsoft.Json.Linq;

namespace ZumoContrib.Sync.SQLCeStore
{
    internal class SqlHelpers
    {
        public static object SerializeValue(JValue value, bool allowNull)
        {
            string columnType = SqlHelpers.GetColumnType(value.Type, allowNull);
            return SerializeValue(value, columnType, value.Type);
        }

        public static object SerializeValue(JToken value, string sqlType, JTokenType columnType)
        {
            if (value == null || value.Type == JTokenType.Null)
            {
                return null;
            }
            switch (sqlType)
            {
                case SqlColumnType.DateTime:
                {
                    var date = value.ToObject<DateTime>();
                    if (date.Kind == DateTimeKind.Unspecified)
                    {
                        date = DateTime.SpecifyKind(date, DateTimeKind.Utc);
                    }
                    return date.ToUniversalTime();
                }

                case SqlColumnType.NText:
                case SqlColumnType.NVarchar:
                    return SerializeAsText(value, columnType);

                case SqlColumnType.Double:
                    return SerializeAsDouble(value, columnType);

                case SqlColumnType.BigInt:
                    return SerializeAsInteger(value, columnType);
                
                default:
                    return value.ToString();
            }
        }

        public static string FormatTableName(string tableName)
        {
            ValidateIdentifier(tableName);
            return string.Format("[{0}]", tableName);
        }

        public static string FormatMember(string memberName)
        {
            ValidateIdentifier(memberName);
            return string.Format("[{0}]", memberName);
        }

        private static long SerializeAsInteger(JToken value, JTokenType columnType)
        {
            return value.Value<long>();
        }

        private static double SerializeAsDouble(JToken value, JTokenType columnType)
        {
            return value.Value<double>();
        }

        private static string SerializeAsText(JToken value, JTokenType columnType)
        {
            if (columnType == JTokenType.Bytes && value.Type == JTokenType.Bytes)
            {
                return Convert.ToBase64String(value.Value<byte[]>());
            }

            return value.ToString();
        }

        public static JToken ParseText(JTokenType type, object value)
        {
            string strValue = value as string;
            if (value == null)
            {
                return strValue;
            }

          
            if (type == JTokenType.Bytes)
            {
                return Convert.FromBase64String(strValue);
            }

            if (type == JTokenType.Array || type == JTokenType.Object)
            {
                return JToken.Parse(strValue);
            }

            return strValue;
        }

        public static JToken ParseDouble(JTokenType type, object value)
        {
            double dblValue = (value as double?).GetValueOrDefault();
            return dblValue;
        }

        public static JToken ParseInteger(JTokenType type, object value)
        {
            return Convert.ToInt64(value);
        }

        public static JToken ParseBoolean(JTokenType type, object value)
        {
            bool boolValue = (value as bool?).GetValueOrDefault();
            return boolValue;
        }

        public static JToken ParseUniqueIdentier(JTokenType type, object value)
        {
            return (Guid)value;
        }

        public static string GetColumnType(Type type)
        {
            if (type == typeof(bool))
            {
                return SqlColumnType.Bit;
            }
            else if (type == typeof(int))
            {
                return SqlColumnType.BigInt;
            }
            else if (type == typeof(DateTime))
            {
                return SqlColumnType.DateTime;
            }
            else if (type == typeof(float) ||
                     type == typeof(double))
            {
                return SqlColumnType.Double;
            }
            else if (type == typeof(Guid))
            {
                return SqlColumnType.UniqueIdentifier;
            }
            else if (type == typeof(string))
            {
                return SqlColumnType.NVarchar;
            }
            else if (type == typeof(byte[]))
            {
                return SqlColumnType.NText;
            }

            throw new NotSupportedException(string.Format(CultureInfo.InvariantCulture, Properties.Resources.SqlCeStore_ValueTypeNotSupported, type.Name));
        }

        public static string GetColumnType(JTokenType type, bool allowNull)
        {
            switch (type)
            {
                case JTokenType.Boolean:
                    return SqlColumnType.Bit;
                case JTokenType.Integer:
                    return SqlColumnType.BigInt;
                case JTokenType.Date:
                    return SqlColumnType.DateTime;
                case JTokenType.Float:
                    return SqlColumnType.Double;
                case JTokenType.Guid:
                    return SqlColumnType.UniqueIdentifier;
                case JTokenType.String:
                    return SqlColumnType.NVarchar;
                case JTokenType.Array:
                case JTokenType.Object:
                case JTokenType.Bytes:
                    return SqlColumnType.NText;
                case JTokenType.Null:
                    if (allowNull)
                    {
                        return null;
                    }
                    throw new NotSupportedException(string.Format(CultureInfo.InvariantCulture, Properties.Resources.SqlCeStore_JTokenNotSupported, type));
                case JTokenType.Comment:
                case JTokenType.Constructor:
                case JTokenType.None:
                case JTokenType.Property:
                case JTokenType.Raw:
                case JTokenType.TimeSpan:
                case JTokenType.Undefined:
                case JTokenType.Uri:
                default:
                    throw new NotSupportedException(string.Format(CultureInfo.InvariantCulture, Properties.Resources.SqlCeStore_JTokenNotSupported, type));

            }
        }

        private static void ValidateIdentifier(string identifier)
        {
            if (!IsValidIdentifier(identifier))
            {
                throw new ArgumentException(string.Format(Properties.Resources.SqlCeStore_InvalidIdentifier, identifier), "identifier");
            }
        }

        private static bool IsValidIdentifier(string identifier)
        {
            if (String.IsNullOrWhiteSpace(identifier) || identifier.Length > 128)
            {
                return false;
            }

            char first = identifier[0];
            if (!(Char.IsLetter(first) || first == '_'))
            {
                return false;
            }

            for (int i = 1; i < identifier.Length; i++)
            {
                char ch = identifier[i];
                if (!(Char.IsLetterOrDigit(ch) || ch == '_'))
                {
                    return false;
                }
            }

            return true;
        }
    }
}
