using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.WindowsAzure.MobileServices;
using Microsoft.WindowsAzure.MobileServices.Query;
using Newtonsoft.Json.Linq;

namespace ZumoContrib.Sync.SQLCeStore
{
    internal class SqlQueryFormatter: QueryNodeVisitor<QueryNode>
    {
        private MobileServiceTableQueryDescription query;
        private StringBuilder sql;
        
        public IDictionary<string, object> Parameters { get; private set; }

        public SqlQueryFormatter(MobileServiceTableQueryDescription query)
        {
            this.query = query;
        }

        public string FormatSelect()
        {
            var command = new StringBuilder("SELECT ");

            // has top but not skip, use SELECT TOP
            if (this.query.Top.HasValue && query.Top.Value>0 && !this.query.Skip.HasValue)
            {
                command.AppendFormat(" TOP {0} ", this.query.Top.Value);
            }

            if (this.query.Selection.Any())
            {
                string columnNames = String.Join(", ", this.query.Selection.Select(c => SqlHelpers.FormatMember(c)));
                command.Append(columnNames);
            }
            else
            {
                command.Append("*");
            }

            return FormatQuery(command.ToString());
        }

        public string FormatSelectCount()
        {
            this.Reset();

            if (this.query.IncludeTotalCount)
            {
                this.FormatCountQuery();                
            }

            return GetSql();
        }

        public string FormatDelete()
        {
            var delQuery = this.query.Clone(); // create a copy to avoid modifying the original

            delQuery.Selection.Clear();
            delQuery.Selection.Add(MobileServiceSystemColumns.Id);
            delQuery.IncludeTotalCount = false;
            
            var formatter = new SqlQueryFormatter(delQuery);
            string selectIdQuery = formatter.FormatSelect();
            string idMemberName = SqlHelpers.FormatMember(MobileServiceSystemColumns.Id);
            string tableName = SqlHelpers.FormatTableName(delQuery.TableName);
            string command = string.Format("DELETE FROM {0} WHERE {1} IN ({2})", tableName, idMemberName, selectIdQuery);
            this.Parameters = formatter.Parameters;

            return command;
        }

        private string FormatQuery(string command)
        {
            Reset();
            
            this.sql.Append(command);

            string tableName = SqlHelpers.FormatTableName(this.query.TableName);
            this.sql.AppendFormat(" FROM {0}", tableName);

            if (this.query.Filter != null)
            {
                this.FormatWhereClause(this.query.Filter);
            }

            if (this.query.Ordering.Count > 0)
            {
                this.FormatOrderByClause(this.query.Ordering);
            }

            // OFFSET with FETCH
            if (this.query.Skip.HasValue && query.Skip.Value > 0 && this.query.Top.HasValue && query.Top.Value >0)
            {
                this.FormatLimitClause(this.query.Top.Value, this.query.Skip.Value);
            }

            return GetSql();
        }

        private string GetSql()
        {
            return this.sql.ToString().TrimEnd();
        }

        private void Reset()
        {
            this.sql = new StringBuilder();
            this.Parameters = new Dictionary<string, object>();
        }

        private void FormatLimitClause(int limit, int offset)
        {
            this.sql.AppendFormat(" OFFSET {0} ROWS", offset);
            this.sql.AppendFormat(" FETCH FIRST {0} ONLY", limit);
        }

        private void FormatCountQuery()
        {
            string tableName = SqlHelpers.FormatTableName(this.query.TableName);
            this.sql.AppendFormat("SELECT COUNT(1) AS [count] FROM {0}", tableName);

            if (this.query.Filter != null)
            {
                this.FormatWhereClause(this.query.Filter);
            }
        }

        private void FormatOrderByClause(IList<OrderByNode> orderings)
        {
            this.sql.Append(" ORDER BY ");
            string separator = String.Empty;

            foreach (OrderByNode node in orderings)
            {
                this.sql.Append(separator);
                node.Expression.Accept(this);
                if (node.Direction == OrderByDirection.Descending)
                {
                    this.sql.Append(" DESC");
                }
                separator = ", ";
            }
        }

        private void FormatWhereClause(QueryNode expression)
        {
            this.sql.Append(" WHERE ");
            expression.Accept(this);
        }

        public override QueryNode Visit(BinaryOperatorNode nodeIn)
        {
            this.sql.Append("(");

            QueryNode left = nodeIn.LeftOperand;
            QueryNode right = nodeIn.RightOperand;            
            
            if (left != null) 
            {
                // modulo requires the dividend to be an integer, monetary or numeric
                // rewrite the expression to convert to numeric, allowing the DB to apply
                // rounding if needed. our default data type for number is float which
                // is incompatible with modulo.
                if (nodeIn.OperatorKind == BinaryOperatorKind.Modulo)
                {
                    left = new ConvertNode(left, typeof(int));
                }

                left = left.Accept(this);
            }

            var rightConstant = right as ConstantNode;
            if (rightConstant != null && rightConstant.Value == null) 
            {
                // inequality expressions against a null literal have a special
                // translation in SQL
                if (nodeIn.OperatorKind == BinaryOperatorKind.Equal) 
                {
                    this.sql.Append(" IS NULL");
                }
                else if (nodeIn.OperatorKind == BinaryOperatorKind.NotEqual) 
                {
                    this.sql.Append(" IS NOT NULL");
                }
            }
            else 
            {
                switch (nodeIn.OperatorKind) 
                {
                    case BinaryOperatorKind.Equal:
                        this.sql.Append(" = ");
                        break;
                    case BinaryOperatorKind.NotEqual:
                        this.sql.Append(" <> ");
                        break;
                    case BinaryOperatorKind.LessThan:
                        this.sql.Append(" < ");
                        break;
                    case BinaryOperatorKind.LessThanOrEqual:
                        this.sql.Append(" <= ");
                        break;
                    case BinaryOperatorKind.GreaterThan:
                        this.sql.Append(" > ");
                        break;
                    case BinaryOperatorKind.GreaterThanOrEqual:
                        this.sql.Append(" >= ");
                        break;
                    case BinaryOperatorKind.And:
                        this.sql.Append(" AND ");
                        break;
                    case BinaryOperatorKind.Or:
                        this.sql.Append(" OR ");
                        break;
                    case BinaryOperatorKind.Add:
                        this.sql.Append(" + ");
                        break;
                    case BinaryOperatorKind.Subtract:
                        this.sql.Append(" - ");
                        break;
                    case BinaryOperatorKind.Multiply:
                        this.sql.Append(" * ");
                        break;
                    case BinaryOperatorKind.Divide:
                        this.sql.Append(" / ");
                        break;
                    case BinaryOperatorKind.Modulo:
                        this.sql.Append(" % ");
                        break;
                }
                
                if (right != null)
                {
                    right = right.Accept(this);
                }
            }

            this.sql.Append(")");

            if (left != nodeIn.LeftOperand || right != nodeIn.RightOperand)
            {
                return new BinaryOperatorNode(nodeIn.OperatorKind, left, right);
            }

            return nodeIn;
        }

        public override QueryNode Visit(ConstantNode nodeIn)
        {
            if (nodeIn.Value == null)
            {
                this.sql.Append("NULL");
            }
            else
            {
                this.sql.Append(this.CreateParameter(nodeIn.Value));
            }
            

            return nodeIn;
        }        

        public override QueryNode Visit(MemberAccessNode nodeIn)
        {
            string memberName = SqlHelpers.FormatMember(nodeIn.MemberName);
            this.sql.Append(memberName);

            return nodeIn;
        }

        public override QueryNode Visit(FunctionCallNode nodeIn)
        {
            switch (nodeIn.Name)
            {
                case "day":
                    return this.FormatDateFunction("dd", nodeIn);
                case "month":
                    return this.FormatDateFunction("mm", nodeIn);
                case "year":
                    return this.FormatDateFunction("yyyy", nodeIn);
                case "hour":
                    return this.FormatDateFunction("hh", nodeIn);
                case "minute":
                    return this.FormatDateFunction("mi", nodeIn);
                case "second":
                    return this.FormatDateFunction("ss", nodeIn);
                case "floor":
                    return this.FormatFloorFunction(nodeIn);
                case "ceiling":
                    return this.FormatCeilingFunction(nodeIn);
                case "round":
                    return this.FormatRoundFunction(nodeIn);
                case "tolower":
                    return this.FormatStringFunction("LOWER", nodeIn);
                case "toupper":
                    return this.FormatStringFunction("UPPER", nodeIn);
                case "length":
                    return this.FormatStringFunction("LEN", nodeIn);
                case "trim":
                    return this.FormatStringFunction("TRIM", nodeIn);
                case "substringof":
                    return this.FormatLikeFunction(true, nodeIn, true);
                case "startswith":
                    return this.FormatLikeFunction(false, nodeIn, true);
                case "endswith":
                    return this.FormatLikeFunction(true, nodeIn, false);
                case "concat":
                    return this.FormatConcatFunction(nodeIn);
                case "indexof":
                    return this.FormatIndexOfFunction(nodeIn);
                case "replace":
                    return this.FormatStringFunction("REPLACE", nodeIn);
                case "substring":
                    return FormatSubstringFunction(nodeIn);
            }

            throw new NotImplementedException();
        }

        private QueryNode FormatLikeFunction(bool startAny, FunctionCallNode nodeIn, bool endAny)
        {
            nodeIn.Arguments[0].Accept(this);
            this.sql.Append(" LIKE ");
            if (startAny)
            {
                var constantParam = (ConstantNode)nodeIn.Arguments[1];
                nodeIn.Arguments[1] = new ConstantNode("%" + constantParam.Value); //have to modify the parameter itself as SqlCe complains when doing plain string concatenation
            }

            if (endAny)
            {
                var constantParam = (ConstantNode)nodeIn.Arguments[1];
                nodeIn.Arguments[1] = new ConstantNode(constantParam.Value + "%");
            }
            
            nodeIn.Arguments[1].Accept(this);
            return nodeIn;
        }

        private QueryNode FormatConcatFunction(FunctionCallNode nodeIn)
        {
            string separator = String.Empty;
            
            foreach (QueryNode arg in nodeIn.Arguments)
            {
                this.sql.Append(separator);               
                arg.Accept(this);
                separator = " + ";
            }
          
            return nodeIn;
        }

        private QueryNode FormatIndexOfFunction(FunctionCallNode nodeIn)
        {
            this.sql.Append("CHARINDEX(");
            nodeIn.Arguments[1].Accept(this);
            this.sql.Append(", ");
            nodeIn.Arguments[0].Accept(this);
            this.sql.Append(")");
            this.sql.Append(" - 1");
            return nodeIn;
        }

        private QueryNode FormatSubstringFunction(FunctionCallNode nodeIn)
        {
            this.sql.Append("SUBSTRING(");
            nodeIn.Arguments[0].Accept(this);
            if (nodeIn.Arguments.Count > 1)
            {
                this.sql.Append(", ");

                var constantParam = (ConstantNode)nodeIn.Arguments[1];
                nodeIn.Arguments[1] = new ConstantNode(Convert.ToInt32(constantParam.Value) + 1); //workaround for SQLCe firing exception when simply appending +1 to the query parameter

                nodeIn.Arguments[1].Accept(this);

                if (nodeIn.Arguments.Count > 2)
                {
                    this.sql.Append(", ");
                    nodeIn.Arguments[2].Accept(this);
                }
                else // SQL Substring requires the length, if nothing passed, we grab till the end of string
                {
                    this.sql.Append(", ");
                    this.sql.AppendFormat("LEN(");
                     nodeIn.Arguments[0].Accept(this);
                    this.sql.Append(")");
                }
            }
            this.sql.Append(")");
            return nodeIn;
        }

        private QueryNode FormatStringFunction(string fn, FunctionCallNode nodeIn)
        {
            this.sql.AppendFormat("{0}(", fn);
            string separator = String.Empty;
            foreach (QueryNode arg in nodeIn.Arguments)
            {
                this.sql.Append(separator);
                arg.Accept(this);
                separator = ", ";
            }
            this.sql.Append(")");
            
            return nodeIn;
        }

        private QueryNode FormatCeilingFunction(FunctionCallNode nodeIn)
        {
            this.sql.Append("CEILING(");
            nodeIn.Arguments[0].Accept(this);
            this.sql.Append(")");

            return nodeIn;
        }

        private QueryNode FormatRoundFunction(FunctionCallNode nodeIn)
        {
            this.sql.Append("ROUND(");

            nodeIn.Arguments[0].Accept(this);

            this.sql.Append(", 0)");

            return nodeIn;
        }

        private QueryNode FormatFloorFunction(FunctionCallNode nodeIn)
        {
            this.sql.Append("FLOOR(");
            nodeIn.Arguments[0].Accept(this);
            this.sql.Append(")");

            return nodeIn;
        }

        private QueryNode FormatDateFunction(string formatSting, FunctionCallNode nodeIn)
        {
            this.sql.AppendFormat("DATEPART({0}, ", formatSting);
            nodeIn.Arguments[0].Accept(this);
            this.sql.Append(")");

            return nodeIn;
        }

        public override QueryNode Visit(UnaryOperatorNode nodeIn)
        {
            if (nodeIn.OperatorKind == UnaryOperatorKind.Negate)
            {
                this.sql.Append("-(");
                //this.sql.Append("NOT(");
            }
            else if (nodeIn.OperatorKind == UnaryOperatorKind.Not)
            {
                this.sql.Append("NOT(");
            }
            QueryNode operand = nodeIn.Operand.Accept(this);
            this.sql.Append(")");

            if (operand != nodeIn.Operand)
            {
                return new UnaryOperatorNode(nodeIn.OperatorKind, operand);
            }

            return nodeIn;  
        }

        public override QueryNode Visit(ConvertNode nodeIn)
        {
            this.sql.Append("CAST(");

            QueryNode source = nodeIn.Source.Accept(this);
            
            this.sql.Append(" AS ");

            string sqlType = SqlHelpers.GetColumnType(nodeIn.TargetType);
            this.sql.Append(sqlType);

            this.sql.Append(")");

            if (source != nodeIn.Source)
            {
                return new ConvertNode(source, nodeIn.TargetType);
            }

            return nodeIn;
        }

        private string CreateParameter(object value)
        {
            int paramNumber = this.Parameters.Count + 1;
            string paramName = "@p" + paramNumber;
            this.Parameters.Add(paramName, SqlHelpers.SerializeValue(new JValue(value), allowNull: true));
            return paramName;
        }        
    }
}
