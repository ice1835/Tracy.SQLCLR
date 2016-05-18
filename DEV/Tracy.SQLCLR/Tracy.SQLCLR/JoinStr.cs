using System;
using System.Data.SqlTypes;
using System.Text;
using Microsoft.SqlServer.Server;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;

/*
请先在SQL Server里执行以下命名，来启用CLR
EXEC sp_configure 'clr enabled',1 --1,启用clr 0,禁用clr
RECONFIGURE WITH OVERRIDE
*/
namespace Tracy.SQLCLR
{
    [Serializable]
    [SqlUserDefinedAggregate(
        Format.UserDefined,
        IsInvariantToNulls = true, //指示聚合是否与空值无关。
        IsInvariantToDuplicates = false, //指示聚合是否与重复值无关。
        IsInvariantToOrder = false, //指示聚合是否与顺序无关。
        MaxByteSize = 8000)] //聚合实例的最大大小（以字节为单位）。
    public class CLR_JoinStr : IBinarySerialize
    {
        private StringBuilder sb;

        /// <summary>
        /// 用户初始化
        /// </summary>
        public void Init()
        {
            sb = new StringBuilder();
        }

        /// <summary>
        /// 用来实现具体的聚合算法，是用户调用的入口
        /// 比如：select num,dbo.JoinStr([name],',') from dbo.p group by num;
        /// </summary>
        /// <param name="input">数据列中string,int,datetime都支持</param>
        /// <param name="delimiter">eg:"|" "," "-_-"</param>
        /// <param name="isNeedDistinct"></param>
        public void Accumulate(SqlChars input, SqlString delimiter, SqlBoolean isNeedDistinct)
        {
            if (input.IsNull)
            {
                return;
            }

            var temp = input.ToStr();
            if (isNeedDistinct && sb.ToString().Contains(delimiter.Value + temp + delimiter.Value))
            {
                return;
            }

            if (sb.Length > 0 )
            {
                sb.Append(temp + delimiter.Value);
            }
            else
            {
                // 把分隔符放在聚合里是因为遇到了无法从此方法向Terminate方法传递其它变量值
                // 问题原型可参考https://social.msdn.microsoft.com/Forums/en-US/e081feed-3611-4520-8463-22ce9152885c/sql-clraccumulateterminate
                sb.Append(delimiter.Value + "#<@>#" + delimiter.Value + temp + delimiter.Value);
            }
        }

        /// <summary>
        /// 用来执行每一次的聚合逻辑顺序
        /// </summary>
        /// <param name="group"></param>
        public void Merge(CLR_JoinStr group)
        {
            sb.Append(group.sb);
        }

        /// <summary>
        /// 用来将聚合的结果返回
        /// </summary>
        /// <returns></returns>
        public SqlChars Terminate()
        {
            var outPut = "";
            if (sb != null && sb.Length > 0)
            {
                outPut = sb.ToString();
                var splitIndex = outPut.IndexOf("#<@>#");
                var plitString = outPut.Substring(0, splitIndex);
                // 内容形式：分隔符#<@>#分隔符content1分隔符content2分隔符
                outPut = outPut.Substring( (plitString.Length * 2 + 5) , outPut.Length - 5 - (plitString.Length*3));
            }

            return new SqlChars(outPut);
        }

        public void Read(BinaryReader r)
        {
            sb = new StringBuilder(r.ReadString());
        }

        public void Write(BinaryWriter w)
        {
            w.Write(sb.ToString());
        }
    }
}
