using System.Data.SqlTypes;
using System.Text.RegularExpressions;
using System.IO;
using System.IO.Compression;
using System;
using Microsoft.SqlServer.Server;
using System.Text;
using System.Collections;

/*
请先在SQL Server里执行以下命名，来启用CLR
EXEC sp_configure 'clr enabled',1 --1,启用clr 0,禁用clr
RECONFIGURE WITH OVERRIDE
*/

namespace Tracy.SQLCLR
{
    public static partial class UserDefinedFunctions
    {
        /// <summary>
        /// 使用.net的正则实现替换。eg:
        /// UPDATE dbo.PBS_FlightOrderGroupApprovement SET ApproveData= dbo.fn_CLR_RegexReplace(ApproveData,N'(AbacusTaxes.+?)Flight.Entity',N'$1Ticket.Entity',1);
        /// </summary>
        /// <param name="input"></param>
        /// <param name="pattern"></param>
        /// <param name="replacement"></param>
        /// <param name="isIgnoreCase"></param>
        /// <returns></returns>
        [SqlFunction(Name = "fn_CLR_RegexReplace")]
        public static SqlChars RegexReplace(SqlChars input, SqlString pattern, SqlString replacement, SqlBoolean isIgnoreCase)
        {
            if (input.IsNull)
            {
                return null;
            }

            var re = new Regex(pattern.Value, isIgnoreCase ? RegexOptions.IgnoreCase : RegexOptions.None);
            return new SqlChars(re.Replace(input.ToStr(), replacement.Value));
        }

        /// <summary>
        /// 使用.net的正则来检查是否匹配。eg:
        /// SELECT * FROM dbo.PBS_FlightOrderGroupApprovement WHERE dbo.fn_CLR_RegexIsMatch(ApproveData,N'(AbacusTaxes.+?)Flight.Entity',1)=1;
        /// </summary>
        /// <param name="input"></param>
        /// <param name="pattern"></param>
        /// <param name="isIgnoreCase"></param>
        /// <returns></returns>
        [SqlFunction(Name = "fn_CLR_RegexIsMatch")]
        public static SqlBoolean RegexIsMatch(SqlChars input, SqlString pattern, SqlBoolean isIgnoreCase)
        {
            if (input.IsNull)
            {
                return false;
            }

            var re = new Regex(pattern.Value, isIgnoreCase ? RegexOptions.IgnoreCase : RegexOptions.None);
            return re.IsMatch(input.ToStr());
        }

        [SqlFunction(Name = "fn_CLR_RegexMatch")]
        public static SqlString RegexMatch(SqlChars input, SqlString pattern, SqlBoolean isIgnoreCase, SqlInt32 groupnum)
        {
            if (input.IsNull)
            {
                return null;
            }

            var re = new Regex(pattern.Value, isIgnoreCase ? RegexOptions.IgnoreCase : RegexOptions.None);
            var match = re.Match(input.ToStr());

            if (match.Success)
            {
                return match.Groups[groupnum.Value].Value;
            }

            return null;
        }

        /// <summary>
        /// SQL CLR 使用.net的Contains查找是否满足条件,eg:
        /// select dbo.ContainsOne('我是柳永法，','柳永法');
        /// select * from Articles where dbo.ContainsOne(txtContent,'柳永法')=1;
        /// </summary>
        /// <param name="input">源串，或字段名</param>
        /// <param name="search">要搜索的字符串</param>
        /// <returns>返回是否匹配,1,0</returns>
        [SqlFunction(Name = "fn_CLR_ContainsOne")]
        public static SqlBoolean ContainsOne(SqlChars input, SqlString search)
        {
            if (input.IsNull)
            {
                return false;
            }

            return input.ToStr().Contains(search.Value);
        }

        /// <summary>
        /// 實現類似 DateTime.ToString("yyyy-MM-dd");
        /// </summary>
        /// <param name="input"></param>
        /// <param name="format"></param>
        /// <returns></returns>
        [SqlFunction(Name = "fn_CLR_GetDateTimeString")]
        public static SqlString GetDateTimeString(SqlDateTime input, SqlString format)
        {
            if (input.IsNull)
            {
                return null;
            }

            return input.Value.ToString(format.Value);
        }


        /// <summary>
        /// SQL CLR 使用.net的Contains查找是否满足其中之一的条件,eg:
        /// select dbo.ContainsAny('我是柳永法，','柳|永|法');
        /// select * from Articles where dbo.ContainsAny(txtContent,'柳|永|法')=1;
        /// </summary>
        /// <param name="input">源串，或字段名</param>
        /// <param name="search">要搜索的字符串，以"|"分隔，自己处理空格问题</param>
        /// <returns>返回是否匹配,1,0</returns>
        [SqlFunction(Name = "fn_CLR_ContainsAny")]
        public static SqlBoolean ContainsAny(SqlChars input, SqlString search)
        {
            if (input.IsNull)
            {
                return false;
            }

            var strTemp = input.ToStr();
            foreach (var item in search.Value.Split('|'))
            {
                if (strTemp.Contains(item))
                {
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// SQL CLR 使用.net的Contains查找是否满足所有的条件,eg:
        /// select dbo.ContainsAll('我是柳永法，','柳|永|法');
        /// select * from Articles where dbo.ContainsAll(txtContent,'柳|永|法')=1;
        /// </summary>
        /// <param name="input">源串，或字段名</param>
        /// <param name="search">要搜索的字符串，以"|"分隔，自己处理空格问题</param>
        /// <returns>返回是否匹配,1,0</returns>
        [SqlFunction(Name = "fn_CLR_ContainsAll")]
        public static SqlBoolean ContainsAll(SqlChars input, SqlString search)
        {
            if (input.IsNull)
            {
                return false;
            }

            var strTemp = input.ToStr();
            foreach (var item in search.Value.Split('|'))
            {
                if (!strTemp.Contains(item))
                {
                    return false;
                }
            }
            return true;
        }

        /// <summary>
        /// 字符串split
        /// </summary>
        /// <param name="input">输入字符串,支持包含空值</param>
        /// <param name="delimiter">分隔符,支持字符串,比如-_-</param>
        /// <param name="isRemoveEmptyEntries"></param>
        /// <returns></returns>
        [SqlFunction(Name = "fn_CLR_Split", FillRowMethodName = "FillRow", TableDefinition = "id nvarchar(max)")]
        public static IEnumerable SqlArray(SqlChars input, SqlString delimiter, SqlBoolean isRemoveEmptyEntries)
        {
            if (input.IsNull)
            {
                return new string[] { };
            }
            if (delimiter.IsNull)
            {
                return new string[1] { input.ToStr() };
            }
            var isNeedRemoveEmpty = isRemoveEmptyEntries ? StringSplitOptions.RemoveEmptyEntries : StringSplitOptions.None;
            return input.ToStr().Split(new string[] { delimiter.Value }, isNeedRemoveEmpty);
        }

        /// <summary>
        /// 用于填充数据的方法
        /// </summary>
        /// <param name="row">输入</param>
        /// <param name="str">输出的类型</param>
        public static void FillRow(object row, out SqlString str)
        {
            str = new SqlString((string)row);
        }

        public static string ToStr(this SqlChars input)
        {
            if (input.IsNull)
            {
                return null;
            }

            return new string(input.Value);
        }

        #region Gzip压缩解压相关

        /// <summary>
        /// Gzip压缩
        /// </summary>
        /// <param name="str"></param>
        /// <returns></returns>
        [SqlFunction(Name = "fn_CLR_GZipCompress")]
        public static byte[] GZipCompress(this string str)
        {
            var buffer = System.Text.Encoding.UTF8.GetBytes(str);
            using (var ms = new MemoryStream())
            {
                using (var gzs = new GZipStream(ms, CompressionMode.Compress, true))
                {
                    gzs.Write(buffer, 0, buffer.Length);
                    gzs.Close();

                    return ms.GetBuffer();
                }
            }
        }

        /// <summary>
        /// GZipStream解压
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        [return: SqlFacet(MaxSize = -1)]
        [SqlFunction(Name = "fn_CLR_GZipDecompress")]
        public static string GZipDecompress([SqlFacet(MaxSize = -1)] this byte[] data)
        {
            if (data == null)
            {
                return string.Empty;
            }
            using (var stream = new MemoryStream(data))
            {
                var buffer = new byte[0x1000];
                var length = 0;

                using (var gzs = new GZipStream(stream, CompressionMode.Decompress))
                {
                    using (var ms = new MemoryStream())
                    {
                        while ((length = gzs.Read(buffer, 0, buffer.Length)) != 0)
                        {
                            ms.Write(buffer, 0, length);
                        }
                        return Encoding.UTF8.GetString(ms.ToArray());
                    }
                }
            }
        }

        /// <summary>
        ///  GZipStream解压并查找是否含有字符串
        /// </summary>
        /// <param name="data">需要解压的数据</param>
        /// <param name="search">需要查找的字符串</param>
        /// <param name="isIgnoreCase">是否需要忽略大小写</param>
        /// <returns>True：包含需要查找的字符串，False：不包含需要查找的字符串</returns>
        [SqlFunction(Name = "fn_CLR_GZipDecompressAndSearch")]
        public static SqlBoolean GZipDecompressAndSearch([SqlFacet(MaxSize = -1)] this byte[] data, SqlString search, SqlBoolean isIgnoreCase)
        {
            var resultStr = GZipDecompress(data);
            var ignoreCaseComparison = isIgnoreCase ? StringComparison.OrdinalIgnoreCase : System.StringComparison.Ordinal;
            return resultStr.IndexOf(search.ToString(), ignoreCaseComparison) > 0;
        }

        #endregion
    };
}