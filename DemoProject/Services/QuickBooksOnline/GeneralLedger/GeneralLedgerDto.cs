using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DemoProject.Services.GeneralLedger
{
    public class GeneralLedgerResponeReportDto
    {
        [JsonProperty("status")]
        public bool Status { get; set; }

        [JsonProperty("message")]
        public string Message { get; set; }

        [JsonProperty("result")]
        public List<QuickbooksGeneralLedger>? QuickbooksGeneralLedger { get; set; }
    }
    public class GeneralLedgerReportDto
    {
        public GLReportHeaderDto Header { get; set; }
        public GLRowsDto Rows { get; set; }
    }

    public class GLReportHeaderDto
    {
        public string ReportBasis { get; set; }
        public string StartPeriod { get; set; }
        public string EndPeriod { get; set; }
        public string Currency { get; set; }
    }

    public class GLRowsDto
    {
        public List<GLRowsRowDto> Row { get; set; }
    }
    public class GLRowsInsideDto
    {
        public List<GLRowsRowDto> Row { get; set; }
    }

    public class GLRowsRowDto
    {
        public GLColumnDataDto Header { get; set; }
        public GLRowDataValueDto Rows { get; set; }
        public GLColumnDataDto Summary { get; set; }
        public List<GLColDataValuDto> ColData { get; set; }
    }

    public class GLRowDataValueDto
    {
        public List<GLColumnDataInsideDto> Row { get; set; }
    }

    public class GLColumnDataInsideDto
    {
        public GLColumnDataDto Header { get; set; }
        public GLRowsInsideDto Rows { get; set; }
        public List<GLColDataValuDto> ColData { get; set; }
        public GLColumnDataDto Summary { get; set; }
    }
    public class GLColumnDataDto
    {
        public List<GLColDataValuDto> ColData { get; set; }
    }
    public class GLColDataValuDto
    {
        public string value { get; set; }
    }
}
