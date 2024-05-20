using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DemoProject.Services.GeneralLedger
{
    public class ChartsOfAccountsDto
    {
        [JsonProperty("status")]
        public bool Status { get; set; }

        [JsonProperty("message")]
        public string Message { get; set; }

        [JsonProperty("code")]
        public int? Code { get; set; }

        [JsonProperty("result")]
        public List<ChartOfAccountsEntity>? QuickbookChartsOfAccounts { get; set; }
    }

    public class ValidTokenDto
    {
        [JsonProperty("status")]
        public bool Status { get; set; }

        [JsonProperty("message")]
        public string Message { get; set; }

        [JsonProperty("code")]
        public int? Code { get; set; }

    }

    public class InValidTokenCompanyDto
    {
        public string CompanyName { get; set; }
        public string CompanyId { get; set; }

    }

    public class GetValidTokenCompanyDto
    {
        [JsonProperty("status")]
        public bool Status { get; set; }

        [JsonProperty("message")]
        public string Message { get; set; }
        [JsonProperty("result")]
        public ValidTokenCompanyDto? ValidTokenCompanyDto { get; set; }

    }
    public class ValidTokenCompanyDto
    {
        [JsonProperty("refreshToken")]
        public string RefreshToken { get; set; }

        [JsonProperty("accessToken")]
        public string AccessToken { get; set; }
    }
}
