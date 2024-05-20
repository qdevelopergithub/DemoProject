using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DemoProject.Services.TrialBalance
{

    public class TrialBalanceResponseReportDto
    {
        [JsonProperty("status")]
        public bool Status { get; set; }

        [JsonProperty("message")]
        public string? Message { get; set; }

        [JsonProperty("result")]
        public List<QuickbooksTrialBalance>? QuickbooksTrialBalance { get; set; }
    }

}
