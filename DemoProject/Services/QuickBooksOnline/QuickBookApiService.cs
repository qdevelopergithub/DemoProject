using AutoMapper;
using Microsoft.Extensions.Configuration;
using System;
using System.ComponentModel.Design;
using System.Dynamic;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace DemoProject.Services.QuickBooksOnline
{
    public class QuickBookApiService : IAppService
    {
        private readonly IMapper _mapper;
        private readonly HttpClientRequestBase _httpClientRequestBase;
        private IConfiguration _configuration;
        private readonly string BaseUrl, XApiKey;
        private readonly string NuworkzBaseUrl;


        #region constructor
        public QuickBookApiService(IMapper mapper, IConfiguration configuration, HttpClientRequestBase httpClientRequestBase)
        {
            _configuration = configuration;
            _mapper = mapper;
            _httpClientRequestBase = httpClientRequestBase;
            BaseUrl = _configuration.GetSection("QuickBooks:BaseUrl").Value;
            NuworkzBaseUrl = _configuration.GetSection("Nuworkz:BaseUrl").Value;
            XApiKey = _configuration.GetSection("XApiKey").Value;
        }
        #endregion

        public async Task<T> CallQuickBookApi<T>(string url)
        {
            var headers = new Dictionary<string, string>();
            headers.Add("XApiKey", XApiKey);
            headers.Add("Accept", "application/json");
            var httpClientRequest = new HttpClientRequestDto
            {
                BaseUrl = BaseUrl,
                ExternalApiUrl = url,
                RequestType = HttpClientRequestType.GET,
                Headers = headers
            };
            var result = await _httpClientRequestBase.CallApi<T>(httpClientRequest);
            return result;
        }


        public async Task QBOSendResponseToCallBackURI(ImportReportDto model, string status, string reportName, string companyName, string companyId, bool dataResponse, bool isDataAlreadyImported = false, bool isTokenInValid = false,
            bool isChartOfAccountsResponse = false, string[]? dataNotFoundMonths = null, bool isDataImportedForAllCompanies = false, string[]? companyIds = null, string uniqueRequestNumber = "", string entityName = "", int[]? importReportIds = null)
        {
            var headers = new Dictionary<string, string>();
            headers.Add("Accept", "application/json");
            dynamic rt;
            rt = new ExpandoObject();
            rt.Type = "QBO";
            rt.UserId = model.UserId;
            rt.TenantId = model.TenantId;
            rt.ReportId = model.ReportId;
            rt.Status = status;
            rt.Report = reportName;
            rt.CompanyId = companyId;
            rt.CompanyName = companyName;
            rt.EntityName = entityName;
            rt.StartDate = model.StartDate;
            rt.EndDate = model.EndDate;
            rt.IsAnyDataFound = dataResponse;
            rt.IsDataAlreadyImported = isDataAlreadyImported;
            rt.IsChartOfAccountsReport = isChartOfAccountsResponse;
            rt.IsReportNeedToExport = model.IsReportNeedToExport;
            rt.IsDataImportedForAllCompanies = isDataImportedForAllCompanies;
            rt.CompanyIds = companyIds;
            rt.UniqueRequestNumber = uniqueRequestNumber;
            rt.ImportReportIds = importReportIds;
            if (dataNotFoundMonths != null && dataNotFoundMonths.Any())
            {
                rt.DataNotFoundMonths = dataNotFoundMonths;
            }
            if (isTokenInValid)
            {
                rt.StatusCode = 401;
            }
            var httpClientRequest = new HttpClientRequestDto
            {
                BaseUrl = model.CallBackBaseUrl,
                ExternalApiUrl = model.CallBackApiUrl,
                RequestType = HttpClientRequestType.POST,
                Headers = headers,
                Data = rt
            };
            var result = await _httpClientRequestBase.CallApiToGetJson(httpClientRequest);

        }

        public async Task QBOSendResponseToCallBackURI(int tenantId, long userId, string callBackBaseUrl, string callBackApiUrl, string url, string startDate, string endDate, string entityName = "", string reportCode = "")
        {

            var headers = new Dictionary<string, string>();
            headers.Add("Accept", "application/json");

            dynamic rt = new ExpandoObject();
            rt.UserId = userId;
            rt.TenantId = tenantId;
            rt.FilePath = url;
            rt.StartDate = startDate;
            rt.EndDate = endDate;
            rt.CompanyName = entityName;
            rt.ReportCode = reportCode;
            rt.Status = "Success";
            var httpClientRequest = new HttpClientRequestDto
            {
                BaseUrl = callBackBaseUrl,
                ExternalApiUrl = callBackApiUrl,
                RequestType = HttpClientRequestType.POST,
                Headers = headers,
                Data = rt
            };
            var result = await _httpClientRequestBase.CallApiToGetJson(httpClientRequest);
        }
    }
}
