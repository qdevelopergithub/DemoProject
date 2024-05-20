using DemoProject.ApplicationContext;
using DemoProject.Constants;
using DemoProject.Model.Dto;
using DemoProject.Services;
using DemoProject.Services.APAging;
using DemoProject.Services.ARAging;
using DemoProject.Services.ChartsOfAccounts;
using DemoProject.Services.Common;
using DemoProject.Services.GeneralLedger;
using DemoProject.Services.OAuth;
using DemoProject.Services.QuickBooksOnline;
using DemoProject.Services.TrialBalance;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DemoProject.Controller.Quickbooks
{
    [Route("api/[controller]")]
    public class QuickbooksOnlineController : BaseApiController
    {
        #region fields

        private readonly AppDbContext _context;
        private readonly OAuthService _oAuthService;
        private readonly GeneralLedgerService _generalLedgerService;
        private readonly TrialBalanceService _trialBalanceService;
        private readonly ChartsOfAccountsService _chartOfAccountsService;
        private readonly QuickbookCommonService _quickbookCommonService;
        private readonly QuickbookOnlineExcelService _quickbookOnlineExcelService;
        private readonly IWebHostEnvironment _webHostEnvironment;
        private readonly IHttpContextAccessor _httpContextAccessor;
        private readonly QuickBookApiService _quickBookApiService;
        #endregion

        #region constructor
        public QuickbooksOnlineController(AppDbContext context,
            OAuthService oAuthService,
            GeneralLedgerService generalLedgerService,
            TrialBalanceService trialBalanceService,
            ChartsOfAccountsService chartOfAccountsService,
            QuickbookCommonService quickbookCommonService,
            QuickbookOnlineExcelService quickbookOnlineExcelService,
            IWebHostEnvironment webHostEnvironment,
            IHttpContextAccessor httpContextAccessor,
            QuickBookApiService quickBookApiService
            )
        {
            _context = context;
            _oAuthService = oAuthService;
            _generalLedgerService = generalLedgerService;
            _trialBalanceService = trialBalanceService;
            _chartOfAccountsService = chartOfAccountsService;
            _quickbookCommonService = quickbookCommonService;
            _quickbookOnlineExcelService = quickbookOnlineExcelService;
            _webHostEnvironment = webHostEnvironment;
            _httpContextAccessor = httpContextAccessor;
            _quickBookApiService = quickBookApiService;
        }

        #endregion

        /// <summary>
        /// Check Client Companies Status
        /// </summary>
        [HttpPost("IsQuickbookCompaniesOnlineConnected")]
        public async Task<IActionResult> IsQuickbookCompaniesOnlineConnected(ImportReportDto model)
        {
            try
            {
                var checkIfInvalidTokenCompany = await _quickbookCommonService.GetInvalidTokenCompanies(model);
                if (checkIfInvalidTokenCompany.Any())
                {
                    return ApiResponse(false, GlobalConstants.MESSAGE_INVALID_TOKEN, checkIfInvalidTokenCompany);
                }
                return ApiResponse(true, string.Format(GlobalConstants.MESSAGE_SUCCESS));
            }
            catch (Exception ex)
            {
                LogError(typeof(QuickbooksOnlineController), ex);
                return ApiResponse(false, string.Format("{0} {1} Line : {2}", GlobalConstants.MESSAGE_EXCEPTION, ex.Message, ex.StackTrace));
            }
        }


        #region FetchData Api's
        /// <summary>
        /// Fetch General Ledger report
        /// </summary>
        [HttpPost("GeneralLedger")]
        public async Task<IActionResult> GeneralLedger(ImportReportDto model)
        {
            try
            {
                var scheduleCompReportJobs = await _generalLedgerService.ImportGeneralLedger(model);
                if (scheduleCompReportJobs.Any())
                {
                    return ApiResponse(true, string.Format(GlobalConstants.MESSAGE_JOB_SCHEDULED, "General Ledger"), scheduleCompReportJobs, GlobalConstants.DATA_ALREADY_IMPORTED);
                }
                return ApiResponse(true, string.Format(GlobalConstants.MESSAGE_JOB_SCHEDULED, "General Ledger"));
            }
            catch (Exception ex)
            {
                LogError(typeof(QuickbooksOnlineController), ex);
                return ApiResponse(false, string.Format("{0} {1} Line : {2}", GlobalConstants.MESSAGE_EXCEPTION, ex.Message, ex.StackTrace));
            }
        }

        /// <summary>
        /// Fetch Trial Balance report
        /// </summary>
        [HttpPost("TrialBalance")]
        public async Task<IActionResult> TrialBalance(ImportReportDto model)
        {
            try
            {
                var scheduleCompReportJobs = await _trialBalanceService.ImportTrialBalance(model);
                if (scheduleCompReportJobs.Any())
                {
                    return ApiResponse(true, string.Format(GlobalConstants.MESSAGE_JOB_SCHEDULED, "Trial Balance"), scheduleCompReportJobs, GlobalConstants.DATA_ALREADY_IMPORTED);
                }
                return ApiResponse(true, string.Format(GlobalConstants.MESSAGE_JOB_SCHEDULED, "Trial Balance"));
            }
            catch (Exception ex)
            {
                LogError(typeof(QuickbooksOnlineController), ex);
                return ApiResponse(false, string.Format("{0} {1} Line : {2}", GlobalConstants.MESSAGE_EXCEPTION, ex.Message, ex.StackTrace));
            }
        }

        /// <summary>
        /// Get Chart of Accounts report
        /// </summary>
        [HttpPost("ChartOfAccounts")]
        public async Task<IActionResult> ChartOfAccounts(ImportReportDto model)
        {
            try
            {
                await _chartOfAccountsService.ImportChartOfAccounts(model);
                return ApiResponse(true, string.Format(GlobalConstants.MESSAGE_JOB_SCHEDULED, "Chart Of Accounts"));
            }
            catch (Exception ex)
            {
                LogError(typeof(AccountController), ex);
                return ApiResponse(false, string.Format("{0} {1} Line : {2}", GlobalConstants.MESSAGE_EXCEPTION, ex.Message, ex.StackTrace));
            }
        }

        #endregion


        #region General  Ledger



        /// <summary>
        /// Get General Ledger report
        /// </summary>
        [HttpPost("GetGeneralLedgerReport")]
        public async Task<IActionResult> GetGeneralLedgerReport(ReportDataDto model)
        {
            try
            {
                return ApiResponse(true, string.Format(GlobalConstants.MESSAGE_SUCCESS, "General Ledger"), await _generalLedgerService.GetGeneralLedgerReportAsync(model));
            }
            catch (Exception ex)
            {
                LogError(typeof(QuickbooksOnlineController), ex);
                return ApiResponse(false, string.Format("{0} {1} Line : {2}", GlobalConstants.MESSAGE_EXCEPTION, ex.Message, ex.StackTrace));
            }
        }

        /// <summary>
        /// Get General Ledger view report
        /// </summary>
        [HttpPost("GetGeneralLedgerReportView")]
        public async Task<IActionResult> GetGeneralLedgerViewReport(ReportDataDto model)
        {
            try
            {
                return ApiResponse(true, string.Format(GlobalConstants.MESSAGE_SUCCESS, "General Ledger"), await _generalLedgerService.GetGeneralLedgerReportByPagingAsync(model));
            }
            catch (Exception ex)
            {
                LogError(typeof(QuickbooksOnlineController), ex);
                return ApiResponse(false, string.Format("{0} {1} Line : {2}", GlobalConstants.MESSAGE_EXCEPTION, ex.Message, ex.StackTrace));
            }
        }

        [HttpPost("GetGLReportPaginationColumnsAsync")]
        public async Task<IActionResult> GetGLReportPaginationColumnsAsync(ReportDataDto model)
        {
            try
            {
                return ApiResponse(true, string.Format(GlobalConstants.MESSAGE_SUCCESS, "Trial Balance"), await _generalLedgerService.GetGLReportPaginationColumnsAsync(model));
            }
            catch (Exception ex)
            {
                LogError(typeof(QuickbooksOnlineController), ex);
                return ApiResponse(false, string.Format("{0} {1} Line : {2}", GlobalConstants.MESSAGE_EXCEPTION, ex.Message, ex.StackTrace));
            }
        }

        [HttpPost("GetTBReportPaginationColumnsAsync")]
        public async Task<IActionResult> GetTBReportPaginationColumnsAsync(ReportDataDto model)
        {
            try
            {
                return ApiResponse(true, string.Format(GlobalConstants.MESSAGE_SUCCESS, "Trial Balance"), await _trialBalanceService.GetTBReportPaginationColumnsAsync(model));
            }
            catch (Exception ex)
            {
                LogError(typeof(QuickbooksOnlineController), ex);
                return ApiResponse(false, string.Format("{0} {1} Line : {2}", GlobalConstants.MESSAGE_EXCEPTION, ex.Message, ex.StackTrace));
            }
        }
        #endregion

        #region TrialBalance

        /// <summary>
        /// Get Trial Balance report
        /// </summary>
        [HttpPost("GetTrialBalanceReport")]
        public async Task<IActionResult> GetTrialBalanceReport(ReportDataDto model)
        {
            try
            {
                return ApiResponse(true, string.Format(GlobalConstants.MESSAGE_SUCCESS, "Trial Balance"), await _trialBalanceService.GetTrialReportAsync(model));
            }
            catch (Exception ex)
            {
                LogError(typeof(QuickbooksOnlineController), ex);
                return ApiResponse(false, string.Format("{0} {1} Line : {2}", GlobalConstants.MESSAGE_EXCEPTION, ex.Message, ex.StackTrace));
            }
        }

        /// <summary>
        /// Get Trial Balance report
        /// </summary>
        [HttpPost("GetTrialBalanceReportByMonth")]
        public async Task<IActionResult> GetTrialBalanceReportByMonth(ReportDataDto model)
        {
            try
            {
                return ApiResponse(true, string.Format(GlobalConstants.MESSAGE_SUCCESS, "Trial Balance"), await _trialBalanceService.GetTrialReportByMonthAsync(model));
            }
            catch (Exception ex)
            {
                LogError(typeof(QuickbooksOnlineController), ex);
                return ApiResponse(false, string.Format("{0} {1} Line : {2}", GlobalConstants.MESSAGE_EXCEPTION, ex.Message, ex.StackTrace));
            }
        }

        /// <summary>
        /// Get Companies Trial Balance reports
        /// </summary>
        [HttpPost("GetTrialBalanceReportsByMonth")]
        public async Task<IActionResult> GetTrialBalanceReportsByMonth(CompanyReportDataDto model)
        {
            try
            {
                return ApiResponse(true, string.Format(GlobalConstants.MESSAGE_SUCCESS, "Trial Balance"), await _trialBalanceService.GetTrialReportsByMonthAsync(model));
            }
            catch (Exception ex)
            {
                LogError(typeof(QuickbooksOnlineController), ex);
                return ApiResponse(false, string.Format("{0} {1} Line : {2}", GlobalConstants.MESSAGE_EXCEPTION, ex.Message, ex.StackTrace));
            }
        }


        /// <summary>
        /// Get Companies Trial Balance reports
        /// </summary>
        [HttpPost("GetTrialBalanceReportView")]
        public async Task<IActionResult> GetTrialBalanceReportView(CompanyReportDataDto model)
        {
            try
            {
                return ApiResponse(true, string.Format(GlobalConstants.MESSAGE_SUCCESS, "Trial Balance"), await _trialBalanceService.GetTrialReportsByMonthPagingAsync(model));
            }
            catch (Exception ex)
            {
                LogError(typeof(QuickbooksOnlineController), ex);
                return ApiResponse(false, string.Format("{0} {1} Line : {2}", GlobalConstants.MESSAGE_EXCEPTION, ex.Message, ex.StackTrace));
            }
        }

        #endregion

        #region ChartsOfAccount



        /// <summary>
        /// Get Chart Of Accounts
        /// </summary>
        [HttpPost("GetChartOfAccounts")]
        public async Task<IActionResult> GetChartOfAccounts(ReportDataDto model)
        {
            try
            {
                return ApiResponse(true, string.Format(GlobalConstants.MESSAGE_SUCCESS, "Chart of Accounts"), await _chartOfAccountsService.GetChartOfAccountsAsync(model));
            }
            catch (Exception ex)
            {
                LogError(typeof(QuickbooksOnlineController), ex);
                return ApiResponse(false, string.Format("{0} {1} Line : {2}", GlobalConstants.MESSAGE_EXCEPTION, ex.Message, ex.StackTrace));
            }
        }

        #endregion

        #region TBCurrent
        /// <summary>
        /// Get TBCurrent
        /// </summary>
        [HttpPost("GetTBCurrentReport")]
        public async Task<IActionResult> GetTBCurrentReport(ReportDataDto model)
        {
            try
            {
                return ApiResponse(true, string.Format(GlobalConstants.MESSAGE_SUCCESS, "Trial Balance"), await _trialBalanceService.GetTBCurrentReportAsync(model));
            }
            catch (Exception ex)
            {
                LogError(typeof(QuickbooksOnlineController), ex);
                return ApiResponse(false, string.Format("{0} {1} Line : {2}", GlobalConstants.MESSAGE_EXCEPTION, ex.Message, ex.StackTrace));
            }
        }


        [HttpPost("GetTBCurrentReportPaginationAsync")]
        public async Task<IActionResult> GetTBCurrentReportPaginationAsync(ReportDataDto model)
        {
            try
            {
                return ApiResponse(true, string.Format(GlobalConstants.MESSAGE_SUCCESS, "Trial Balance"), await _trialBalanceService.GetTBCurrentReportPaginationAsync(model));
            }
            catch (Exception ex)
            {
                LogError(typeof(QuickbooksOnlineController), ex);
                return ApiResponse(false, string.Format("{0} {1} Line : {2}", GlobalConstants.MESSAGE_EXCEPTION, ex.Message, ex.StackTrace));
            }
        }


        [HttpPost("GetTBCurrentReportPaginationColumnsAsync")]
        public async Task<IActionResult> GetTBCurrentReportPaginationColumnsAsync(ReportDataDto model)
        {
            try
            {
                return ApiResponse(true, string.Format(GlobalConstants.MESSAGE_SUCCESS, "Trial Balance"), await _trialBalanceService.GetTBCurrentReportPaginationColumnsAsync(model));
            }
            catch (Exception ex)
            {
                LogError(typeof(QuickbooksOnlineController), ex);
                return ApiResponse(false, string.Format("{0} {1} Line : {2}", GlobalConstants.MESSAGE_EXCEPTION, ex.Message, ex.StackTrace));
            }
        }

        #endregion

        #region TBDump
        /// <summary>
        /// Get TBDump
        /// </summary>
        [HttpPost("GetTBDumpReport")]
        public async Task<IActionResult> GetTBDumpReport(ReportDataDto model)
        {
            try
            {
                return ApiResponse(true, string.Format(GlobalConstants.MESSAGE_SUCCESS, "Trial Balance"), await _trialBalanceService.GetTBDumpReportAsync(model));
            }
            catch (Exception ex)
            {
                LogError(typeof(QuickbooksOnlineController), ex);
                return ApiResponse(false, string.Format("{0} {1} Line : {2}", GlobalConstants.MESSAGE_EXCEPTION, ex.Message, ex.StackTrace));
            }
        }
        #endregion

        #region TBExceptionalReport
        /// <summary>
        /// Get TBExceptionalReport
        /// </summary>
        [HttpPost("GetTBExceptionalReport")]
        public async Task<IActionResult> GetTBExceptionalReport(ReportDataDto model)
        {
            try
            {
                return ApiResponse(true, string.Format(GlobalConstants.MESSAGE_SUCCESS, "Invalid Exceptional Report"), await _trialBalanceService.GetTBInvalidExceptionReportsAsync(model));
            }
            catch (Exception ex)
            {
                LogError(typeof(QuickbooksOnlineController), ex);
                return ApiResponse(false, string.Format("{0} {1} Line : {2}", GlobalConstants.MESSAGE_EXCEPTION, ex.Message, ex.StackTrace));
            }
        }

        [HttpPost("GetTBExceptionReportPaginationAsync")]
        public async Task<IActionResult> GetTBExceptionReportPaginationAsync(ReportDataDto model)
        {
            try
            {
                return ApiResponse(true, string.Format(GlobalConstants.MESSAGE_SUCCESS, "Invalid Exceptional Report"), await _trialBalanceService.GetTBExceptionReportPaginationAsync(model));
            }
            catch (Exception ex)
            {
                LogError(typeof(QuickbooksOnlineController), ex);
                return ApiResponse(false, string.Format("{0} {1} Line : {2}", GlobalConstants.MESSAGE_EXCEPTION, ex.Message, ex.StackTrace));
            }
        }
        #endregion

        #region Excel report

        [HttpPost("CreateExcelReportRequest")]
        public async Task<IActionResult> CreateExcelReportRequest(ReportDataDto model)
        {
            try
            {
                var request = _httpContextAccessor.HttpContext.Request;
                var baseUrl = $"{request.Scheme}://{request.Host.Value}";
                await _quickbookOnlineExcelService.CreateExcelReportRequest(model, _webHostEnvironment.WebRootPath, baseUrl);
                return ApiResponse(true, string.Format(GlobalConstants.MESSAGE_SUCCESS, "Excel Report Succeed"));
            }
            catch (Exception ex)
            {
                LogError(typeof(QuickbooksOnlineController), ex);
                return ApiResponse(false, string.Format("{0} {1} Line : {2}", GlobalConstants.MESSAGE_EXCEPTION, ex.Message, ex.StackTrace));
            }
        }

        [HttpPost("CreateExcelRequestByReport")]
        public async Task<IActionResult> CreateExcelRequestByReport(ReportDataDto model)
        {
            try
            {
                var request = _httpContextAccessor.HttpContext.Request;
                var baseUrl = $"{request.Scheme}://{request.Host.Value}";
                var path = await _quickbookOnlineExcelService.CreateExcelRequestByReport(model, _webHostEnvironment.WebRootPath, baseUrl);
                return ApiResponse(true, string.Format(GlobalConstants.MESSAGE_SUCCESS, "Excel Report Succeed"), path);
            }
            catch (Exception ex)
            {
                LogError(typeof(QuickbooksOnlineController), ex);
                return ApiResponse(false, string.Format("{0} {1} Line : {2}", GlobalConstants.MESSAGE_EXCEPTION, ex.Message, ex.StackTrace));
            }
        }
        #endregion

    }
}
