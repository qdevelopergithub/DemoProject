using Hangfire;
using LazyCache;
using Microsoft.EntityFrameworkCore;
using Microsoft.VisualBasic;
using Newtonsoft.Json;
using SpreadsheetLight;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.Design;
using System.Data;
using System.Dynamic;
using System.Globalization;
using System.Net.Http.Headers;
using System.Security.Cryptography.X509Certificates;
using System.Security.Principal;
using System.Text.RegularExpressions;

namespace DemoProject.Services.TrialBalance
{
    public class TrialBalanceService : IAppService
    {
        #region fields
        private readonly AppDbContext _context;
        private readonly ReportService _reportService;
        private readonly OAuthService _oAuthService;
        private readonly QuickBookApiService _quickBookApiService;
        private readonly CommonService _commonService;
        private readonly QuickbookCommonService _quickbookCommonService;
        private readonly NuWorkzCompanyRepository _nuWorkzCompanyRepository;
        private readonly CompanyService _companyService;

        #endregion

        #region constructor

        public TrialBalanceService(AppDbContext context, ReportService reportService,
            OAuthService oAuthService, QuickBookApiService quickBookApiService, CommonService commonService,
            QuickbookCommonService quickbookCommonService, NuWorkzCompanyRepository nuWorkzCompanyRepository, CompanyService companyService
      )
        {
            _context = context;
            _reportService = reportService;
            _quickBookApiService = quickBookApiService;
            _oAuthService = oAuthService;
            _commonService = commonService;
            _quickbookCommonService = quickbookCommonService;
            _nuWorkzCompanyRepository = nuWorkzCompanyRepository;
            _companyService = companyService;
        }
        #endregion

        #region methods

        public async Task<List<CompanyMonthsDto>> ImportTrialBalance(ImportReportDto model)
        {
            using (var uow = new AppUnitOfWork(_context))
            {
                var ifAlreadyImportedForAnyMonth = new List<CompanyMonthsDto>();
                var companies = await uow.RefreshTokenRepository.FindByQueryable(r => r.TenantId == model.TenantId && model.CompanyId.Contains(r.CompanyId)).ToListAsync();
                if (model.OverwriteAlreadyImportedData == null)
                {
                    ifAlreadyImportedForAnyMonth = await _commonService.GetAlreadyImportedCompanyReportMonths(uow, model);
                    if (ifAlreadyImportedForAnyMonth.Any())
                    {
                        var alreadyImpCompIds = ifAlreadyImportedForAnyMonth.Select(f => f.CompanyId).ToArray();

                        // Remove companies that have already imported data
                        companies = companies.Where(f => !alreadyImpCompIds.Contains(f.CompanyId)).ToList();
                    }
                }
                var requestList = new List<QBOImportRequest>();
                foreach (var item in companies)
                {
                    var importRequest = new QBOImportRequest() { CompanyId = item.CompanyId, CompanyName = item.CompanyName, TenantId = model.TenantId, ReportId = model.ReportId, UniqueRequestNumber = model.UniqueRequestNumber };
                    requestList.Add(importRequest);
                    BackgroundJob.Schedule(() => FetchTrialBalance(model, item.CompanyId, item.CompanyName), TimeSpan.FromSeconds(20));
                }
                if (requestList.Count > 0)
                {
                    uow.QBOImportRequestsRepository.Insert(requestList);
                    await uow.CommitAsync();
                }
                return ifAlreadyImportedForAnyMonth;
            }
        }


        public async Task FetchTrialBalance(ImportReportDto model, string companyId, string companyName)
        {
            try
            {
                DataTable dtDates = CommonService.GetMonthsBetweenDates(Convert.ToDateTime(model.StartDate), Convert.ToDateTime(model.EndDate));
                using (AppUnitOfWork uow = new AppUnitOfWork(_context))
                {
                    if (await _quickbookCommonService.IsCompanyTokenValid(uow, companyId, model.TenantId))
                    {
                        var isAnySelectedMonthHasData = false; var dataNotFoundMonths = new List<string>();
                        var startDate = ""; var endDate = "";
                        foreach (var monthRow in dtDates.AsEnumerable())
                        {
                            startDate = Convert.ToString(monthRow["StartDay"]);
                            endDate = Convert.ToString(monthRow["EndDay"]);
                            if (await FetchTrialBalanceAsync(uow, companyId, model.TenantId, model.ReportId, model.UniqueRequestNumber, startDate, endDate))
                            {
                                isAnySelectedMonthHasData = true;
                            }
                            else
                            {
                                dataNotFoundMonths.Add(startDate);
                            }
                        }
                        #region Update Import Status For Imported Report
                        var getImportedReq = uow.QBOImportRequestsRepository.Find(r => r.TenantId == model.TenantId && r.CompanyId == companyId
                                                                                   && r.UniqueRequestNumber == model.UniqueRequestNumber && r.ReportId == model.ReportId).FirstOrDefault();
                        if (getImportedReq != null)
                        {
                            getImportedReq.IsImported = true;
                            uow.QBOImportRequestsRepository.InsertOrUpdate(getImportedReq);
                            await uow.CommitAsync();
                        }
                        #endregion

                        // if any of IsImported is false it means some report is pending to be imorted
                        var allImportRequests = uow.QBOImportRequestsRepository
                                .Find(request => request.UniqueRequestNumber == model.UniqueRequestNumber && request.TenantId == model.TenantId
                                , null, 1, int.MaxValue)
                                .ToList();
                        var isDataImportedForAllRequests = allImportRequests.Where(f => f.IsImported == false).Any() == true ? false : true;
                        var importReportIds = allImportRequests.Select(f => f.ReportId).Distinct().ToArray();
                        if (isDataImportedForAllRequests)
                        {
                            foreach (var compId in model.CompanyId)
                            {
                                await UpdateChartOfAccountsData(uow, companyId, model.TenantId, model.ChartOfAccountsReportId, model.UniqueRequestNumber);
                            }
                        }

                        var entityName = uow.NwzCompanyRepository.FindByQueryable(x => companyId == x.ExtCompanyId).FirstOrDefault()?.EntityUID;
                        await _quickBookApiService.QBOSendResponseToCallBackURI(model, "Success", "Trial Balance", companyName, companyId, isAnySelectedMonthHasData, false, false, false, CommonService.FormatDates(dataNotFoundMonths.ToArray()), isDataImportedForAllRequests, model.CompanyId, null, entityName, importReportIds);
                    }
                    else
                    {
                        await _quickBookApiService.QBOSendResponseToCallBackURI(model, "Error", "Trial Balance", companyName, companyId, false, false, true);
                    }
                }
            }
            catch (Exception ex)
            {
                await _quickBookApiService.QBOSendResponseToCallBackURI(model, "Error", "Trial Balance", companyName, companyId, false);
                throw;
            }
        }

        public async Task<bool> UpdateChartOfAccountsData(AppUnitOfWork uow, string companyId, int tenantId, int chartOfAccountsReportId, string uniqueRequestNumber)
        {

            //check if any data exist against cselected company and delete it
            var isAlreadyDataExist = uow.ChartOfAccountsRepository.Find(x => x.TenantId == tenantId && x.CompanyId == companyId, null, 1, int.MaxValue).ToList(); ;
            var alreadyExistedEntries = uow.ImportedReportsInfoRepository.Find(x => x.TenantId == tenantId && x.CompanyId == companyId && x.ReportId == chartOfAccountsReportId, null, 1, int.MaxValue).ToList();

            if (alreadyExistedEntries.Any() || isAlreadyDataExist.Any())
            {
                if (alreadyExistedEntries.Any()) uow.ImportedReportsInfoRepository.SoftDelete(alreadyExistedEntries);
                if (isAlreadyDataExist.Any()) uow.ChartOfAccountsRepository.SoftDelete(isAlreadyDataExist);

                await uow.CommitAsync();
            }

            var refreshToken = _oAuthService.GetRefreshToken(uow, tenantId, companyId);
            var url = string.Format(ExternalUrl.ChartOfAccounts, companyId, tenantId, refreshToken.Token);
            var data = await _quickBookApiService.CallQuickBookApi<ChartsOfAccountsDto>(url);
            if (data != null && data.Status)
            {
                DateTime currentDate = DateTime.Now; 
                var startDate = new DateTime(currentDate.Year, currentDate.Month, 1).ToString("yyyy-MM-dd");
                var endDate = new DateTime(currentDate.Year, currentDate.Month, 1).ToString("yyyy-MM-dd");

                var importDataInfo = new ImportedReportsInfo(startDate, endDate, companyId, tenantId, chartOfAccountsReportId, uniqueRequestNumber);
                uow.ImportedReportsInfoRepository.InsertOrUpdate(importDataInfo);

                if (data.QuickbookChartsOfAccounts != null && data.QuickbookChartsOfAccounts.Count > 0)
                {
                    var companyName = uow.RefreshTokenRepository.Find(x => x.TenantId == tenantId && x.CompanyId == companyId).FirstOrDefault();
                    data.QuickbookChartsOfAccounts.ForEach(item =>
                    {
                        item.CompanyId = companyId;
                        item.CompanyName = refreshToken.CompanyName;
                        item.TenantId = tenantId;
                        item.CreatedOn = DateTime.Now;
                    });
                    uow.ChartOfAccountsRepository.Insert(data.QuickbookChartsOfAccounts);
                    await uow.CommitAsync();
                    return true;
                }
                await uow.CommitAsync();
            }
            return false;
        }


        private async Task<dynamic> FetchTrialBalanceAsync(AppUnitOfWork uow, string companyId, int tenantId, int reportId, string uniqueRequestNumber, string startDate, string endDate)
        {
            try
            {
                // Check if there is already data in the trial balance for the given parameters
                var isAlreadyDataTB = uow.TrialBalanceRepository.Find(x => x.StartPeriod.Date == DateTime.ParseExact(startDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date
                    && x.TenantId == tenantId && x.CompanyId == companyId, null, 1, int.MaxValue).ToList();


                // If overwrite is enabled, delete existing entries in ImportedReportsInfoRepository and TrialBalanceRepository
                var alreadyExistedEntries = uow.ImportedReportsInfoRepository.Find(x => x.StartPeriod == startDate
                    && x.TenantId == tenantId && x.CompanyId == companyId && x.ReportId == reportId, null, 1, int.MaxValue).ToList();

                if (alreadyExistedEntries.Any() || isAlreadyDataTB.Any())
                {
                    if (alreadyExistedEntries.Any()) uow.ImportedReportsInfoRepository.SoftDelete(alreadyExistedEntries);
                    if (isAlreadyDataTB.Any()) uow.TrialBalanceRepository.SoftDelete(isAlreadyDataTB);

                    isAlreadyDataTB.Clear();
                    await uow.CommitAsync();
                }

                if (!isAlreadyDataTB.Any())
                {
                    // Fetch data from QuickBooks API
                    var refreshToken = _oAuthService.GetRefreshToken(uow, tenantId, companyId);
                    var url = string.Format(ExternalUrl.TrialBalance, startDate, endDate, companyId, tenantId, refreshToken.Token);
                    var data = await _quickBookApiService.CallQuickBookApi<TrialBalanceResponseReportDto>(url);

                    if (data != null && data.Status)
                    {
                        // Insert data into ImportedReportsInfoRepository
                        var importDataInfo = new ImportedReportsInfo(startDate, endDate, companyId, tenantId, reportId, uniqueRequestNumber);
                        uow.ImportedReportsInfoRepository.InsertOrUpdate(importDataInfo);

                        if (data.QuickbooksTrialBalance != null && data.QuickbooksTrialBalance.Count > 0)
                        {
                            // Update CompanyId, CompanyName, and TenantId for each item in QuickbooksGeneralLedger
                            data.QuickbooksTrialBalance.ForEach(item =>
                            {
                                item.CompanyId = companyId;
                                item.CompanyName = refreshToken.CompanyName;
                                item.TenantId = tenantId;
                            });

                            // Insert data into GeneralLedgerRepository
                            uow.TrialBalanceRepository.Insert(data.QuickbooksTrialBalance);

                            await uow.CommitAsync();
                            return true;
                        }

                        await uow.CommitAsync();
                    }

                    return false;
                }
                else
                {
                    return true;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public async Task<ReportResultDto> GetTrialReportAsync(ReportDataDto model)
        {
            try
            {
                using (AppUnitOfWork uow = new AppUnitOfWork(_context))
                {
                    var viewModel = new ReportResultDto();
                    dynamic list = new List<ExpandoObject>();
                    viewModel.EndDate = model.endDate;
                    DataTable dtDates = CommonService.GetMonthStartEndDate(model.endDate);
                    var getLastMonthOfPeriod = dtDates.AsEnumerable().Last();
                    var reportImportInfo = uow.ImportedReportsInfoRepository.Find(x => x.TenantId == model.tenantId && x.ReportId == model.reportId && x.CompanyId == model.companyId).ToList();
                    var lastMonthStartDate = Convert.ToString(getLastMonthOfPeriod["StartDay"]);
                    var lastMonthEndDate = Convert.ToString(getLastMonthOfPeriod["EndDay"]);

                    var staticColumnlist = new List<string>();
                    var staticDynamiclist = new List<StaticColumnNames>();
                    var staticDynamicObj = new StaticColumnNames();

                    var checkIfLastMonthImported = reportImportInfo.Any(info =>
                            info.StartPeriod == lastMonthStartDate);
                    if (!checkIfLastMonthImported)
                    {
                        viewModel.IsDataImported = false;
                        viewModel.Months = dtDates.AsEnumerable()
                            .Where(dateRow => !reportImportInfo.Any(info =>
                                info.StartPeriod == Convert.ToString(dateRow["StartDay"]))).
                                Select(x => new MonthsDto { StartDate = Convert.ToString(x["StartDay"]), EndDate = Convert.ToString(x["EndDay"]) }).ToList();
                        return viewModel;
                    }
                    viewModel.IsDataImported = true;

                    var extCompanyId = uow.NwzCompanyRepository.Find(f => !string.IsNullOrEmpty(f.ExtCompanyId) && f.ExtCompanyId.Trim() == model.companyId, null, 1, int.MaxValue).Select(f => f.CompanyId).FirstOrDefault();

                    var validFs_Type_List = uow.FSTypeRepository.FindByQueryable(f => f.ParentId != null).Select(f => f.Name).ToArray();

                    var categoryMappings = uow.CategoryMappingRepository.Find(f => validFs_Type_List.Contains(f.FS_Type) && f.Company_ID == extCompanyId, null, 1, int.MaxValue).ToList();


                    var accountMappings = uow.AccountMappingRepository.Find(f => f.Company_ID == extCompanyId, null, 1, int.MaxValue).ToList();
                    var trialBalanceList = uow.TrialBalanceRepository.Find(x =>
                        (x.StartPeriod.Date >= DateTime.ParseExact(lastMonthStartDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date
                        && x.EndPeriod.Date <= DateTime.ParseExact(lastMonthEndDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date) &&
                        x.CompanyId == model.companyId &&
                        x.TenantId == model.tenantId, null, 1, int.MaxValue
                    ).ToList();

                    var customCoulmns = categoryMappings.GroupBy(f => f.FS_Type).Select(f => f.Key).ToList();

                    foreach (var item in trialBalanceList)
                    {
                        staticDynamiclist = new List<StaticColumnNames>();
                        dynamic tb = new ExpandoObject();
                        tb.TenantId = item.TenantId;
                        tb.CompanyId = item.CompanyId;
                        tb.StartPeriod = item.StartPeriod.ToString("yyyy-MM-dd");
                        tb.EndPeriod = item.EndPeriod.ToString("yyyy-MM-dd");
                        tb.CompanyName = item.CompanyName;
                        tb.Credit = item.Credit;
                        tb.Debit = item.Debit;
                        tb.ReportBasis = item.ReportBasis;
                        tb.Label = item.Label;
                        tb.Currency = item.Currency;
                        tb.AccountNumber = item.AccountNumber;
                        tb.AccountDescription = item.AccountDescription;
                        tb.Department = "";
                        tb.Entity = "";
                        tb.FS_ID = "";
                        tb.FS_Type = "";
                        tb.AdjustingEntry = "";
                        tb.CustomColumnNames = customCoulmns;
                        tb.Net = CommonService.CalculateDebitCreditDifference(item.Debit != null ? (decimal)item.Debit : 0.00m, item.Credit != null ? (decimal)item.Credit : 0.00m);
                        tb.AdjustingTb = tb.Net;
                        if (!string.IsNullOrEmpty(tb.AccountNumber))
                        {
                            var getAccountMapping = accountMappings.Where(f => f.Account_UID == tb.AccountNumber).FirstOrDefault();
                            if (getAccountMapping != null)
                            {
                                tb.Entity = getAccountMapping.Entity_ID;
                                tb.FS_ID = getAccountMapping.Category_UID;
                                tb.FS_Type = getAccountMapping.FS;


                                var getCategoryMapping = categoryMappings.Where(f => f.Category_UID == getAccountMapping.Category_UID && !string.IsNullOrEmpty(f.FS_Type)).GroupBy(f => f.FS_Type).ToList();
                                foreach (var categoryMap in getCategoryMapping)
                                {
                                    staticDynamicObj = new StaticColumnNames();
                                    staticDynamicObj.ColumnName = categoryMap.Key;
                                    staticDynamicObj.ColumnValue = categoryMap.FirstOrDefault()?.FS_ID;
                                    staticDynamiclist.Add(staticDynamicObj);
                                }
                            }
                        }
                        tb.CustomColumns = staticDynamiclist;
                        list.Add(tb);
                    }
                    viewModel.Data = list;
                    return viewModel;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public async Task<ReportResultDto> GetTrialReportByMonthAsync(ReportDataDto model)
        {
            try
            {
                using (AppUnitOfWork uow = new AppUnitOfWork(_context))
                {
                    var viewModel = new ReportResultDto();

                    if (string.IsNullOrEmpty(model.startDate) || model.startDate == model.endDate)
                    {
                        var financialStartMonth = await _companyService.GetCompanyStartFinancYRByCompanyId(uow, model.companyId, model.tenantId);
                        if (financialStartMonth > 0)
                        {
                            model.startDate = CommonService.GetStartDateOfFincByDate(financialStartMonth, model.endDate);
                        }
                    }

                    viewModel.StartDate = model.startDate;
                    viewModel.EndDate = model.endDate;

                    dynamic tbMonthList = new List<ExpandoObject>();
                    var customDateColumnlist = new List<string>();
                    var staticColumnlist = new List<string>();
                    var staticDynamiclist = new List<StaticColumnNames>();
                    var staticDateDynamiclist = new List<StaticColumnNames>();
                    var staticDynamicObj = new StaticColumnNames();

                    viewModel.IsDataImported = true;

                    var extCompanyId = uow.NwzCompanyRepository.Find(f => !string.IsNullOrEmpty(f.ExtCompanyId) && f.ExtCompanyId.Trim() == model.companyId, null, 1, int.MaxValue).Select(f => f.CompanyId).FirstOrDefault();

                    var validFs_Type_List = uow.FSTypeRepository.FindByQueryable(f => f.ParentId != null).Select(f => f.Name).ToArray();

                    var categoryMappings = uow.CategoryMappingRepository.Find(f => validFs_Type_List.Contains(f.FS_Type) && f.Company_ID == extCompanyId, null, 1, int.MaxValue).ToList();
                    var accountMappings = uow.AccountMappingRepository.Find(f => f.Company_ID == extCompanyId, null, 1, int.MaxValue).ToList();
                    var trialBalanceList = uow.TrialBalanceRepository.FindLargeRecords(x =>
                        (x.StartPeriod.Date >= DateTime.ParseExact(model.startDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date
                        && x.EndPeriod.Date <= DateTime.ParseExact(model.endDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date) &&
                        x.CompanyId == model.companyId &&
                        x.TenantId == model.tenantId, null, 1, int.MaxValue
                    ).ToList();
                    viewModel.CompanyName = uow.RefreshTokenRepository.FindByQueryable(r => r.CompanyId == model.companyId && r.TenantId == model.tenantId).FirstOrDefault().CompanyName;
                    var customCoulmns = categoryMappings.GroupBy(f => f.FS_Type).Select(f => f.Key).ToList();
                    var monthByTrialBalanceList = trialBalanceList.GroupBy(x => new { x.EndPeriod }).OrderBy(f => f.Key.EndPeriod);
                    foreach (var tbRecord in monthByTrialBalanceList)
                    {
                        dynamic tbMonth = new ExpandoObject();
                        tbMonth.MonthStartDate = tbRecord.FirstOrDefault().StartPeriod.ToString("yyyy-MM-dd");
                        tbMonth.MonthEndDate = tbRecord.FirstOrDefault().EndPeriod.ToString("yyyy-MM-dd");
                        tbMonth.Period = tbRecord.FirstOrDefault().EndPeriod.ToString("yyyy-MM");
                        tbMonth.CompanyName = viewModel.CompanyName;
                        dynamic list = new List<ExpandoObject>();
                        foreach (var item in tbRecord)
                        {
                            staticDynamiclist = new List<StaticColumnNames>();
                            dynamic tb = new ExpandoObject();
                            tb.TenantId = item.TenantId;
                            tb.CompanyId = item.CompanyId;
                            tb.StartPeriod = item.StartPeriod.ToString("yyyy-MM-dd");
                            tb.EndPeriod = item.EndPeriod.ToString("yyyy-MM-dd");
                            tb.CompanyName = item.CompanyName;
                            tb.Period = tbMonth.Period;
                            tb.Credit = item.Credit;
                            tb.Debit = item.Debit;
                            tb.ReportBasis = item.ReportBasis;
                            tb.Label = item.Label;
                            tb.Currency = item.Currency;
                            tb.AccountNumber = item.AccountNumber;
                            tb.AccountDescription = item.AccountDescription;
                            tb.Department = "";
                            tb.Entity = "";
                            tb.FS_ID = "";
                            tb.FS_Type = "";
                            tb.AdjustingEntry = "";
                            tb.CustomColumnNames = customCoulmns;
                            tb.Net = CommonService.CalculateDebitCreditDifference(item.Debit != null ? (decimal)item.Debit : 0.00m, item.Credit != null ? (decimal)item.Credit : 0.00m);
                            tb.AdjustingTb = tb.Net;
                            if (!string.IsNullOrEmpty(tb.AccountNumber))
                            {
                                var getAccountMapping = accountMappings.Where(f => f.Account_UID == tb.AccountNumber).FirstOrDefault();
                                if (getAccountMapping != null)
                                {
                                    tb.Entity = getAccountMapping.Entity_ID;
                                    tb.FS_ID = getAccountMapping.Category_UID;
                                    tb.FS_Type = getAccountMapping.FS;


                                    var getCategoryMapping = categoryMappings.Where(f => f.Category_UID == getAccountMapping.Category_UID && !string.IsNullOrEmpty(f.FS_Type)).GroupBy(f => f.FS_Type).ToList();
                                    foreach (var categoryMap in getCategoryMapping)
                                    {
                                        staticDynamicObj = new StaticColumnNames();
                                        staticDynamicObj.ColumnName = categoryMap.Key;
                                        staticDynamicObj.ColumnValue = categoryMap.FirstOrDefault()?.FS_ID;
                                        staticDynamiclist.Add(staticDynamicObj);
                                    }
                                }
                            }
                            tb.CustomColumns = staticDynamiclist;
                            list.Add(tb);
                        }
                        tbMonth.MonthList = list;
                        tbMonthList.Add(tbMonth);
                    }
                    viewModel.Data = tbMonthList;
                    return viewModel;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }


        public async Task<dynamic> GetTrialReportsByMonthAsync(CompanyReportDataDto model)
        {
            try
            {
                using (AppUnitOfWork uow = new AppUnitOfWork(_context))
                {
                    dynamic trialBalanceDto = new ExpandoObject();
                    dynamic tbCompanyMonthsList = new List<ExpandoObject>();
                    dynamic tbMappedUnMappedList = new List<ExpandoObject>();
                    var getExtCompanies = uow.NwzCompanyRepository.Find(f => !string.IsNullOrEmpty(f.ExtCompanyId) && model.companyIds.Contains(f.ExtCompanyId.Trim()), null, 1, int.MaxValue).ToList();
                    var getExtCompanyIds = getExtCompanies.Select(f => f.CompanyId).ToArray();
                    var validFs_Type_List = uow.FSTypeRepository.FindByQueryable(f => f.ParentId != null).Select(f => f.Name).ToArray();

                    var categoryCompanyMappings = uow.CategoryMappingRepository.Find(f => validFs_Type_List.Contains(f.FS_Type) && getExtCompanyIds.Contains(f.Company_ID), null, 1, int.MaxValue).ToList();
                    var customCoulmns = categoryCompanyMappings.GroupBy(f => f.FS_Type).Select(f => f.Key).ToList();
                    var allAccountMappings = uow.AccountMappingRepository.Find(f => getExtCompanyIds.Contains(f.Company_ID.Trim()), null, 1, int.MaxValue).ToList();

                    if (string.IsNullOrEmpty(model.startDate) || model.startDate == model.endDate)
                    {
                        var financialStartMonth = await _companyService.GetCompanyStartFinancYRByCompanyId(uow, model.companyIds[0], model.tenantId);
                        if (financialStartMonth > 0)
                        {
                            model.startDate = CommonService.GetStartDateOfFincByDate(financialStartMonth, model.endDate);
                        }
                    }

                    var fiscalCalendars = uow.FiscalCalendarRepository.FindByQueryable(x => getExtCompanies.Select(e => e.EntityUID).Contains(x.EntityGroup));

                    //Group By Month
                    var allByTrialBalanceList = await uow.TrialBalanceRepository.FindLargeRecordsInListAsync(x =>
                          (x.StartPeriod.Date >= DateTime.ParseExact(model.startDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date
                          && x.EndPeriod.Date <= DateTime.ParseExact(model.endDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date) &&
                          model.companyIds.Contains(x.CompanyId) &&
                          x.TenantId == model.tenantId, null, 1, int.MaxValue);

                    var monthByTrialBalanceList = allByTrialBalanceList.GroupBy(x => new { x.EndPeriod }).OrderBy(f => f.Key.EndPeriod);
                    var tbByCompanies = allByTrialBalanceList.GroupBy(x => x.CompanyId);

                    var staticDynamiclist = new List<StaticColumnNames>();
                    var staticDynamicObj = new StaticColumnNames();
                    dynamic periodBalanceList = new List<ExpandoObject>();
                    if (monthByTrialBalanceList.Any())
                    {
                        var companyEntities = getExtCompanies.Select(f => f.EntityUID).Distinct().ToArray();
                        //Loop on Month
                        foreach (var monthlyTrialBalnce in monthByTrialBalanceList)
                        {
                            dynamic tbCompanyMonthsObj = new ExpandoObject();
                            tbCompanyMonthsObj.StartDate = model.startDate;
                            tbCompanyMonthsObj.EndDate = model.endDate;
                            tbCompanyMonthsObj.PeriodMonthYear = monthlyTrialBalnce.FirstOrDefault().EndPeriod.ToString("yyyy-MM");
                            tbCompanyMonthsObj.Entity_Name = companyEntities != null ? string.Join(" ,", companyEntities) : "";

                            //Loop on Company
                            dynamic tbMonthCompany = new List<ExpandoObject>();
                            periodBalanceList = new List<ExpandoObject>();
                            var netDiff = 0.00m;
                            foreach (var companyId in model.companyIds)//coming from refresh_tokens companyIds
                            {

                                dynamic tbMonth = new ExpandoObject();
                                tbMonth.MonthStartDate = monthlyTrialBalnce.FirstOrDefault().StartPeriod.ToString("yyyy-MM-dd");
                                tbMonth.MonthEndDate = monthlyTrialBalnce.FirstOrDefault().EndPeriod.ToString("yyyy-MM-dd");
                                tbMonth.PeriodMonthYear = monthlyTrialBalnce.FirstOrDefault().EndPeriod.ToString("yyyy-MM");
                                tbMonth.CompanyId = companyId;
                                dynamic list = new List<ExpandoObject>();
                                var nwzCompany = getExtCompanies.Where(f => f.ExtCompanyId == companyId).FirstOrDefault();
                                var nwzCompanyId = nwzCompany?.CompanyId;
                                var companyTbList = monthlyTrialBalnce.Where(f => f.CompanyId == companyId);
                                var categoryMappings = categoryCompanyMappings.Where(f => f.Company_ID == nwzCompanyId).ToList();
                                var accountMappings = allAccountMappings.Where(f => f.Company_ID == nwzCompanyId).ToList();

                                var fc = fiscalCalendars.Where(x => x.Entity_UID == nwzCompanyId && x.date_key.Date == monthlyTrialBalnce.FirstOrDefault().StartPeriod.Date).FirstOrDefault();

                                Parallel.ForEach(companyTbList, item =>
                                {
                                    var accountNumber = accountMappings.Where(x => item.AccountNumber == x.Account_UID).FirstOrDefault();
                                    staticDynamiclist = new List<StaticColumnNames>();
                                    dynamic tb = new ExpandoObject();
                                    var isCsutomColmAdded = false;
                                    tb.TenantId = item.TenantId;
                                    tb.CompanyId = item.CompanyId;
                                    tb.StartPeriod = item.StartPeriod.ToString("yyyy-MM-dd");
                                    tb.EndPeriod = item.EndPeriod.ToString("yyyy-MM-dd");
                                    tb.CompanyName = nwzCompany != null ? !string.IsNullOrEmpty(nwzCompany.Name) ? nwzCompany.Name : item.CompanyName : item.CompanyName; ;
                                    tb.Credit = item.Credit;
                                    tb.Debit = item.Debit;
                                    tb.ReportBasis = item.ReportBasis;
                                    tb.Label = item.Label;
                                    tb.Currency = item.Currency;
                                    tb.AccountNumber = accountNumber?.New_Account_UID ?? string.Empty;
                                    tb.AccountDescription = item.AccountDescription;
                                    tb.Department = "";
                                    tb.Company_Entity = nwzCompanyId;
                                    tb.FS_ID = "";
                                    tb.FS_Type = "";
                                    tb.AdjustingEntry = "";
                                    tb.Entity = nwzCompany?.EntityUID;
                                    tb.Period = fc?.fp ?? 0;
                                    tb.Entity_Group = nwzCompany?.Entity_Group;
                                    tb.Quarter = fc?.Quarter ?? string.Empty;
                                    tb.Year = fc?.fy.ToString() ?? string.Empty;
                                    tb.Date = fc?.date_key.ToString() ?? string.Empty;
                                    tb.YearMonth = fc?.fiscal_year_month.ToString() ?? string.Empty;
                                    tb.CustomColumnNames = customCoulmns;
                                    netDiff = CommonService.CalculateDebitCreditDifferenceInDecimal(item.Debit != null ? (decimal)item.Debit : 0.00m, item.Credit != null ? (decimal)item.Credit : 0.00m);
                                    tb.Net = netDiff;
                                    tb.NetValue = netDiff;
                                    tb.AdjustingTb = tb.Net;
                                    if (!string.IsNullOrEmpty(tb.AccountNumber))
                                    {
                                        var getAccountMapping = accountMappings.Where(x => item.AccountNumber == x.Account_UID).FirstOrDefault();
                                        if (getAccountMapping != null)
                                        {
                                            isCsutomColmAdded = true;
                                            tb.FS_ID = getAccountMapping.FS_ID;
                                            tb.FS_Type = getAccountMapping.FS;

                                            var getCategoryMapping = categoryMappings.Where(f => f.Category_UID == getAccountMapping.Category_UID && !string.IsNullOrEmpty(f.FS_Type)).GroupBy(f => f.FS_Type).ToList();
                                            foreach (var categoryMap in getCategoryMapping)
                                            {
                                                staticDynamicObj = new StaticColumnNames();
                                                staticDynamicObj.ColumnName = categoryMap.Key;
                                                getAccountMapping.FS_ID = (getAccountMapping.FS_ID == "" || getAccountMapping.FS_ID == null) ? "MISSING" : getAccountMapping.FS_ID;
                                                staticDynamicObj.ColumnValue = getAccountMapping.FS_ID;
                                                staticDynamicObj.ColumnDescription = getAccountMapping.Company_Name;// categoryMap.FirstOrDefault()?.Name;
                                                staticDynamiclist.Add(staticDynamicObj);
                                            }
                                        }
                                    }
                                    if (isCsutomColmAdded == false)
                                    {
                                        foreach (var column in customCoulmns)
                                        {
                                            staticDynamicObj = new StaticColumnNames();
                                            staticDynamicObj.ColumnName = column;
                                            staticDynamicObj.ColumnValue = "MISSING";
                                            staticDynamiclist.Add(staticDynamicObj);
                                        }
                                    }

                                    tb.CustomColumns = staticDynamiclist;
                                    list.Add(tb);
                                });
                                tbMonth.Data = list;
                                dynamic periodBalanceObj = new ExpandoObject();
                                periodBalanceObj.Entity = nwzCompanyId;
                                periodBalanceObj.FcPeriod = fc?.fp ?? 0;
                                periodBalanceObj.Balance = ((IEnumerable<dynamic>)list).Sum(f => (decimal)f.NetValue);
                                periodBalanceList.Add(periodBalanceObj);

                                tbMonthCompany.Add(tbMonth);
                            }

                            tbCompanyMonthsObj.PeriodBalanceData = periodBalanceList;
                            tbCompanyMonthsObj.Data = tbMonthCompany;
                            tbCompanyMonthsList.Add(tbCompanyMonthsObj);
                        }
                    }

                    foreach (var tbList in tbByCompanies)
                    {
                        dynamic mappedUnmappedObj = new ExpandoObject();
                        mappedUnmappedObj.Entity = getExtCompanies.Where(f => f.ExtCompanyId == tbList.Key).FirstOrDefault()?.CompanyId;
                        var accountNumbers = tbList.Select(accntNo => accntNo.AccountNumber);
                        int nullAccountsCount = accountNumbers?.Where(x => string.IsNullOrEmpty(x)).Count() ?? 0;
                        int distinctAccountsCount = accountNumbers?.Where(x => !string.IsNullOrEmpty(x)).Distinct().Count() ?? 0;
                        mappedUnmappedObj.TotalAccounts = nullAccountsCount + distinctAccountsCount;
                        mappedUnmappedObj.TotalNetBalance = CommonService.ConvertValueToAbs(CommonService.CalculateDebitCreditDifferenceInDecimal(tbList.Where(x => x.Debit != null).Select(x => (decimal)x.Debit).ToArray(),
                            tbList.Where(x => x.Credit != null).Select(x => (decimal)x.Credit).ToArray()));

                        var mappedAccounts = ((IEnumerable<dynamic>)tbCompanyMonthsList).SelectMany(f => (IEnumerable<dynamic>)f.Data).Where(f => f.CompanyId == tbList.Key)
                            .SelectMany(f => (IEnumerable<dynamic>)f.Data)
                            .Where(f => string.IsNullOrEmpty(f.FS_ID) && ((IEnumerable<dynamic>)f.CustomColumns).Where(f => !string.IsNullOrEmpty(f.ColumnValue)).Any()).Select(f => f.AccountDescription).Distinct().ToArray();

                        mappedUnmappedObj.TotalUnMappedAccounts = mappedAccounts.Count();
                        mappedUnmappedObj.TotalMappedAccounts = mappedUnmappedObj.TotalAccounts - mappedUnmappedObj.TotalUnMappedAccounts;

                        tbMappedUnMappedList.Add(mappedUnmappedObj);
                    }
                    trialBalanceDto.TrialBalanceData = tbCompanyMonthsList;
                    trialBalanceDto.MappedUnMappedData = tbMappedUnMappedList;
                    return trialBalanceDto;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public async Task<dynamic> GetTrialReportsByMonthPagingAsync(CompanyReportDataDto model)
        {
            try
            {
                using (AppUnitOfWork uow = new AppUnitOfWork(_context))
                {
                    dynamic tbCompanyMonthsList = new List<ExpandoObject>();
                    dynamic data = new ExpandoObject();
                    var getExtCompanies = uow.NwzCompanyRepository.Find(f => !string.IsNullOrEmpty(f.ExtCompanyId) && model.companyIds.Contains(f.ExtCompanyId.Trim()), null, 1, int.MaxValue).ToList();
                    var getExtCompanyIds = getExtCompanies.Select(f => f.CompanyId).ToArray();
                    var validFs_Type_List = uow.FSTypeRepository.FindByQueryable(f => f.ParentId != null).Select(f => f.Name).ToArray();

                    var categoryCompanyMappings = uow.CategoryMappingRepository.Find(f => validFs_Type_List.Contains(f.FS_Type) && getExtCompanyIds.Contains(f.Company_ID), null, 1, int.MaxValue).ToList();
                    var customCoulmns = categoryCompanyMappings.GroupBy(f => f.FS_Type).Select(f => f.Key).ToList();
                    var allAccountMappings = uow.AccountMappingRepository.Find(f => getExtCompanyIds.Contains(f.Company_ID.Trim()), null, 1, int.MaxValue).ToList();
                    if (string.IsNullOrEmpty(model.startDate) || model.startDate == model.endDate)
                    {
                        var financialStartMonth = await _companyService.GetCompanyStartFinancYRByCompanyId(uow, model.companyIds[0], model.tenantId);
                        if (financialStartMonth > 0)
                        {
                            model.startDate = CommonService.GetStartDateOfFincByDate(financialStartMonth, model.endDate);
                        }
                    }

                    //Group By Month
                  
                    var OrderColumn = model.sorting.Split(" ")[0];
                    BaseRepository<QuickbooksTrialBalance, AppDbContext>.LargeRecordsResult<QuickbooksTrialBalance> trialBalanceList;

                    Func<IQueryable<QuickbooksTrialBalance>, IOrderedQueryable<QuickbooksTrialBalance>> orderByExpression;

                    bool isAscending = model.sorting.Contains("asc", StringComparison.InvariantCultureIgnoreCase);

                    switch (OrderColumn)
                    {
                        case "Period":
                            orderByExpression = x => isAscending ?
                                x.OrderBy(m => m.StartPeriod) :
                                x.OrderByDescending(m => m.StartPeriod);
                            break;
                        case "Entity":
                        case "Company_Entity":
                        case "CompanyEntity":
                            string[] qbCompanyIds = null;
                            if (isAscending)
                            {
                                qbCompanyIds = getExtCompanies.OrderBy(f => f.CompanyId).Select(f => f.ExtCompanyId).ToArray();
                            }
                            else
                            {
                                qbCompanyIds = getExtCompanies.OrderByDescending(f => f.CompanyId).Select(f => f.ExtCompanyId).ToArray();
                            }
                            var qbCompanyIdsCsv = string.Join(",", qbCompanyIds);
                            orderByExpression = x => x.OrderBy(m => qbCompanyIds == null ? 0 : AppDbContext.ArrayIndexOf(qbCompanyIdsCsv, m.CompanyId));
                            break;
                        case "AccountNumber":
                            orderByExpression = x => isAscending ?
                                x.OrderBy(m => AppDbContext.ParseAccountNumber(m.AccountNumber))
                             : x.OrderByDescending(m => AppDbContext.ParseAccountNumber(m.AccountNumber));
                            break;

                        default:
                            orderByExpression = null; // No specific order
                            break;
                    }

                    trialBalanceList = await uow.TrialBalanceRepository.FindLargeRecordsByPaging(x =>
                        (x.StartPeriod.Date >= DateTime.ParseExact(model.startDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date &&
                        x.EndPeriod.Date <= DateTime.ParseExact(model.endDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date) &&
                        model.companyIds.Contains(x.CompanyId) && x.TenantId == model.tenantId,
                        orderByExpression,
                        model.skipSize, model.pageSize, model.search);
                    int total = trialBalanceList.TotalRecords;
                    var staticDynamiclist = new List<StaticColumnNames>();
                    var staticDynamicObj = new StaticColumnNames();
                    var companyEntities = getExtCompanies.Select(f => f.EntityUID).Distinct().ToArray();

                    var fiscalCalendars = uow.FiscalCalendarRepository.FindByQueryable(x => getExtCompanies.Select(e => e.EntityUID).Contains(x.EntityGroup));

                    var journalHeaderIds = uow.JournalHeaderRepository.FindByQueryable(x => getExtCompanyIds.Contains(x.Company_ID) && x.Status != Enum.GetName(AjeStatusEnum.Posted)).Select(x => x.Journal_ID).ToArray();
                    var journals = uow.JournalDetailRepository.FindByQueryable(x => journalHeaderIds.Contains(x.Journal_ID)).ToList();

                    List<ExpandoObject> list = new List<ExpandoObject>();
                    //Loop on Month
                    foreach (var item in trialBalanceList.Data)
                    {
                        var accountNumber = allAccountMappings.Where(x => item.AccountNumber == x.Account_UID).FirstOrDefault();
                        dynamic tbMonthCompany = new List<ExpandoObject>();
                        var extCompany = getExtCompanies.Where(f => f.ExtCompanyId == item.CompanyId).FirstOrDefault();
                        var nwzCompanyId = extCompany?.CompanyId;
                        var fc = fiscalCalendars.Where(x => x.Entity_UID == nwzCompanyId && x.date_key.Date == item.StartPeriod.Date).FirstOrDefault();
                        var journalDetails = journals.Where(x => x.Account_UID == item.AccountNumber && x.Fiscal_Year == fc?.fy.ToString() && x.Fiscal_Period == fc?.fp.ToString()).ToArray();
                        var journal = journalDetails.FirstOrDefault();
                        var adjustingEntry = journalDetails.Sum(x => x.Debit) - journalDetails.Sum(x => x.Credit);
                        dynamic tbMonth = new ExpandoObject();
                        tbMonth.MonthStartDate = item.StartPeriod.ToString("yyyy-MM-dd");
                        tbMonth.MonthEndDate = item.EndPeriod.ToString("yyyy-MM-dd");
                        tbMonth.Period = item.EndPeriod.ToString("yyyy-MM");
                        var isCsutomColmAdded = false;

                        staticDynamiclist = new List<StaticColumnNames>();
                        dynamic tb = new ExpandoObject();
                        tb.TenantId = item.TenantId;
                        tb.CompanyId = item.CompanyId;
                        tb.StartPeriod = item.StartPeriod.ToString("yyyy-MM-dd");
                        tb.EndPeriod = item.EndPeriod.ToString("yyyy-MM-dd");
                        tb.CompanyName = item.CompanyName;
                        tb.Credit = item.Credit;
                        tb.Debit = item.Debit;
                        tb.ReportBasis = item.ReportBasis;
                        tb.Label = item.Label;
                        tb.Currency = item.Currency;
                        tb.AccountNumber = accountNumber?.New_Account_UID ?? "MISSING";
                        tb.AccountDescription = item.AccountDescription;
                        tb.Department = "";

                        tb.Entity = extCompany?.EntityUID;
                        tb.Company_Entity = nwzCompanyId;
                        tb.FS_ID = "";
                        tb.FS_Type = "";
                        tb.AdjustingEntry = journalDetails.Count() > 1 ? adjustingEntry : journal?.Debit != null ? Convert.ToDecimal(journal?.Debit) : Convert.ToDecimal(journal?.Credit) * -1;
                        tb.Period = fc?.fp.ToString() ?? string.Empty;
                        tb.Entity_Group = fc?.EntityGroup ?? string.Empty;
                        tb.Quarter = fc?.Quarter ?? string.Empty;
                        tb.Year = fc?.fy.ToString() ?? string.Empty;
                        tb.Date = fc?.date_key.ToString() ?? string.Empty;
                        tb.YearMonth = fc?.fiscal_year_month.ToString() ?? string.Empty;
                        tb.CustomColumnNames = customCoulmns;
                        tb.Net = CommonService.CalculateDebitCreditDifference(item.Debit != null ? (decimal)item.Debit : 0.00m, item.Credit != null ? (decimal)item.Credit : 0.00m);
                        tb.AdjustingTb = CommonService.CalculateDebitCreditSumInDecimal(tb.Net.Replace("(", "").Replace(")", ""), tb.AdjustingEntry.ToString());
                        if (!string.IsNullOrEmpty(tb.AccountNumber))
                        {
                            var categoryMappings = categoryCompanyMappings.Where(f => f.Company_ID == nwzCompanyId).ToList();
                            var accountMappings = allAccountMappings.Where(f => f.Company_ID == nwzCompanyId).ToList();
                            var getAccountMapping = accountMappings.Where(f => f.Account_UID == tb.AccountNumber).FirstOrDefault();
                            if (getAccountMapping != null)
                            {
                                isCsutomColmAdded = true;

                                tb.FS_ID = getAccountMapping.Category_UID;
                                tb.FS_Type = getAccountMapping.FS;

                                var getCategoryMapping = categoryMappings.Where(f => f.Category_UID == getAccountMapping.Category_UID && !string.IsNullOrEmpty(f.FS_Type)).GroupBy(f => f.FS_Type).ToList();
                                foreach (var column in customCoulmns)
                                {
                                    var categoryMap = getCategoryMapping.Where(f => f.Key == column).FirstOrDefault();
                                    if (categoryMap != null)
                                    {
                                        staticDynamicObj = new StaticColumnNames();
                                        staticDynamicObj.ColumnName = categoryMap.Key;
                                        staticDynamicObj.ColumnValue = (categoryMap.FirstOrDefault()?.FS_ID == "" || categoryMap.FirstOrDefault()?.FS_ID == null) ? "MISSING" : categoryMap.FirstOrDefault()?.FS_ID; ;

                                        staticDynamiclist.Add(staticDynamicObj);
                                        ((IDictionary<string, object>)tb)[categoryMap.Key] = staticDynamicObj.ColumnValue;
                                    }
                                    else
                                    {
                                        ((IDictionary<string, object>)tb)[column] = "MISSING";
                                    }
                                }
                            }
                        }

                        if (isCsutomColmAdded == false)
                        {
                            foreach (var column in customCoulmns)
                            {
                                ((IDictionary<string, object>)tb)[column] = "MISSING";
                            }
                        }

                        tb.CustomColumns = staticDynamiclist;
                        list.Add(tb);
                    }

                    data.Items = list;
                    data.TotalRecords = total;
                    return data;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public async Task<ReportResultDto> GetTBCurrentReportAsync(ReportDataDto model)
        {
            try
            {
                using (AppUnitOfWork uow = new AppUnitOfWork(_context))
                {
                    var viewModel = new ReportResultDto();
                    dynamic list = new List<ExpandoObject>();
                    dynamic companyList = new List<ExpandoObject>();
                    dynamic companyObj = new ExpandoObject();

                    if (string.IsNullOrEmpty(model.startDate) || model.startDate == model.endDate)
                    {
                        var financialStartMonth = await _companyService.GetCompanyStartFinancYRByCompanyId(uow, model.companyId, model.tenantId);
                        if (financialStartMonth > 0)
                        {
                            model.startDate = CommonService.GetStartDateOfFincByDate(financialStartMonth, model.endDate);
                        }
                    }

                    viewModel.StartDate = model.startDate;
                    viewModel.EndDate = model.endDate;

                    var customDateColumnlist = new List<string>();
                    var staticColumnlist = new List<string>();
                    var staticDynamiclist = new List<StaticColumnNames>();
                    var staticDateDynamiclist = new List<StaticColumnNames>();
                    var staticDynamicObj = new StaticColumnNames();

                    var extCompanies = uow.NwzCompanyRepository.Find(f => !string.IsNullOrEmpty(f.ExtCompanyId) && model.companyIds.Contains(f.ExtCompanyId.Trim()), null, 1, int.MaxValue).ToList();
                    var extCompanyIds = extCompanies.Select(f => f.CompanyId).ToArray();
                    var validFs_Type_List = uow.FSTypeRepository.FindByQueryable(f => f.ParentId != null).Select(f => f.Name).ToArray();

                    var allcategoryMappings = uow.CategoryMappingRepository.Find(f => validFs_Type_List.Contains(f.FS_Type) && extCompanyIds.Contains(f.Company_ID), null, 1, int.MaxValue);
                    var allaccountMappings = uow.AccountMappingRepository.Find(f => extCompanyIds.Contains(f.Company_ID), null, 1, int.MaxValue);

                    var alltrialBalanceList = await uow.TrialBalanceRepository.FindLargeRecordsInListAsync(x =>
                        (x.StartPeriod.Date >= DateTime.ParseExact(model.startDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date
                        && x.EndPeriod.Date <= DateTime.ParseExact(model.endDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date) &&
                        model.companyIds.Contains(x.CompanyId) &&
                        x.TenantId == model.tenantId, null, 1, int.MaxValue
                    );

                    var groupedColumnMonthTrialBalanceList = alltrialBalanceList.GroupBy(x => x.EndPeriod).ToList();
                    var customCoulmns = allcategoryMappings.GroupBy(f => f.FS_Type).Select(f => f.Key).ToList();
                    var customDateCoulmns = alltrialBalanceList.GroupBy(x => new { x.EndPeriod }).OrderBy(f => f.Key.EndPeriod).Select(f => CommonService.ConvertDateFormat(f.Key.EndPeriod.ToString("yyyy-MM-dd"))).ToList();
                    var entities = extCompanies.Select(f => f.EntityUID).Distinct().ToArray();

                    var fiscalCalendars = uow.FiscalCalendarRepository.FindByQueryable(x => extCompanies.Select(e => e.EntityUID).Contains(x.EntityGroup)).ToList();

                    foreach (var compId in model.companyIds)
                    {
                        var extComp = extCompanies.Where(f => f.ExtCompanyId == compId).FirstOrDefault();
                        var extCompId = extComp?.CompanyId;
                        var trialBalanceList = alltrialBalanceList.Where(f => f.CompanyId == compId).ToList();
                        var categoryMappings = allcategoryMappings.Where(f => f.Company_ID == extCompId).ToList();
                        var accountMappings = allaccountMappings.Where(f => f.Company_ID == extCompId).ToList();

                        list = new List<ExpandoObject>();

                        companyObj = new ExpandoObject();
                        companyObj.EntiyName = string.Join(" ,", entities);
                        companyObj.CustomColumnNames = customCoulmns;
                        companyObj.CustomDateColumnNames = customDateCoulmns;
                        var groupedAccountTrialBalanceList = trialBalanceList.GroupBy(x => new { AccountLabel = x.Label })
                              .Select(g => new
                              {
                                  AccountLabel = g.Key.AccountLabel,
                                  TrialBalances = g.ToList()
                              })
                      .ToList();


                        Parallel.ForEach(groupedAccountTrialBalanceList, account =>
                        {
                            var staticDynamiclist = new List<StaticColumnNames>();
                            var staticDateDynamiclist = new List<StaticColumnNames>();
                            dynamic tb = new ExpandoObject();

                            var firstTbAccnt = account.TrialBalances.First();
                            var label = firstTbAccnt.Label;
                            tb.TenantId = firstTbAccnt.TenantId;
                            tb.CompanyId = firstTbAccnt.CompanyId;
                            tb.StartPeriod = firstTbAccnt.StartPeriod.ToString("yyyy-MM-dd");
                            tb.EndPeriod = firstTbAccnt.EndPeriod.ToString("yyyy-MM-dd");
                            tb.CompanyName = extComp != null ? !string.IsNullOrEmpty(extComp.Name) ? extComp.Name : firstTbAccnt.CompanyName : firstTbAccnt.CompanyName;
                            tb.CompanyEntity = extCompId;
                            tb.Credit = firstTbAccnt.Credit;
                            tb.Debit = firstTbAccnt.Debit;
                            tb.ReportBasis = firstTbAccnt.ReportBasis;
                            tb.Label = account.AccountLabel;
                            tb.Currency = firstTbAccnt.Currency;
                            tb.AccountNumber = firstTbAccnt.AccountNumber?.Trim();
                            tb.AccountDescription = firstTbAccnt.AccountDescription;
                            tb.Department = "";
                            tb.Entity = "";
                            tb.FS_ID = "";
                            tb.FS_Type = "";
                            tb.AdjustingEntry = "";
                            var fc = fiscalCalendars.Where(x => x.Entity_UID == extCompId && x.date_key.Date == firstTbAccnt.StartPeriod.Date).FirstOrDefault();

                            tb.Entity = fc?.Entity_UID ?? string.Empty;
                            tb.Period = fc?.fp.ToString() ?? string.Empty;
                            tb.Entity_Group = fc?.EntityGroup ?? string.Empty;
                            tb.Quarter = fc?.Quarter ?? string.Empty;
                            tb.Year = fc?.fy.ToString() ?? string.Empty;
                            tb.Date = fc?.date_key.ToString() ?? string.Empty;
                            tb.YearMonth = fc?.fiscal_year_month.ToString() ?? string.Empty;
                            tb.CustomColumnNames = customCoulmns;
                            tb.CustomDateColumnNames = customDateCoulmns;
                            if (!string.IsNullOrEmpty(tb.AccountNumber))
                            {
                                var getAccountMapping = accountMappings.Where(f => f.Account_UID?.Trim() == tb.AccountNumber.Trim()).FirstOrDefault();
                                if (getAccountMapping != null)
                                {
                                    tb.FS_ID = getAccountMapping.Category_UID;
                                    tb.FS_Type = getAccountMapping.FS;
                                    var getCategoryMapping = categoryMappings.Where(f => f.Category_UID == getAccountMapping.Category_UID && !string.IsNullOrEmpty(f.FS_Type)).GroupBy(f => f.FS_Type).ToList();
                                    foreach (var categoryMap in getCategoryMapping)
                                    {
                                        staticDynamicObj = new StaticColumnNames();
                                        staticDynamicObj.ColumnName = categoryMap.Key;
                                        staticDynamicObj.ColumnValue = categoryMap.FirstOrDefault()?.FS_ID;
                                        staticDynamiclist.Add(staticDynamicObj);
                                    }
                                }
                            }

                            tb.CustomColumns = staticDynamiclist;
                            var groupedMonthTrialBalanceList = account.TrialBalances.GroupBy(x => new { Month = x.StartPeriod.Month, Year = x.StartPeriod.Year })
                            .Select(g => new
                            {
                                Month = g.Key.Month,
                                Year = g.Key.Year,
                                TrialBalances = g.ToList()
                            }).ToList();

                            foreach (var accountByMonth in groupedMonthTrialBalanceList)
                            {
                                staticDynamicObj = new StaticColumnNames();
                                staticDynamicObj.ColumnName = CommonService.ConvertDateFormat(accountByMonth.TrialBalances.First().EndPeriod.ToString("yyyy-MM-dd"));
                                staticDynamicObj.ColumnValue = CommonService.CalculateBalance(accountByMonth.TrialBalances.Where(f => f.Debit != null).Select(f => (decimal)f.Debit).ToArray()
                                , accountByMonth.TrialBalances.Where(f => f.Credit != null).Select(f => (decimal)f.Credit).ToArray());

                                staticDateDynamiclist.Add(staticDynamicObj);
                            }
                            tb.CustomDateColumns = staticDateDynamiclist;
                            list.Add(tb);
                        });
                        companyObj.Data = list;
                        companyList.Add(companyObj);
                    }

                    viewModel.Data = companyList;
                    return viewModel;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }


        public async Task<dynamic> GetTBCurrentReportPaginationAsync(ReportDataDto model)
        {
            try
            {
                using (AppUnitOfWork uow = new AppUnitOfWork(_context))
                {
                    dynamic data = new ExpandoObject();
                    dynamic list = new List<ExpandoObject>();


                    if (string.IsNullOrEmpty(model.startDate) || model.startDate == model.endDate)
                    {
                        var financialStartMonth = await _companyService.GetCompanyStartFinancYRByCompanyId(uow, model.companyId, model.tenantId);
                        if (financialStartMonth > 0)
                        {
                            model.startDate = CommonService.GetStartDateOfFincByDate(financialStartMonth, model.endDate);
                        }
                    }

                    var customDateColumnlist = new List<string>();
                    var staticColumnlist = new List<string>();
                    var staticDynamiclist = new List<StaticColumnNames>();
                    var staticDateDynamiclist = new List<StaticColumnNames>();
                    var staticDynamicObj = new StaticColumnNames();

                    var extCompanies = uow.NwzCompanyRepository.Find(f => !string.IsNullOrEmpty(f.ExtCompanyId) && model.companyIds.Contains(f.ExtCompanyId.Trim()), null, 1, int.MaxValue).ToList();
                    var extCompanyIds = extCompanies.Select(f => f.CompanyId).ToArray();
                    var validFs_Type_List = uow.FSTypeRepository.FindByQueryable(f => f.ParentId != null).Select(f => f.Name).ToArray();

                    var allcategoryMappings = uow.CategoryMappingRepository.Find(f => validFs_Type_List.Contains(f.FS_Type) && extCompanyIds.Contains(f.Company_ID), null, 1, int.MaxValue);
                    var allaccountMappings = uow.AccountMappingRepository.Find(f => extCompanyIds.Contains(f.Company_ID), null, 1, int.MaxValue);

                    var alltrialBalanceList = uow.TrialBalanceRepository.FindLargeRecords(x =>
                        (x.StartPeriod.Date >= DateTime.ParseExact(model.startDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date
                        && x.EndPeriod.Date <= DateTime.ParseExact(model.endDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date) &&
                        model.companyIds.Contains(x.CompanyId) &&
                        x.TenantId == model.tenantId, null, 1, int.MaxValue
                    );

                    var groupedColumnMonthTrialBalanceList = alltrialBalanceList.GroupBy(x => x.EndPeriod).ToList();
                    var customCoulmns = allcategoryMappings.GroupBy(f => f.FS_Type).Select(f => f.Key).ToList();
                    var customDateCoulmns = alltrialBalanceList.GroupBy(x => new { x.EndPeriod }).OrderBy(f => f.Key.EndPeriod).Select(f => CommonService.ConvertDateFormat(f.Key.EndPeriod.ToString("yyyy-MM-dd"))).ToList();
                    var entities = extCompanies.Select(f => f.EntityUID).Distinct().ToArray();

                    var groupedAccountTrialBalanceList = alltrialBalanceList.GroupBy(x => new { CompanyId = x.CompanyId, AccountLabel = x.Label })
                    .Select(g => new
                    {
                        CompanyId = g.Key.CompanyId,
                        AccountLabel = g.Key.AccountLabel,
                        TrialBalances = g.ToList()
                    })
                    .ToList();
                    var fiscalCalendars = uow.FiscalCalendarRepository.FindByQueryable(x => extCompanies.Select(e => e.EntityUID).Contains(x.EntityGroup)).ToList();

                    var filterdList = groupedAccountTrialBalanceList.Skip((int)model.skipSize).Take((int)model.pageSize).ToList();
                    data.TotalRecords = groupedAccountTrialBalanceList.Count();
                    var isCsutomColmAdded = false;
                    var isDateCsutomColmAdded = false;
                    Parallel.ForEach(filterdList, account =>
                    {
                        isCsutomColmAdded = false;
                        isDateCsutomColmAdded = false;

                        staticDynamiclist = new List<StaticColumnNames>();
                        staticDateDynamiclist = new List<StaticColumnNames>();
                        dynamic tb = new ExpandoObject();


                        var firstTbAccnt = account.TrialBalances.First();
                        var label = firstTbAccnt.Label;

                        var extComp = extCompanies.Where(f => f.ExtCompanyId == firstTbAccnt.CompanyId).FirstOrDefault();
                        var extCompId = extComp?.CompanyId;


                        tb.EntiyName = string.Join(" ,", entities);
                        tb.TenantId = firstTbAccnt.TenantId;
                        tb.CompanyId = firstTbAccnt.CompanyId;
                        tb.StartPeriod = firstTbAccnt.StartPeriod.ToString("yyyy-MM-dd");
                        tb.EndPeriod = firstTbAccnt.EndPeriod.ToString("yyyy-MM-dd");
                        tb.CompanyName = firstTbAccnt.CompanyName;
                        tb.CompanyEntity = extCompId;
                        tb.Credit = firstTbAccnt.Credit;
                        tb.Debit = firstTbAccnt.Debit;
                        tb.ReportBasis = firstTbAccnt.ReportBasis;
                        tb.Label = account.AccountLabel;
                        tb.Currency = firstTbAccnt.Currency;
                        tb.AccountNumber = firstTbAccnt.AccountNumber?.Trim();
                        tb.AccountDescription = firstTbAccnt.AccountDescription;
                        tb.Department = "";
                        tb.Entity = "";
                        tb.FS_ID = "";
                        tb.FS_Type = "";
                        tb.AdjustingEntry = "";
                        tb.CustomColumnNames = customCoulmns;
                        tb.CustomDateColumnNames = customDateCoulmns;
                        var fc = fiscalCalendars.Where(x => x.Entity_UID == extCompId && x.date_key.Date == firstTbAccnt.StartPeriod.Date).FirstOrDefault();

                        tb.Entity = extCompId;
                        tb.Period = fc?.fp.ToString() ?? string.Empty;
                        tb.Entity_Group = fc?.EntityGroup ?? string.Empty;
                        tb.Quarter = fc?.Quarter ?? string.Empty;
                        tb.Year = fc?.fy.ToString() ?? string.Empty;
                        tb.Date = fc?.date_key.ToString() ?? string.Empty;
                        tb.YearMonth = fc?.fiscal_year_month.ToString() ?? string.Empty;


                        if (!string.IsNullOrEmpty(tb.AccountNumber))
                        {
                            var getAccountMapping = allaccountMappings.Where(f => f.Company_ID == extCompId && f.Account_UID?.Trim() == tb.AccountNumber.Trim()).FirstOrDefault();
                            if (getAccountMapping != null)
                            {
                                isCsutomColmAdded = true;
                                tb.FS_ID = getAccountMapping.Category_UID;
                                tb.FS_Type = getAccountMapping.FS;
                                var getCategoryMapping = allcategoryMappings.Where(f => f.Company_ID == extCompId && f.Category_UID == getAccountMapping.Category_UID
                                && !string.IsNullOrEmpty(f.FS_Type)).GroupBy(f => f.FS_Type).ToList();
                                foreach (var column in customCoulmns)
                                {
                                    var categoryMap = getCategoryMapping.Where(f => f.Key == column).FirstOrDefault();
                                    if (categoryMap != null)
                                    {
                                        staticDynamicObj = new StaticColumnNames();
                                        staticDynamicObj.ColumnName = categoryMap.Key;
                                        staticDynamicObj.ColumnValue = categoryMap.FirstOrDefault()?.FS_ID;
                                        staticDynamiclist.Add(staticDynamicObj);
                                        ((IDictionary<string, object>)tb)[categoryMap.Key] = categoryMap.FirstOrDefault()?.FS_ID;
                                    }
                                    else
                                    {
                                        ((IDictionary<string, object>)tb)[column] = "";
                                    }
                                }
                            }
                        }
                        if (isCsutomColmAdded == false)
                        {
                            foreach (var column in customCoulmns)
                            {
                                ((IDictionary<string, object>)tb)[column] = "";
                            }
                        }
                        tb.CustomColumns = staticDynamiclist;

                        var groupedMonthTrialBalanceList = account.TrialBalances.GroupBy(x => new { Month = x.StartPeriod.Month, Year = x.StartPeriod.Year })
                        .Select(g => new
                        {
                            EndPeriod = CommonService.ConvertDateFormat(g.FirstOrDefault().EndPeriod.ToString("yyyy-MM-dd")),
                            Month = g.Key.Month,
                            Year = g.Key.Year,
                            TrialBalances = g.ToList()
                        }).ToList();

                        foreach (var dateColumn in customDateCoulmns)
                        {
                            var accountByMonth = groupedMonthTrialBalanceList.Where(f => f.EndPeriod == dateColumn).FirstOrDefault();
                            if (accountByMonth != null)
                            {
                                staticDynamicObj = new StaticColumnNames();
                                staticDynamicObj.ColumnName = dateColumn;
                                staticDynamicObj.ColumnValue = CommonService.CalculateBalance(accountByMonth.TrialBalances.Where(f => f.Debit != null).Select(f => (decimal)f.Debit).ToArray()
                                , accountByMonth.TrialBalances.Where(f => f.Credit != null).Select(f => (decimal)f.Credit).ToArray());
                                ((IDictionary<string, object>)tb)[staticDynamicObj.ColumnName] = staticDynamicObj.ColumnValue;
                                staticDateDynamiclist.Add(staticDynamicObj);
                            }
                            else
                            {
                                ((IDictionary<string, object>)tb)[dateColumn] = "";
                            }
                        }

                        tb.CustomDateColumns = staticDateDynamiclist;
                        list.Add(tb);
                    });

                    data.Items = list;
                    return data;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public async Task<string[]?> GetTBCurrentReportPaginationColumnsAsync(ReportDataDto model)
        {
            try
            {
                using (AppUnitOfWork uow = new AppUnitOfWork(_context))
                {
                    dynamic data = new ExpandoObject();
                    dynamic list = new List<ExpandoObject>();


                    if (string.IsNullOrEmpty(model.startDate) || model.startDate == model.endDate)
                    {
                        var financialStartMonth = await _companyService.GetCompanyStartFinancYRByCompanyId(uow, model.companyId, model.tenantId);
                        if (financialStartMonth > 0)
                        {
                            model.startDate = CommonService.GetStartDateOfFincByDate(financialStartMonth, model.endDate);
                        }
                    }

                    var customDateColumnlist = new List<string>();
                    var staticColumnlist = new List<string>();
                    var staticDynamiclist = new List<StaticColumnNames>();
                    var staticDateDynamiclist = new List<StaticColumnNames>();
                    var staticDynamicObj = new StaticColumnNames();

                    var extCompanies = uow.NwzCompanyRepository.Find(f => !string.IsNullOrEmpty(f.ExtCompanyId) && model.companyIds.Contains(f.ExtCompanyId.Trim()), null, 1, int.MaxValue).ToList();
                    var extCompanyIds = extCompanies.Select(f => f.CompanyId).ToArray();
                    var validFs_Type_List = uow.FSTypeRepository.FindByQueryable(f => f.ParentId != null).Select(f => f.Name).ToArray();

                    var allcategoryMappings = uow.CategoryMappingRepository.Find(f => validFs_Type_List.Contains(f.FS_Type) && extCompanyIds.Contains(f.Company_ID), null, 1, int.MaxValue);
                    var allaccountMappings = uow.AccountMappingRepository.Find(f => extCompanyIds.Contains(f.Company_ID), null, 1, int.MaxValue);

                    var alltrialBalanceList = uow.TrialBalanceRepository.FindLargeRecords(x =>
                        (x.StartPeriod.Date >= DateTime.ParseExact(model.startDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date
                        && x.EndPeriod.Date <= DateTime.ParseExact(model.endDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date) &&
                        model.companyIds.Contains(x.CompanyId) &&
                        x.TenantId == model.tenantId, null, 1, int.MaxValue
                    );

                    var customCoulmns = allcategoryMappings.GroupBy(f => f.FS_Type).Select(f => f.Key).ToList();
                    var customDateCoulmns = alltrialBalanceList.GroupBy(x => new { x.EndPeriod }).OrderBy(f => f.Key.EndPeriod).Select(f => CommonService.ConvertDateFormat(f.Key.EndPeriod.ToString("yyyy-MM-dd"))).ToList();

                    customCoulmns.AddRange(customDateCoulmns);
                    return customCoulmns.ToArray();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public async Task<string[]?> GetTBReportPaginationColumnsAsync(ReportDataDto model)
        {
            try
            {
                using (AppUnitOfWork uow = new AppUnitOfWork(_context))
                {
                    dynamic tbCompanyMonthsList = new List<ExpandoObject>();
                    dynamic data = new ExpandoObject();
                    var getExtCompanyIds = uow.NwzCompanyRepository.Find(f => !string.IsNullOrEmpty(f.ExtCompanyId) && model.companyIds.Contains(f.ExtCompanyId.Trim()), null, 1, int.MaxValue).Select(f => f.CompanyId).ToArray();
                    var validFs_Type_List = uow.FSTypeRepository.FindByQueryable(f => f.ParentId != null).Select(f => f.Name).ToArray();

                    var categoryCompanyMappings = uow.CategoryMappingRepository.Find(f => validFs_Type_List.Contains(f.FS_Type) && getExtCompanyIds.Contains(f.Company_ID), null, 1, int.MaxValue).ToList();
                    var customCoulmns = categoryCompanyMappings.GroupBy(f => f.FS_Type).Select(f => f.Key).ToList();
                    return customCoulmns.ToArray();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }



        public async Task<ReportResultDto> GetTBDumpReportAsync(ReportDataDto model)
        {
            try
            {
                using (AppUnitOfWork uow = new AppUnitOfWork(_context))
                {
                    var viewModel = new ReportResultDto();
                    dynamic list = new List<ExpandoObject>();
                    viewModel.EndDate = model.endDate;
                    DataTable dtDates = CommonService.GetMonthStartEndDate(model.endDate);
                    var getLastMonthOfPeriod = dtDates.AsEnumerable().Last();
                    var reportImportInfo = uow.ImportedReportsInfoRepository.Find(x => x.TenantId == model.tenantId && x.ReportId == model.reportId && x.CompanyId == model.companyId).ToList();
                    var lastMonthStartDate = Convert.ToString(getLastMonthOfPeriod["StartDay"]);
                    var lastMonthEndDate = Convert.ToString(getLastMonthOfPeriod["EndDay"]);

                    var staticColumnlist = new List<string>();
                    var staticDynamiclist = new List<StaticColumnNames>();
                    var staticDynamicObj = new StaticColumnNames();

                    var checkIfLastMonthImported = reportImportInfo.Any(info =>
                            info.StartPeriod == lastMonthStartDate);
                    if (!checkIfLastMonthImported)
                    {
                        viewModel.IsDataImported = false;
                        viewModel.Months = dtDates.AsEnumerable()
                            .Where(dateRow => !reportImportInfo.Any(info =>
                                info.StartPeriod == Convert.ToString(dateRow["StartDay"]))).
                                Select(x => new MonthsDto { StartDate = Convert.ToString(x["StartDay"]), EndDate = Convert.ToString(x["EndDay"]) }).ToList();
                        return viewModel;
                    }
                    viewModel.IsDataImported = true;

                    var extCompanyId = uow.NwzCompanyRepository.Find(f => !string.IsNullOrEmpty(f.ExtCompanyId) && f.ExtCompanyId.Trim() == model.companyId, null, 1, int.MaxValue).Select(f => f.CompanyId).FirstOrDefault();
                    var validFs_Type_List = uow.FSTypeRepository.FindByQueryable(f => f.ParentId != null).Select(f => f.Name).ToArray();

                    var categoryMappings = uow.CategoryMappingRepository.Find(f => validFs_Type_List.Contains(f.FS_Type) && f.Company_ID == extCompanyId, null, 1, int.MaxValue).ToList();

                    var accountMappings = uow.AccountMappingRepository.Find(f => f.Company_ID == extCompanyId, null, 1, int.MaxValue).ToList();
                    var trialBalanceList = uow.TrialBalanceRepository.Find(x =>
                        (x.StartPeriod.Date >= DateTime.ParseExact(lastMonthStartDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date
                        && x.EndPeriod.Date <= DateTime.ParseExact(lastMonthEndDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date) &&
                        x.CompanyId == model.companyId &&
                        x.TenantId == model.tenantId, null, 1, int.MaxValue
                    ).ToList();

                    var customCoulmns = categoryMappings.GroupBy(f => f.FS_Type).Select(f => f.Key).ToList();

                    foreach (var item in trialBalanceList)
                    {
                        staticDynamiclist = new List<StaticColumnNames>();
                        dynamic tb = new ExpandoObject();
                        tb.TenantId = item.TenantId;
                        tb.CompanyId = item.CompanyId;
                        tb.StartPeriod = item.StartPeriod.ToString("yyyy-MM-dd");
                        tb.EndPeriod = item.EndPeriod.ToString("yyyy-MM-dd");
                        tb.CompanyName = item.CompanyName;
                        tb.Credit = item.Credit;
                        tb.Debit = item.Debit;
                        tb.ReportBasis = item.ReportBasis;
                        tb.Label = item.Label;
                        tb.Currency = item.Currency;
                        tb.AccountNumber = item.AccountNumber;
                        tb.AccountDescription = item.AccountDescription;
                        tb.Department = "";
                        tb.Entity = "";
                        tb.FS_ID = "";
                        tb.FS_Type = "";
                        tb.AdjustingEntry = "";
                        tb.CustomColumnNames = customCoulmns;
                        tb.Net = CommonService.CalculateDebitCreditDifference(item.Debit != null ? (decimal)item.Debit : 0.00m, item.Credit != null ? (decimal)item.Credit : 0.00m);
                        tb.AdjustingTb = tb.Net;
                        if (!string.IsNullOrEmpty(tb.AccountNumber))
                        {
                            var getAccountMapping = accountMappings.Where(f => f.Account_UID == tb.AccountNumber).FirstOrDefault();
                            if (getAccountMapping != null)
                            {
                                tb.FS_ID = getAccountMapping.Category_UID;
                                tb.FS_Type = getAccountMapping.FS;


                                var getCategoryMapping = categoryMappings.Where(f => f.Category_UID == getAccountMapping.Category_UID && !string.IsNullOrEmpty(f.FS_Type)).GroupBy(f => f.FS_Type).ToList();
                                foreach (var categoryMap in getCategoryMapping)
                                {
                                    staticDynamicObj = new StaticColumnNames();
                                    staticDynamicObj.ColumnName = categoryMap.Key;
                                    staticDynamicObj.ColumnValue = categoryMap.FirstOrDefault()?.FS_ID;
                                    staticDynamiclist.Add(staticDynamicObj);
                                }
                            }
                        }
                        tb.CustomColumns = staticDynamiclist;
                        list.Add(tb);
                    }
                    viewModel.Data = list;
                    return viewModel;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public async Task<ReportResultDto> GetTBInvalidExceptionReportsAsync(ReportDataDto model)
        {

            try
            {
                using (AppUnitOfWork uow = new AppUnitOfWork(_context))
                {
                    var viewModel = new ReportResultDto();
                    var accountMappings = uow.AccountMappingRepository.FindByQueryable(x => !x.IsDeleted);
                    string[] companyIds = model.companyIds.Where(id => !string.IsNullOrEmpty(id)).ToArray();

                    var getExtCompanies = uow.NwzCompanyRepository.Find(f => !string.IsNullOrEmpty(f.ExtCompanyId) && companyIds.Contains(f.ExtCompanyId.Trim()), null, 1, int.MaxValue).ToList();

                    var getExtCompanyIds = getExtCompanies.Select(f => f.CompanyId.Trim()).ToArray();

                    var validFS_Types = uow.FinancialReportRepository.FindByQueryable(f => getExtCompanyIds.Contains(f.Entity)).ToList();
                    var validFs_CustomColumns = validFS_Types.Select(f => f.FsType).Distinct().ToList();

                    var categoryMappings = uow.CategoryMappingRepository.FindByQueryable(x => !x.IsDeleted && !string.IsNullOrEmpty(x.Company_ID));
                    var categoryCompanyMappings = categoryMappings.Where(f => !string.IsNullOrEmpty(f.FS_Type) && validFs_CustomColumns.ToArray().Contains(f.FS_Type) && getExtCompanyIds.Contains(f.Company_ID.Trim())).ToList();

                    var companies = uow.NwzCompanyRepository.FindByQueryable(x => (companyIds.Any() && (!x.IsDeleted && companyIds.Any() && companyIds.Contains(x.ExtCompanyId))) || (!companyIds.Any() && !x.IsDeleted)).Select(x => x.ExtCompanyId).Distinct().ToList();//&& model.EntityIds.Contains(x.EntityUID)

                    var tbAccounts = uow.TrialBalanceRepository.FindLargeRecords(x =>
                       (x.StartPeriod.Date >= DateTime.ParseExact(model.startDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date
                       && x.EndPeriod.Date <= DateTime.ParseExact(model.endDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date) &&
                       model.companyIds.Contains(x.CompanyId) &&
                       x.TenantId == model.tenantId, null, 1, int.MaxValue
                   );

                    var accountMappingList = new List<AccountMappingDto>();
                    foreach (var item in getExtCompanies)
                    {
                       accountMappingList.AddRange(tbAccounts.Where(x => x.CompanyId == item.ExtCompanyId && (string.IsNullOrEmpty(x.AccountNumber) || string.IsNullOrWhiteSpace(x.AccountNumber))).GroupBy(f => f.AccountDescription).ToList().
                      Select(a => new AccountMappingDto()
                      {
                          Name = a.First().AccountDescription,
                          New_Account_UID = a.First().AccountNumber,
                          Account_UID = a.First().AccountNumber,
                          Company_ID = a.First().CompanyId,
                      })
                  .ToList());

                        var getAccountFromAccountMappings = accountMappings.Where(f => f.Company_ID == item.CompanyId && !string.IsNullOrEmpty(f.Account_UID)
                        && !string.IsNullOrWhiteSpace(f.Account_UID)).Select(f => f.Account_UID.Trim()).ToList();


                        accountMappingList.AddRange(tbAccounts.Where(x => x.CompanyId == item.ExtCompanyId && !string.IsNullOrEmpty(x.AccountNumber) && !getAccountFromAccountMappings.Contains(x.AccountNumber)).GroupBy(x => x.AccountNumber)
                        .Select(a => new AccountMappingDto()
                        {
                            Name = a.First().AccountDescription,
                            New_Account_UID = a.First().AccountNumber,
                            Account_UID = a.First().AccountNumber,
                            Company_ID = a.First().CompanyId,
                        })
                        .ToList());
                    }


                    var entityAccountMappings = accountMappings.Where(a => validFs_CustomColumns.Contains(a.FS_Type) && getExtCompanyIds.Contains(a.Company_ID));
                    var accountMappingFSNull = entityAccountMappings.Where(a => string.IsNullOrEmpty(a.FS_ID)).Select(x => new AccountMappingDto()
                    {
                        Name = x.Platform_Meta_Data,
                        Account_UID = x.Account_UID,
                        Company_ID = x.Company_ID,
                        FS = x.FS,
                        FS_TypeID = x.FS_Type,
                        FS_ID = x.FS_ID,
                        Currency = x.Currency,
                        Subcategory1_UID = x.Subcategory1_UID != x.Entity_ID ? x.Subcategory1_UID : string.Empty,
                        Subcategory2_UID = x.Subcategory2_UID != x.Location_ID ? x.Subcategory2_UID : string.Empty,
                        Subcategory3_UID = x.Subcategory3_UID ?? string.Empty,
                        Subcategory4_UID = x.Subcategory4_UID ?? string.Empty,
                        Subcategory5_UID = x.Subcategory5_UID ?? string.Empty,
                    }).ToList();
                    accountMappingList.AddRange(accountMappingFSNull);
                    var groupByCompany = accountMappingList.GroupBy(f => f.Company_ID).ToList();
                    dynamic result = new List<ExpandoObject>();

                    foreach (var compGroup in groupByCompany)
                    {
                        var extCompany = getExtCompanies.Where(f => f.ExtCompanyId == compGroup.Key).FirstOrDefault();
                        var companyCustomColumns = validFS_Types.Where(f => f.Entity == extCompany?.CompanyId || f.Entity == compGroup.Key).Select(x => x.FsType).ToArray();

                        foreach (var modelData in compGroup.Where(f => string.IsNullOrEmpty(f.Account_UID) || string.IsNullOrWhiteSpace(f.Account_UID)))
                        {
                            var extsCompany = getExtCompanies.Where(f => f.CompanyId == modelData.Company_ID || f.ExtCompanyId == modelData.Company_ID).FirstOrDefault();
                            dynamic tb = new ExpandoObject();
                            tb.AccountDescription = modelData.Name;
                            tb.Account_UID = modelData.Account_UID;
                            tb.AccountNumber = modelData.Account_UID;
                            tb.Entity = extsCompany?.CompanyId;
                            result.Add(tb);
                        }

                        foreach (var modelData in compGroup.Where(f => !string.IsNullOrEmpty(f.Account_UID) && !string.IsNullOrWhiteSpace(f.Account_UID)).DistinctBy(x => x.Account_UID))
                        {
                            var extsCompany = getExtCompanies.Where(f => f.CompanyId == modelData.Company_ID || f.ExtCompanyId == modelData.Company_ID).FirstOrDefault();
                            dynamic tb = new ExpandoObject();
                            tb.AccountDescription = modelData.Name;
                            tb.Account_UID = modelData.Account_UID;
                            tb.AccountNumber = modelData.Account_UID;
                            tb.Entity = extsCompany?.CompanyId;
                            result.Add(tb);
                        }
                    }
                    viewModel.Data = result;
                    return viewModel;
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Error while fetching data: " + ex.Message);
            }
        }


        public async Task<ReportResultDto> GetTBInvalidExceptionReportsAsync(AppUnitOfWork uow, ReportDataDto model)
        {
           
            try
            {
                var viewModel = new ReportResultDto();
                var accountMappings = uow.AccountMappingRepository.FindByQueryable(x => !x.IsDeleted);
                string[] companyIds = model.companyIds.Where(id => !string.IsNullOrEmpty(id)).ToArray();

                var getExtCompanies = uow.NwzCompanyRepository.Find(f => !string.IsNullOrEmpty(f.ExtCompanyId) && companyIds.Contains(f.ExtCompanyId.Trim()), null, 1, int.MaxValue).ToList();

                var getExtCompanyIds = getExtCompanies.Select(f => f.CompanyId.Trim()).ToArray();

                var validFS_Types = uow.FinancialReportRepository.FindByQueryable(f => getExtCompanyIds.Contains(f.Entity)).ToList();
                var validFs_CustomColumns = validFS_Types.Select(f => f.FsType).Distinct().ToList();

                var categoryMappings = uow.CategoryMappingRepository.FindByQueryable(x => !x.IsDeleted && !string.IsNullOrEmpty(x.Company_ID));
                var categoryCompanyMappings = categoryMappings.Where(f => !string.IsNullOrEmpty(f.FS_Type) && validFs_CustomColumns.ToArray().Contains(f.FS_Type) && getExtCompanyIds.Contains(f.Company_ID.Trim())).ToList();

                var companies = uow.NwzCompanyRepository.FindByQueryable(x => (companyIds.Any() && (!x.IsDeleted && companyIds.Any() && companyIds.Contains(x.ExtCompanyId))) || (!companyIds.Any() && !x.IsDeleted)).Select(x => x.ExtCompanyId).Distinct().ToList();//&& model.EntityIds.Contains(x.EntityUID)

                var tbAccounts = uow.TrialBalanceRepository.FindLargeRecords(x =>
                   (x.StartPeriod.Date >= DateTime.ParseExact(model.startDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date
                   && x.EndPeriod.Date <= DateTime.ParseExact(model.endDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date) &&
                   model.companyIds.Contains(x.CompanyId) &&
                   x.TenantId == model.tenantId, null, 1, int.MaxValue
               );

                var accountMappingList = new List<AccountMappingDto>();
                foreach (var item in getExtCompanies)
                {
                   
                    accountMappingList.AddRange(tbAccounts.Where(x => x.CompanyId == item.ExtCompanyId && (string.IsNullOrEmpty(x.AccountNumber) || string.IsNullOrWhiteSpace(x.AccountNumber))).GroupBy(f => f.AccountDescription).ToList().
                  Select(a => new AccountMappingDto()
                  {
                      Name = a.First().AccountDescription,
                      New_Account_UID = a.First().AccountNumber,
                      Account_UID = a.First().AccountNumber,
                      Company_ID = a.First().CompanyId,
                  })
              .ToList());

                    var getAccountFromAccountMappings = accountMappings.Where(f => f.Company_ID == item.CompanyId && !string.IsNullOrEmpty(f.Account_UID)
                    && !string.IsNullOrWhiteSpace(f.Account_UID)).Select(f => f.Account_UID.Trim()).ToList();


                    accountMappingList.AddRange(tbAccounts.Where(x => x.CompanyId == item.ExtCompanyId && !string.IsNullOrEmpty(x.AccountNumber) && !getAccountFromAccountMappings.Contains(x.AccountNumber)).GroupBy(x => x.AccountNumber)
                    .Select(a => new AccountMappingDto()
                    {
                        Name = a.First().AccountDescription,
                        New_Account_UID = a.First().AccountNumber,
                        Account_UID = a.First().AccountNumber,
                        Company_ID = a.First().CompanyId,
                    })
                    .ToList());
                }


                var entityAccountMappings = accountMappings.Where(a => validFs_CustomColumns.Contains(a.FS_Type) && getExtCompanyIds.Contains(a.Company_ID));
                var accountMappingFSNull = entityAccountMappings.Where(a => string.IsNullOrEmpty(a.FS_ID)).Select(x => new AccountMappingDto()
                {
                    Name = x.Platform_Meta_Data,
                    Account_UID = x.Account_UID,
                    Company_ID = x.Company_ID,
                    FS = x.FS,
                    FS_TypeID = x.FS_Type,
                    FS_ID = x.FS_ID,
                    Currency = x.Currency,
                    Subcategory1_UID = x.Subcategory1_UID != x.Entity_ID ? x.Subcategory1_UID : string.Empty,
                    Subcategory2_UID = x.Subcategory2_UID != x.Location_ID ? x.Subcategory2_UID : string.Empty,
                    Subcategory3_UID = x.Subcategory3_UID ?? string.Empty,
                    Subcategory4_UID = x.Subcategory4_UID ?? string.Empty,
                    Subcategory5_UID = x.Subcategory5_UID ?? string.Empty,
                }).ToList();
                accountMappingList.AddRange(accountMappingFSNull);
                var groupByCompany = accountMappingList.GroupBy(f => f.Company_ID).ToList();
                dynamic result = new List<ExpandoObject>();

                foreach (var compGroup in groupByCompany)
                {
                    var extCompany = getExtCompanies.Where(f => f.ExtCompanyId == compGroup.Key).FirstOrDefault();
                    var companyCustomColumns = validFS_Types.Where(f => f.Entity == extCompany?.CompanyId || f.Entity == compGroup.Key).Select(x => x.FsType).ToArray();

                    foreach (var modelData in compGroup.Where(f => string.IsNullOrEmpty(f.Account_UID) || string.IsNullOrWhiteSpace(f.Account_UID)))
                    {
                        var extsCompany = getExtCompanies.Where(f => f.CompanyId == modelData.Company_ID || f.ExtCompanyId == modelData.Company_ID).FirstOrDefault();
                        dynamic tb = new ExpandoObject();
                        tb.AccountDescription = modelData.Name;
                        tb.Account_UID = modelData.Account_UID;
                        tb.AccountNumber = modelData.Account_UID;
                        tb.Entity = extsCompany?.CompanyId;
                        result.Add(tb);
                    }

                    foreach (var modelData in compGroup.Where(f => !string.IsNullOrEmpty(f.Account_UID) && !string.IsNullOrWhiteSpace(f.Account_UID)).DistinctBy(x => x.Account_UID))
                    {
                        var extsCompany = getExtCompanies.Where(f => f.CompanyId == modelData.Company_ID || f.ExtCompanyId == modelData.Company_ID).FirstOrDefault();
                        dynamic tb = new ExpandoObject();
                        tb.AccountDescription = modelData.Name;
                        tb.Account_UID = modelData.Account_UID;
                        tb.AccountNumber = modelData.Account_UID;
                        tb.Entity = extsCompany?.CompanyId;
                        result.Add(tb);
                    }
                }
                viewModel.Data = result;
                return viewModel;
            }
            catch (Exception ex)
            {
                throw new Exception("Error while fetching data: " + ex.Message);
            }
        }

        public async Task<dynamic> GetTBExceptionReportPaginationAsync(ReportDataDto model)
        {
            
            try
            {
                using (AppUnitOfWork uow = new AppUnitOfWork(_context))
                {
                    dynamic obj = new ExpandoObject();
                    var accountMappings = uow.AccountMappingRepository.FindByQueryable(x => !x.IsDeleted);
                    string[] companyIds = model.companyIds.Where(id => !string.IsNullOrEmpty(id)).ToArray();

                    var getExtCompanies = uow.NwzCompanyRepository.Find(f => !string.IsNullOrEmpty(f.ExtCompanyId) && companyIds.Contains(f.ExtCompanyId.Trim()), null, 1, int.MaxValue).ToList();

                    var getExtCompanyIds = getExtCompanies.Select(f => f.CompanyId.Trim()).ToArray();

                    var validFS_Types = uow.FinancialReportRepository.FindByQueryable(f => getExtCompanyIds.Contains(f.Entity)).ToList();
                    var validFs_CustomColumns = validFS_Types.Select(f => f.FsType).Distinct().ToList();

                    var categoryMappings = uow.CategoryMappingRepository.FindByQueryable(x => !x.IsDeleted && !string.IsNullOrEmpty(x.Company_ID));
                    var categoryCompanyMappings = categoryMappings.Where(f => !string.IsNullOrEmpty(f.FS_Type) && validFs_CustomColumns.ToArray().Contains(f.FS_Type) && getExtCompanyIds.Contains(f.Company_ID.Trim())).ToList();

                    var companies = uow.NwzCompanyRepository.FindByQueryable(x => (companyIds.Any() && (!x.IsDeleted && companyIds.Any() && companyIds.Contains(x.ExtCompanyId))) || (!companyIds.Any() && !x.IsDeleted)).Select(x => x.ExtCompanyId).Distinct().ToList();//&& model.EntityIds.Contains(x.EntityUID)
                    var chartOfAccounts = uow.ChartOfAccountsRepository.FindByQueryable(x => !x.IsDeleted && x.TenantId == model.tenantId && x.AccountNumber != null && companies.Any() && companies.Contains(x.CompanyId));

                    var tbAccounts = uow.TrialBalanceRepository.FindLargeRecords(x =>
                     (x.StartPeriod.Date >= DateTime.ParseExact(model.startDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date
                     && x.EndPeriod.Date <= DateTime.ParseExact(model.endDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date) &&
                     model.companyIds.Contains(x.CompanyId) &&
                     x.TenantId == model.tenantId, null, 1, int.MaxValue, model.search
                 );

                    var accountMappingList = new List<AccountMappingDto>();
                    foreach (var item in getExtCompanies)
                    {
                        accountMappingList.AddRange(tbAccounts.Where(x => x.CompanyId == item.ExtCompanyId && string.IsNullOrEmpty(x.AccountNumber) || string.IsNullOrWhiteSpace(x.AccountNumber)).GroupBy(f => f.AccountDescription).ToList().
                        Select(a => new AccountMappingDto()
                        {
                            Name = a.First().AccountDescription,
                            New_Account_UID = a.First().AccountNumber,
                            Account_UID = a.First().AccountNumber,
                            Company_ID = a.First().CompanyId,
                        })
                    .ToList());

                        var getAccountFromAccountMappings = accountMappings.Where(f => f.Company_ID == item.CompanyId && !string.IsNullOrEmpty(f.Account_UID)
                        && !string.IsNullOrWhiteSpace(f.Account_UID)).Select(f => f.Account_UID.Trim()).ToList();


                        accountMappingList.AddRange(tbAccounts.Where(x => x.CompanyId == item.ExtCompanyId && !string.IsNullOrEmpty(x.AccountNumber) && !getAccountFromAccountMappings.Contains(x.AccountNumber)).GroupBy(x => x.AccountNumber)
                        .Select(a => new AccountMappingDto()
                        {
                            Name = a.First().AccountDescription,
                            New_Account_UID = a.First().AccountNumber,
                            Account_UID = a.First().AccountNumber,
                            Company_ID = a.First().CompanyId,
                        })
                        .ToList());
                    }

                    var entityAccountMappings = accountMappings.Where(a => validFs_CustomColumns.Contains(a.FS_Type) && getExtCompanyIds.Contains(a.Entity_ID));
                    var accountMappingFSNull = entityAccountMappings.Where(a => string.IsNullOrEmpty(a.FS_ID)).Select(x => new AccountMappingDto()
                    {
                        Name = x.Platform_Meta_Data,
                        Account_UID = x.Account_UID,
                        Company_ID = x.Company_ID,
                        FS = x.FS,
                        FS_TypeID = x.FS_Type,
                        FS_ID = x.FS_ID,
                        Currency = x.Currency,
                        Subcategory1_UID = x.Subcategory1_UID != x.Entity_ID ? x.Subcategory1_UID : string.Empty,
                        Subcategory2_UID = x.Subcategory2_UID != x.Location_ID ? x.Subcategory2_UID : string.Empty,
                        Subcategory3_UID = x.Subcategory3_UID ?? string.Empty,
                        Subcategory4_UID = x.Subcategory4_UID ?? string.Empty,
                        Subcategory5_UID = x.Subcategory5_UID ?? string.Empty,
                    }).ToList();
                    accountMappingList.AddRange(accountMappingFSNull);
                    dynamic result = new List<ExpandoObject>();

                    var groupByCompany = accountMappingList.GroupBy(f => f.Company_ID).ToList();
                    foreach (var compGroup in groupByCompany)
                    {
                        var extCompany = getExtCompanies.Where(f => f.ExtCompanyId == compGroup.Key).FirstOrDefault();
                        var companyCustomColumns = validFS_Types.Where(f => f.Entity == extCompany?.CompanyId || f.Entity == compGroup.Key).Select(x => x.FsType).ToArray();

                        foreach (var modelData in compGroup.Where(f => string.IsNullOrEmpty(f.Account_UID) || string.IsNullOrWhiteSpace(f.Account_UID)))
                        {
                            var extsCompany = getExtCompanies.Where(f => f.CompanyId == modelData.Company_ID || f.ExtCompanyId == modelData.Company_ID).FirstOrDefault();
                            dynamic tb = new ExpandoObject();
                            tb.AccountDescription = modelData.Name;
                            tb.Account_UID = modelData.Account_UID;
                            tb.AccountNumber = modelData.Account_UID;
                            tb.Entity = extsCompany?.CompanyId;
                            result.Add(tb);
                        }

                        foreach (var modelData in compGroup.Where(f => !string.IsNullOrEmpty(f.Account_UID) && !string.IsNullOrWhiteSpace(f.Account_UID)).DistinctBy(x => x.Account_UID))
                        {
                            var extsCompany = getExtCompanies.Where(f => f.CompanyId == modelData.Company_ID || f.ExtCompanyId == modelData.Company_ID).FirstOrDefault();
                            dynamic tb = new ExpandoObject();
                            tb.AccountDescription = modelData.Name;
                            tb.Account_UID = modelData.Account_UID;
                            tb.AccountNumber = modelData.Account_UID;
                            tb.Entity = extsCompany?.CompanyId;
                            result.Add(tb);
                        }
                    }
                    obj.TotalRecords = ((List<ExpandoObject>)result).Count;
                    obj.Items = result;
                    return obj;
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Error while fetching data: " + ex.Message);
            }
        }

        public async Task<TBReportResponse> GetTrialReportsAsync(AppUnitOfWork uow, ReportDataDto model)
        {
            try
            {
                IAppCache cache = new CachingService();
                dynamic trialBalanceDto = new TBReportResponse();
                dynamic tbCompanyMonthsList = new List<ExpandoObject>();
                dynamic tbMappedUnMappedList = new List<ExpandoObject>();
                var getExtCompanies = uow.NwzCompanyRepository.Find(f => !string.IsNullOrEmpty(f.ExtCompanyId) && model.companyIds.Contains(f.ExtCompanyId.Trim()), null, 1, int.MaxValue).ToList();
                var getExtCompanyIds = getExtCompanies.Select(f => f.CompanyId).ToArray();
                var validFs_Type_List = uow.FSTypeRepository.FindByQueryable(f => f.ParentId != null).Select(f => f.Name).ToArray();

                var categoryCompanyMappings = uow.CategoryMappingRepository.Find(f => validFs_Type_List.Contains(f.FS_Type) && getExtCompanyIds.Contains(f.Company_ID), null, 1, int.MaxValue).ToList();
                var customCoulmns = categoryCompanyMappings.GroupBy(f => f.FS_Type).Select(f => f.Key).ToList();
                var allAccountMappings = uow.AccountMappingRepository.Find(f => getExtCompanyIds.Contains(f.Company_ID.Trim()), null, 1, int.MaxValue).ToList();

                if (string.IsNullOrEmpty(model.startDate) || model.startDate == model.endDate)
                {
                    var financialStartMonth = await _companyService.GetCompanyStartFinancYRByCompanyId(uow, model.companyIds[0], model.tenantId);
                    if (financialStartMonth > 0)
                    {
                        model.startDate = CommonService.GetStartDateOfFincByDate(financialStartMonth, model.endDate);
                    }
                }

                var fiscalCalendars = uow.FiscalCalendarRepository.FindByQueryable(x => getExtCompanies.Select(e => e.EntityUID).Contains(x.EntityGroup));

                var journalHeaderIds = uow.JournalHeaderRepository.FindByQueryable(x => getExtCompanyIds.Contains(x.Company_ID) && x.Status != Enum.GetName(AjeStatusEnum.Posted)).Select(x => x.Journal_ID).ToArray();
                var journals = uow.JournalDetailRepository.FindByQueryable(x => journalHeaderIds.Contains(x.Journal_ID)).ToList();

                //Group By Month
                var allByTrialBalanceList = await uow.TrialBalanceRepository.FindLargeRecordsInListAsync(x =>
                      (x.StartPeriod.Date >= DateTime.ParseExact(model.startDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date
                      && x.EndPeriod.Date <= DateTime.ParseExact(model.endDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date) &&
                      model.companyIds.Contains(x.CompanyId) &&
                      x.TenantId == model.tenantId, null, 1, int.MaxValue);

                var monthByTrialBalanceList = allByTrialBalanceList.GroupBy(x => new { x.EndPeriod }).OrderBy(f => f.Key.EndPeriod);
                var tbByCompanies = allByTrialBalanceList.GroupBy(x => x.CompanyId);

                var staticDynamiclist = new List<StaticColumnNames>();
                var staticDynamicObj = new StaticColumnNames();
                dynamic periodBalanceList = new List<ExpandoObject>();
                if (monthByTrialBalanceList.Any())
                {
                    var companyEntities = getExtCompanies.Select(f => f.EntityUID).Distinct().ToArray();
                    //Loop on Month
                    foreach (var monthlyTrialBalnce in monthByTrialBalanceList)
                    {
                        dynamic tbCompanyMonthsObj = new ExpandoObject();
                        tbCompanyMonthsObj.StartDate = model.startDate;
                        tbCompanyMonthsObj.EndDate = model.endDate;
                        tbCompanyMonthsObj.PeriodMonthYear = monthlyTrialBalnce.FirstOrDefault().EndPeriod.ToString("yyyy-MM");
                        tbCompanyMonthsObj.Entity_Name = companyEntities != null ? string.Join(" ,", companyEntities) : "";

                        //Loop on Company
                        dynamic tbMonthCompany = new List<ExpandoObject>();
                        periodBalanceList = new List<ExpandoObject>();
                        var netDiff = 0.00m;
                        foreach (var companyId in model.companyIds)//coming from refresh_tokens companyIds
                        {

                            dynamic tbMonth = new ExpandoObject();
                            tbMonth.MonthStartDate = monthlyTrialBalnce.FirstOrDefault().StartPeriod.ToString("yyyy-MM-dd");
                            tbMonth.MonthEndDate = monthlyTrialBalnce.FirstOrDefault().EndPeriod.ToString("yyyy-MM-dd");
                            tbMonth.PeriodMonthYear = monthlyTrialBalnce.FirstOrDefault().EndPeriod.ToString("yyyy-MM");
                            tbMonth.CompanyId = companyId;
                            dynamic list = new List<ExpandoObject>();
                            var nwzCompany = getExtCompanies.Where(f => f.ExtCompanyId == companyId).FirstOrDefault();
                            var nwzCompanyId = nwzCompany?.CompanyId;
                            var companyTbList = monthlyTrialBalnce.Where(f => f.CompanyId == companyId);
                            var categoryMappings = categoryCompanyMappings.Where(f => f.Company_ID == nwzCompanyId).ToList();
                            var accountMappings = allAccountMappings.Where(f => f.Company_ID == nwzCompanyId).ToList();

                            var fc = fiscalCalendars.Where(x => x.Entity_UID == nwzCompanyId && x.date_key.Date == monthlyTrialBalnce.FirstOrDefault().StartPeriod.Date).FirstOrDefault();
                            object lockObject = new object();
                            Parallel.ForEach(companyTbList, item =>
                            {
                                var journalDetails = journals.Where(x => x.Account_UID == item.AccountNumber && x.Fiscal_Year == fc?.fy.ToString() && x.Fiscal_Period == fc?.fp.ToString()).ToArray();
                                var journal = journalDetails.FirstOrDefault();
                                var adjustingEntry = journalDetails.Sum(x => x.Debit) - journalDetails.Sum(x => x.Credit);
                                var accountNumber = accountMappings.Where(x => item.AccountNumber == x.Account_UID).FirstOrDefault();
                                staticDynamiclist = new List<StaticColumnNames>();
                                dynamic tb = new ExpandoObject();
                                var isCsutomColmAdded = false;
                                tb.TenantId = item.TenantId;
                                tb.CompanyId = item.CompanyId;
                                tb.StartPeriod = item.StartPeriod.ToString("yyyy-MM-dd");
                                tb.EndPeriod = item.EndPeriod.ToString("yyyy-MM-dd");
                                tb.CompanyName = nwzCompany != null ? !string.IsNullOrEmpty(nwzCompany.Name) ? nwzCompany.Name : item.CompanyName : item.CompanyName; ;
                                tb.Credit = item.Credit;
                                tb.Debit = item.Debit;
                                tb.ReportBasis = item.ReportBasis;
                                tb.Label = item.Label;
                                tb.Currency = item.Currency;
                                tb.AccountNumber = accountNumber?.New_Account_UID ?? string.Empty;
                                tb.AccountDescription = item.AccountDescription;
                                tb.Department = "";
                                tb.Company_Entity = nwzCompanyId;
                                tb.FS_ID = "";
                                tb.FS_Type = "";
                                tb.AdjustingEntry = journalDetails.Count() > 1 ? adjustingEntry : journal?.Debit != null ? Convert.ToDecimal(journal?.Debit) : Convert.ToDecimal(journal?.Credit) * -1;
                                tb.Entity = nwzCompany?.EntityUID;
                                tb.Period = fc?.fp ?? 0;
                                tb.Entity_Group = nwzCompany?.Entity_Group;
                                tb.Quarter = fc?.Quarter ?? string.Empty;
                                tb.Year = fc?.fy.ToString() ?? string.Empty;
                                tb.Date = fc?.date_key.ToString() ?? string.Empty;
                                tb.YearMonth = fc?.fiscal_year_month.ToString() ?? string.Empty;
                                tb.CustomColumnNames = customCoulmns;
                                netDiff = CommonService.CalculateDebitCreditDifferenceInDecimal(item.Debit != null ? (decimal)item.Debit : 0.00m, item.Credit != null ? (decimal)item.Credit * -1 : 0.00m);
                                tb.Net = netDiff;
                                tb.NetValue = netDiff;
                                tb.AdjustingTb = Convert.ToDecimal(tb.Net) + Convert.ToDecimal(tb.AdjustingEntry);
                                if (!string.IsNullOrEmpty(tb.AccountNumber))
                                {
                                    var getAccountMapping = accountMappings.Where(x => item.AccountNumber == x.Account_UID).FirstOrDefault();
                                    if (getAccountMapping != null)
                                    {
                                        isCsutomColmAdded = true;
                                        tb.FS_ID = getAccountMapping.FS_ID;
                                        tb.FS_Type = getAccountMapping.FS;

                                        var getCategoryMapping = categoryMappings.Where(f => f.Category_UID == getAccountMapping.Category_UID && !string.IsNullOrEmpty(f.FS_Type)).GroupBy(f => f.FS_Type).ToList();
                                        foreach (var categoryMap in getCategoryMapping)
                                        {
                                            staticDynamicObj = new StaticColumnNames();
                                            staticDynamicObj.ColumnName = categoryMap.Key;
                                            getAccountMapping.FS_ID = (getAccountMapping.FS_ID == "" || getAccountMapping.FS_ID == null) ? "MISSING" : getAccountMapping.FS_ID;
                                            staticDynamicObj.ColumnValue = getAccountMapping.FS_ID;
                                            staticDynamicObj.ColumnDescription = getAccountMapping.Company_Name;
                                            staticDynamiclist.Add(staticDynamicObj);
                                        }
                                    }
                                }
                                if (isCsutomColmAdded == false)
                                {
                                    foreach (var column in customCoulmns)
                                    {
                                        staticDynamicObj = new StaticColumnNames();
                                        staticDynamicObj.ColumnName = column;
                                        staticDynamicObj.ColumnValue = "MISSING";
                                        staticDynamiclist.Add(staticDynamicObj);
                                    }
                                }

                                tb.CustomColumns = staticDynamiclist;
                                lock (lockObject)
                                {
                                    list.Add(tb);
                                }

                            });
                            tbMonth.Data = list;
                            dynamic periodBalanceObj = new ExpandoObject();
                            periodBalanceObj.Entity = nwzCompanyId;
                            periodBalanceObj.FcPeriod = fc?.fp ?? 0;
                            periodBalanceObj.Balance = list != null ? ((IEnumerable<dynamic>)list).Sum(f => (decimal)f.NetValue) : 0;
                            periodBalanceList.Add(periodBalanceObj);

                            tbMonthCompany.Add(tbMonth);
                        }

                        tbCompanyMonthsObj.PeriodBalanceData = periodBalanceList;
                        tbCompanyMonthsObj.Data = tbMonthCompany;
                        tbCompanyMonthsList.Add(tbCompanyMonthsObj);
                    }
                }

                foreach (var tbList in tbByCompanies)
                {
                    dynamic mappedUnmappedObj = new ExpandoObject();
                    mappedUnmappedObj.Entity = getExtCompanies.Where(f => f.ExtCompanyId == tbList.Key).FirstOrDefault()?.CompanyId;
                    var accountNumbers = tbList.Select(accntNo => accntNo.AccountNumber);
                    int emptyAccountsCount = accountNumbers?.Where(x => string.IsNullOrEmpty(x)).Count() ?? 0;
                    int distinctAccountsCount = accountNumbers?.Where(x => !string.IsNullOrEmpty(x)).Distinct().Count() ?? 0;
                    mappedUnmappedObj.TotalAccounts = emptyAccountsCount + distinctAccountsCount;

                    mappedUnmappedObj.TotalNetBalance = CommonService.ConvertValueToAbs(CommonService.CalculateDebitCreditDifferenceInDecimal(tbList.Where(x => x.Debit != null).Select(x => (decimal)x.Debit).ToArray(),
                        tbList.Where(x => x.Credit != null).Select(x => (decimal)x.Credit).ToArray()));


                    var mappedAccounts = ((IEnumerable<dynamic>)tbCompanyMonthsList).SelectMany(f => (IEnumerable<dynamic>)f.Data).Where(f => f.CompanyId == tbList.Key)
                        .SelectMany(f => (IEnumerable<dynamic>)f.Data)
                        .Where(f => string.IsNullOrEmpty(f.FS_ID) && ((IEnumerable<dynamic>)f.CustomColumns).Where(f => !string.IsNullOrEmpty(f.ColumnValue)).Any()).Select(f => f.AccountDescription).Distinct().ToArray();

                    mappedUnmappedObj.TotalUnMappedAccounts = mappedAccounts.Count();
                    mappedUnmappedObj.TotalMappedAccounts = mappedUnmappedObj.TotalAccounts - mappedUnmappedObj.TotalUnMappedAccounts;

                    tbMappedUnMappedList.Add(mappedUnmappedObj);
                }
                trialBalanceDto.TrialBalanceData = tbCompanyMonthsList;
                trialBalanceDto.MappedUnMappedData = tbMappedUnMappedList;
                return trialBalanceDto;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion
    }
}
