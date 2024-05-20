using DocumentFormat.OpenXml.Wordprocessing;
using Hangfire;
using LazyCache;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Newtonsoft.Json;
using SpreadsheetLight;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.Design;
using System.Data;
using System.Dynamic;
using System.Globalization;
using System.Text.RegularExpressions;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory;

namespace DemoProject.Services.GeneralLedger
{
    public class GeneralLedgerService
    {
        #region fields
        private readonly AppDbContext _context;
        private readonly OAuthService _oAuthService;
        private readonly QuickBookApiService _quickBookApiService;
        private readonly CommonService _commonService;
        private readonly QuickbookCommonService _quickbookCommonService;
        private readonly CompanyService _companyService;
        #endregion

        #region constructor

        public GeneralLedgerService(AppDbContext context, OAuthService oAuthService,
            QuickBookApiService quickBookApiService, CommonService commonService, QuickbookCommonService quickbookCommonService, CompanyService companyService)
        {
            _context = context;
            _quickBookApiService = quickBookApiService;
            _oAuthService = oAuthService;
            _commonService = commonService;
            _quickbookCommonService = quickbookCommonService;
            _companyService = companyService;
        }
        #endregion

        #region methods

        #region Import Functions
        public async Task<List<CompanyMonthsDto>> ImportGeneralLedger(ImportReportDto model)
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
                    BackgroundJob.Schedule(() => FetchGeneralLedger(model, item.CompanyId, item.CompanyName), TimeSpan.FromSeconds(20));
                    // await FetchGeneralLedger(model, item.CompanyId, item.CompanyName);
                }
                if (requestList.Count > 0)
                {
                    uow.QBOImportRequestsRepository.Insert(requestList);
                    await uow.CommitAsync();
                }

                return ifAlreadyImportedForAnyMonth;
            }
        }

        public async Task FetchGeneralLedger(ImportReportDto model, string companyId, string companyName)
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
                            if (await ImportGeneralLedgerAsync(uow, companyId, model.TenantId, model.ReportId, model.UniqueRequestNumber, startDate, endDate))
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
                        var entityName = uow.NwzCompanyRepository.FindByQueryable(x => companyId == x.ExtCompanyId).FirstOrDefault()?.EntityUID;
                        await _quickBookApiService.QBOSendResponseToCallBackURI(model, "Success", "General Ledger", companyName, companyId, isAnySelectedMonthHasData, false, false, false, CommonService.FormatDates(dataNotFoundMonths.ToArray()), isDataImportedForAllRequests, model.CompanyId, model.UniqueRequestNumber, entityName, importReportIds);
                    }
                    else
                    {
                        await _quickBookApiService.QBOSendResponseToCallBackURI(model, "Error", "General Ledger", companyName, companyId, false, false, true);
                    }
                }

            }
            catch (Exception ex)
            {
                await _quickBookApiService.QBOSendResponseToCallBackURI(model, "Error", "General Ledger", companyName, companyId, false);
                throw;
            }
        }

        private async Task<bool> ImportGeneralLedgerAsync(AppUnitOfWork uow, string companyId, int tenantId, int reportId, string uniqueRequestNumber, string startDate, string endDate)
        {
            try
            {
                uow.GeneralLedgerRepository.FindAndSoftDeleteByRawQuery(DateTime.ParseExact(startDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date,
                   tenantId, companyId);


                uow.ImportedReportsInfoRepository.FindAndSoftDelete(x => x.StartPeriod == startDate
                    && x.TenantId == tenantId && x.CompanyId == companyId && x.ReportId == reportId);

                await uow.CommitAsync();

                // Fetch data from QuickBooks API
                var refreshToken = _oAuthService.GetRefreshToken(uow, tenantId, companyId);
                var url = string.Format(ExternalUrl.GeneralLedger, startDate, endDate, companyId, tenantId, refreshToken.Token);
                var data = await _quickBookApiService.CallQuickBookApi<GeneralLedgerResponeReportDto>(url);

                if (data != null && data.Status)
                {
                    // Insert data into ImportedReportsInfoRepository
                    var importDataInfo = new ImportedReportsInfo(startDate, endDate, companyId, tenantId, reportId, uniqueRequestNumber);
                    uow.ImportedReportsInfoRepository.InsertOrUpdate(importDataInfo);

                    if (data.QuickbooksGeneralLedger != null && data.QuickbooksGeneralLedger.Count > 0)
                    {
                        // Update CompanyId, CompanyName, and TenantId for each item in QuickbooksGeneralLedger
                        data.QuickbooksGeneralLedger.ForEach(item =>
                        {
                            item.CompanyId = companyId;
                            item.CompanyPkId = refreshToken.Id;
                            item.CompanyName = refreshToken.CompanyName;
                            item.TenantId = tenantId;
                        });

                        // Insert data into GeneralLedgerRepository
                        uow.GeneralLedgerRepository.Insert(data.QuickbooksGeneralLedger);
                        await uow.CommitAsync();

                        return true;
                    }
                }

                return false;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                uow.Context.ChangeTracker.AutoDetectChangesEnabled = true;
            }
        }


        public async Task<ReportResultDto> GetGeneralLedgerAsync(ReportDataDto model)
        {
            try
            {
                using (AppUnitOfWork uow = new AppUnitOfWork(_context))
                {
                    var viewModel = new ReportResultDto();
                    dynamic list = new List<ExpandoObject>();
                    var staticColumnlist = new List<string>();
                    var staticDynamiclist = new List<StaticColumnNames>();
                    var staticDynamicObj = new StaticColumnNames();

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


                    DataTable dtDates = CommonService.GetMonthsBetweenDates(Convert.ToDateTime(model.startDate), Convert.ToDateTime(model.endDate));
                    var reportImportInfo = uow.ImportedReportsInfoRepository.Find(x => x.TenantId == model.tenantId && x.ReportId == model.reportId && x.CompanyId == model.companyId).ToList();
                    viewModel.Months = dtDates.AsEnumerable()
                        .Where(dateRow => !reportImportInfo.Any(info =>
                            info.StartPeriod == Convert.ToString(dateRow["StartDay"]))).
                            Select(x => new MonthsDto { StartDate = Convert.ToString(x["StartDay"]), EndDate = Convert.ToString(x["EndDay"]) }).ToList();
                    if (viewModel.Months.Any())
                    {
                        viewModel.IsDataImported = false;
                        return viewModel;
                    }
                    viewModel.IsDataImported = true;

                    var extCompanyId = uow.NwzCompanyRepository.Find(f => !string.IsNullOrEmpty(f.ExtCompanyId) && f.ExtCompanyId.Trim() == model.companyId, null, 1, int.MaxValue).Select(f => f.CompanyId).FirstOrDefault();
                    var validFs_Type_List = uow.FSTypeRepository.FindByQueryable(f => f.ParentId != null).Select(f => f.Name).ToArray();

                    var categoryMappings = uow.CategoryMappingRepository.Find(f => validFs_Type_List.Contains(f.FS_Type) && f.Company_ID == extCompanyId, null, 1, int.MaxValue).ToList();
                    var accountMappings = uow.AccountMappingRepository.Find(f => f.Company_ID == extCompanyId, null, 1, int.MaxValue).ToList();
                    var customCoulmns = categoryMappings.GroupBy(f => f.FS_Type).Select(f => f.Key).ToList();
                    var startDate = DateTime.ParseExact(model.startDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date;
                    var endDate = DateTime.ParseExact(model.endDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date;

                    var companyInfo = uow.RefreshTokenRepository.Find(x => x.TenantId == model.tenantId && x.CompanyId == model.companyId).First();
                    var gLlist = uow.GeneralLedgerRepository.FindLargeRecords(x =>
                      (x.StartPeriod.Date >= startDate && x.EndPeriod.Date <= endDate) &&
                       x.CompanyPkId == companyInfo.Id, null, 1, int.MaxValue);

                    foreach (var f in gLlist)
                    {
                        staticDynamiclist = new List<StaticColumnNames>();
                        dynamic gl = new ExpandoObject();
                        gl.TenantId = f.TenantId;
                        gl.CompanyId = f.CompanyId;
                        gl.StartPeriod = f.StartPeriod.ToString("yyyy-MM-dd");
                        gl.EndPeriod = f.EndPeriod.ToString("yyyy-MM-dd");
                        gl.CompanyName = f.CompanyName;
                        gl.Credit = f.Credit;
                        gl.Debit = f.Debit;
                        gl.Amount = f.Amount;
                        gl.Name = f.Name;
                        gl.Account = f.Account;
                        gl.Balance = f.Balance;
                        gl.Split = f.Split;
                        gl.Memo_Description = f.Memo_Description;
                        gl.Doc_Num = f.Doc_Num;
                        gl.Transaction_Type = f.Transaction_Type;
                        gl.Date = f.Date;
                        gl.ReportBasis = f.ReportBasis;
                        gl.Currency = f.Currency;
                        gl.TxnID = f.TxnID;
                        gl.Class = f.Class;
                        gl.AccountNumber = f.AccountNumber;
                        gl.AccountDescription = f.AccountName;
                        gl.Department = "";
                        gl.Description = "";
                        gl.Entity = "";
                        gl.Period = f.StartPeriod.ToString("yyyy-MM");
                        gl.CustomColumnNames = customCoulmns;
                        if (!string.IsNullOrEmpty(gl.AccountNumber))
                        {
                            var getAccountMapping = accountMappings.Where(f => f.Account_UID == gl.AccountNumber).FirstOrDefault();
                            if (getAccountMapping != null)
                            {
                                gl.Entity = getAccountMapping.Entity_ID;
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
                        gl.CustomColumns = staticDynamiclist;
                        list.Add(gl);
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

        #endregion

        public async Task<dynamic> GetGeneralLedgerReportAsync(ReportDataDto model)
        {
            try
            {
                using (AppUnitOfWork uow = new AppUnitOfWork(_context))
                {
                    var viewModel = new ReportResultDto();
                    dynamic resultObj = new ExpandoObject();
                    dynamic companiesByMonthList = new List<ExpandoObject>();
                    dynamic companyByMonthObj = new List<ExpandoObject>();

                    var staticColumnlist = new List<string>();
                    var staticDynamiclist = new List<StaticColumnNames>();
                    var staticDynamicObj = new StaticColumnNames();
                    if (string.IsNullOrEmpty(model.startDate))
                    {
                        var financialStartMonth = await _companyService.GetCompanyStartFinancYRByCompanyId(uow, model.companyId, model.tenantId);
                        if (financialStartMonth > 0)
                        {
                            model.startDate = CommonService.GetStartDateOfFincByDate(financialStartMonth, model.endDate);
                        }
                    }

                    viewModel.StartDate = model.startDate;
                    viewModel.EndDate = model.endDate;

                    DataTable dtDates = CommonService.GetMonthsBetweenDates(Convert.ToDateTime(model.startDate), Convert.ToDateTime(model.endDate));

                    viewModel.IsDataImported = true;

                    var companyInfos = uow.RefreshTokenRepository.Find(x => x.TenantId == model.tenantId && model.companyIds.Contains(x.CompanyId)).ToList();
                    var companyInfoIds = companyInfos.Select(f => f.CompanyId).ToArray();
                    var companyInfoPKIds = companyInfos.Select(f => f.Id).ToArray();

                    var extCompanies = uow.NwzCompanyRepository.Find(f => !string.IsNullOrEmpty(f.ExtCompanyId) && companyInfoIds.Contains(f.ExtCompanyId.Trim()), null, 1, int.MaxValue).ToList();
                    var extCompanyIds = extCompanies.Select(f => f.CompanyId).ToArray();
                    var entities = extCompanies.Select(f => f.EntityUID).Distinct().ToArray();

                    var validFs_Type_List = uow.FSTypeRepository.FindByQueryable(f => f.ParentId != null).Select(f => f.Name).ToArray();

                    var allcategoryMappings = await uow.CategoryMappingRepository.FindLargeRecordsInListAsync(f => validFs_Type_List.Contains(f.FS_Type) && extCompanyIds.Contains(f.Company_ID), null, 1, int.MaxValue);
                    var allaccountMappings = await uow.AccountMappingRepository.FindLargeRecordsInListAsync(f => extCompanyIds.Contains(f.Company_ID), null, 1, int.MaxValue);
                    var customCoulmns = allcategoryMappings.GroupBy(f => f.FS_Type).Select(f => f.Key).ToList();
                    var fiscalCalendars = uow.FiscalCalendarRepository.FindByQueryable(x => extCompanies.Select(e => e.EntityUID).Contains(x.EntityGroup));

                    var datesList = dtDates.AsEnumerable();

                    var selectedStartDate = DateTime.ParseExact(Convert.ToString(datesList.First()["StartDay"]), "yyyy-MM-dd", CultureInfo.InvariantCulture).Date;
                    var selectedEndDate = DateTime.ParseExact(Convert.ToString(datesList.Last()["EndDay"]), "yyyy-MM-dd", CultureInfo.InvariantCulture).Date;

                    var allGeneralLedgerQuery = await uow.GeneralLedgerRepository.FindLargeRecordsInListAsyncByQuery(
                                  selectedStartDate, selectedEndDate,
                                  companyInfoPKIds, companyInfoIds);

                    #region TotalTransactions
                    var beginningMonthStartDate = CommonService.GetStartDateOfPreviousMonth(model.startDate);
                    dynamic totalTransactionObj = new ExpandoObject();
                    dynamic totalTransactionList = new List<ExpandoObject>();
                    var acoountNumbersCalculations = new List<AccountNumbersCalculation>();
                    var acoountNumbersCalculationObj = new AccountNumbersCalculation();

                    var allByTrialBalanceList = await uow.TrialBalanceRepository.FindLargeRecordsInListAsync(x =>
                         (x.StartPeriod.Date == beginningMonthStartDate
                         || x.EndPeriod.Date == DateTime.ParseExact(model.endDate, DataConstants.DATEFORMAT, CultureInfo.InvariantCulture).Date) &&
                         model.companyIds.Contains(x.CompanyId) &&
                         x.TenantId == model.tenantId, null, 1, int.MaxValue);
                    var beginningBalnceMonthTb = allByTrialBalanceList.Where(f => f.StartPeriod.Date == beginningMonthStartDate.Date);
                    var endBalnceMonthTb = allByTrialBalanceList.Where(f => f.EndPeriod.Date == DateTime.ParseExact(model.endDate, DataConstants.DATEFORMAT, CultureInfo.InvariantCulture).Date);

                    object lockObject = new object();

                    foreach (var nwzComp in extCompanies)
                    {
                        var compTransactions = allGeneralLedgerQuery.Where(f => f.CompanyId == nwzComp.ExtCompanyId);
                        acoountNumbersCalculations = new List<AccountNumbersCalculation>();
                        totalTransactionObj = new ExpandoObject();
                        totalTransactionObj.Entity = nwzComp.CompanyId;
                        totalTransactionObj.TotalTransactions = compTransactions.Count();
                        totalTransactionObj.TotalTransactionsImported = totalTransactionObj.TotalTransactions;
                        var groupByAccountNumber = compTransactions.GroupBy(f => f.AccountNumber);
                        var tbBeginningBalanceList = beginningBalnceMonthTb.Where(f => f.CompanyId == nwzComp.ExtCompanyId);
                        var tbEndBalanceList = endBalnceMonthTb.Where(f => f.CompanyId == nwzComp.ExtCompanyId);

                        var companyFiscals = fiscalCalendars.Where(x => x.Entity_UID == nwzComp.CompanyId).ToList();
                        var companyAccountMappings = allaccountMappings.Where(x => x.Company_ID == nwzComp.CompanyId);
                        foreach (var item in groupByAccountNumber)
                        {
                            var accountNumber = allaccountMappings.FirstOrDefault(x => item.First()?.AccountNumber == x.Account_UID);
                            acoountNumbersCalculationObj = new AccountNumbersCalculation();
                            acoountNumbersCalculationObj.AccountNumber = accountNumber?.Account_UID ?? string.Empty;
                            acoountNumbersCalculationObj.BegginingBalance = 0.00m;
                            acoountNumbersCalculationObj.EndBalance = 0.00m;
                            var tbAccntBegginBal = tbBeginningBalanceList.Where(f => f.AccountNumber == acoountNumbersCalculationObj.AccountNumber).FirstOrDefault();
                            if (tbAccntBegginBal != null)
                            {
                                var afc = companyFiscals.FirstOrDefault(x => x.date_key.Date == selectedStartDate.Date);
                                var fs = companyAccountMappings.FirstOrDefault(x => x.Account_UID == acoountNumbersCalculationObj.AccountNumber);
                                if (tbAccntBegginBal != null)
                                {
                                    if (fs?.FS == "PL" && afc?.fp == 1)
                                        acoountNumbersCalculationObj.BegginingBalance = 0;
                                    else
                                        acoountNumbersCalculationObj.BegginingBalance = CommonService.CalculateDebitCreditDifferenceInDecimal(tbAccntBegginBal.Debit != null ? (decimal)tbAccntBegginBal.Debit : 0.00m, tbAccntBegginBal.Credit != null ? (decimal)tbAccntBegginBal.Credit : 0.00m);
                                }
                            }
                            var tbAccntEndBal = tbEndBalanceList.Where(f => f.AccountNumber == acoountNumbersCalculationObj.AccountNumber).LastOrDefault();
                            if (tbAccntEndBal != null)
                            {
                                acoountNumbersCalculationObj.EndBalance = CommonService.CalculateDebitCreditDifferenceInDecimal(tbAccntEndBal.Debit != null ? (decimal)tbAccntEndBal.Debit : 0.00m, tbAccntEndBal.Credit != null ? (decimal)tbAccntEndBal.Credit : 0.00m);
                            }
                            var debits = item.Where(x => !string.IsNullOrEmpty(x.Debit)).Select(x => Convert.ToDecimal(x.Debit)).ToArray();
                            var credits = item.Where(x => !string.IsNullOrEmpty(x.Credit)).Select(x => Convert.ToDecimal(x.Credit)).ToArray();
                            acoountNumbersCalculationObj.NetTransactions = CommonService.CalculateDebitCreditDifferenceInDecimal(debits, credits);
                            if (acoountNumbersCalculationObj.BegginingBalance + acoountNumbersCalculationObj.NetTransactions == acoountNumbersCalculationObj.EndBalance)
                            {
                                acoountNumbersCalculationObj.Check = true;
                            }
                            else
                            {
                                acoountNumbersCalculationObj.Check = false;
                            }
                            acoountNumbersCalculationObj.AccountNumber = accountNumber?.New_Account_UID ?? string.Empty;
                            acoountNumbersCalculations.Add(acoountNumbersCalculationObj);

                        }
                        totalTransactionObj.AccountNumberBalances = acoountNumbersCalculations;
                        totalTransactionList.Add(totalTransactionObj);
                    }
                    #endregion
                    var allLocations = uow.LocationRepository.FindByQueryable(x => extCompanyIds.Contains(x.Company_ID)).ToList();
                    var glList = new List<QuickbooksGeneralLedger>();
                    string? extCompany;
                    var categoryMappings = new List<NwzCategoryMapping>();
                    var accountMappings = new List<NwzAccountMapping>();

                    #region main


                    var fc = new FiscalCalendar();
                    foreach (var companyInfo in extCompanies.OrderBy(f => f.CompanyId))
                    {
                        dynamic tbMonthCompany = new List<ExpandoObject>();

                        var companyGlLists = allGeneralLedgerQuery.Where(x => x.StartPeriod.Date >= selectedStartDate &&
                                            x.EndPeriod.Date <= selectedEndDate &&
                                            x.CompanyId == companyInfo.ExtCompanyId).GroupBy(x => x.AccountNumber);

                        extCompany = companyInfo?.CompanyId;
                        categoryMappings = allcategoryMappings.Where(f => f.Company_ID == extCompany).ToList();
                        accountMappings = allaccountMappings.Where(f => f.Company_ID == extCompany).ToList();
                        var compLocations = allLocations.Where(f => f.Company_ID == extCompany).ToList();
                        var companyFiscals = fiscalCalendars.Where(f => f.Entity_UID == extCompany).ToList();
                        lockObject = new object(); // Create a lock object outside the loop

                        Parallel.ForEach(companyGlLists, glData =>
                        {
                            companyByMonthObj = new ExpandoObject();
                            dynamic list = new List<ExpandoObject>();
                            dynamic tbMonth = new ExpandoObject();
                            var accountNumber = allaccountMappings.FirstOrDefault(x => glData.Key == x.Account_UID);

                            if (accountNumber != null && !string.IsNullOrEmpty(accountNumber.New_Account_UID))
                            {
                                tbMonth.AccountNumber = accountNumber?.New_Account_UID;
                            }
                            else
                            {
                                tbMonth.AccountNumber = null;
                            }
                            foreach (var accountList in glData)
                            {
                                companyByMonthObj.Entity = string.Join(" ,", entities);
                                companyByMonthObj.CustomColumnNames = customCoulmns;


                                if (accountList.Date == "Beginning Balance")
                                {
                                    fc = companyFiscals.FirstOrDefault(x => x.date_key.Date == selectedStartDate.Date);
                                }
                                else
                                {
                                    fc = companyFiscals.FirstOrDefault(x => x.date_key.Date == DateTime.ParseExact(accountList.Date, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date);
                                }

                                tbMonth.CompanyEntity = extCompany;

                                object lockObject = new object(); // Create a lock object for thread-safe access to 'list'
                                var staticDynamiclists = new List<StaticColumnNames>();
                                bool isCustomColumnAdded = false;
                                decimal debit = string.IsNullOrEmpty(accountList.Debit) ? 0.0m : Convert.ToDecimal(accountList.Debit);
                                decimal credit = string.IsNullOrEmpty(accountList.Credit) ? 0.0m : Convert.ToDecimal(accountList.Credit);
                                dynamic gl = new ExpandoObject();

                                // Populate gl object with data
                                gl.TenantId = accountList.TenantId;
                                gl.CompanyId = accountList.CompanyId;
                                gl.StartPeriod = accountList.StartPeriod.ToString("yyyy-MM-dd");
                                gl.EndPeriod = accountList.EndPeriod.ToString("yyyy-MM-dd");
                                gl.CompanyName = companyInfo != null ? (!string.IsNullOrEmpty(companyInfo.Name) ? companyInfo.Name : accountList.CompanyName) : accountList.CompanyName;
                                gl.Credit = accountList.Credit;
                                gl.Debit = accountList.Debit;
                                gl.CompanyEntity = extCompany;
                                gl.Amount = CommonService.CalculateDebitCreditDifferenceInDecimal(debit, credit);
                                gl.Name = accountList.Name;
                                gl.Account = accountList.Account;
                                gl.Balance = accountList.Balance;
                                gl.Split = accountList.Split;
                                gl.Memo_Description = accountList.Memo_Description;
                                gl.Doc_Num = accountList.Doc_Num;
                                gl.Transaction_Type = accountList.Transaction_Type;
                                gl.Date = accountList.Date;
                                gl.ReportBasis = accountList.ReportBasis;
                                gl.Currency = accountList.Currency;
                                gl.TxnID = accountList.TxnID;
                                gl.Class = accountList.Class;
                                gl.AccountNumber = accountNumber?.New_Account_UID ?? null;
                                gl.AccountDescription = accountList.AccountName;
                                gl.Department = "";
                                gl.Description = "";
                                gl.Location = "";
                                gl.Entity = companyInfo?.EntityUID;
                                gl.Period = fc?.fp.ToString() ?? string.Empty;
                                gl.Entity_Group = companyInfo?.Entity_Group;
                                gl.Quarter = fc?.Quarter ?? string.Empty;
                                gl.Year = fc?.fy.ToString() ?? string.Empty;
                                gl.YearMonth = fc?.fiscal_year_month.ToString() ?? string.Empty;
                                gl.CustomColumnNames = customCoulmns;

                                if (!string.IsNullOrEmpty(gl.AccountNumber))
                                {
                                    var getAccountMapping = accountMappings.FirstOrDefault(a => a.Account_UID == gl.AccountNumber);
                                    if (getAccountMapping != null)
                                    {
                                        isCustomColumnAdded = true;

                                        if (!string.IsNullOrEmpty(getAccountMapping.Location_ID))
                                        {
                                            gl.Location = compLocations.FirstOrDefault(cl => cl.Location_ID == getAccountMapping.Location_ID)?.Location_Name;
                                        }

                                        var getCategoryMapping = categoryMappings
                                            .Where(cm => cm.Category_UID == getAccountMapping.Category_UID && !string.IsNullOrEmpty(cm.FS_Type))
                                            .GroupBy(cm => cm.FS_Type)
                                            .ToList();

                                        foreach (var categoryMap in getCategoryMapping)
                                        {
                                            var staticDynamicObjs = new StaticColumnNames();
                                            staticDynamicObjs.ColumnName = categoryMap.Key;
                                            staticDynamicObjs.ColumnValue = string.IsNullOrEmpty(categoryMap.FirstOrDefault()?.FS_ID) ? "MISSING" : categoryMap.FirstOrDefault()?.FS_ID;
                                            staticDynamiclists.Add(staticDynamicObjs);
                                        }
                                    }
                                }
                                if (!isCustomColumnAdded)
                                {
                                    foreach (var column in customCoulmns)
                                    {
                                        var staticDynamicObjss = new StaticColumnNames();
                                        staticDynamicObjss.ColumnName = column;
                                        staticDynamicObjss.ColumnValue = "MISSING";
                                        staticDynamiclists.Add(staticDynamicObjss);
                                    }
                                }
                                gl.CustomColumns = staticDynamiclists;

                                // Add 'gl' to the 'list' in a thread-safe manner using a lock
                                lock (lockObject)
                                {
                                    list.Add(gl);
                                }
                            }
                            tbMonth.Data = list;
                            lock (lockObject)
                            {
                                tbMonthCompany.Add(tbMonth);
                            }
                        });
                        tbMonthCompany = ((IEnumerable<dynamic>)tbMonthCompany).OrderBy(f => f.AccountNumber).ToList();
                        companyByMonthObj.Data = tbMonthCompany;
                        companiesByMonthList.Add(companyByMonthObj);
                    }
                    #endregion

                    resultObj.GlList = companiesByMonthList;
                    resultObj.BalanceTransactions = totalTransactionList;
                    return resultObj;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public async Task<GeneralLedgerReponse> GetGeneralLedgerReportAsync(AppUnitOfWork uow, ReportDataDto model)
        {
            try
            {
                IAppCache cache = new CachingService();
                var viewModel = new ReportResultDto();
                dynamic resultObj = new GeneralLedgerReponse();
                dynamic companiesByMonthList = new List<ExpandoObject>();
                dynamic companyByMonthObj = new ExpandoObject();

                var staticColumnlist = new List<string>();
                var staticDynamiclist = new List<StaticColumnNames>();
                var staticDynamicObj = new StaticColumnNames();
                if (string.IsNullOrEmpty(model.startDate))
                {
                    var financialStartMonth = await _companyService.GetCompanyStartFinancYRByCompanyId(uow, model.companyId, model.tenantId);
                    if (financialStartMonth > 0)
                    {
                        model.startDate = CommonService.GetStartDateOfFincByDate(financialStartMonth, model.endDate);
                    }
                }

                viewModel.StartDate = model.startDate;
                viewModel.EndDate = model.endDate;

                DataTable dtDates = CommonService.GetMonthsBetweenDates(Convert.ToDateTime(model.startDate), Convert.ToDateTime(model.endDate));

                viewModel.IsDataImported = true;

                var companyInfos = uow.RefreshTokenRepository.Find(x => x.TenantId == model.tenantId && model.companyIds.Contains(x.CompanyId)).ToList();
                var companyInfoIds = companyInfos.Select(f => f.CompanyId).ToArray();
                var companyInfoPKIds = companyInfos.Select(f => f.Id).ToArray();

                var extCompanies = uow.NwzCompanyRepository.Find(f => !string.IsNullOrEmpty(f.ExtCompanyId) && companyInfoIds.Contains(f.ExtCompanyId.Trim()), null, 1, int.MaxValue).ToList();
                var extCompanyIds = extCompanies.Select(f => f.CompanyId).ToArray();
                var entities = extCompanies.Select(f => f.EntityUID).Distinct().ToArray();

                var validFs_Type_List = uow.FSTypeRepository.FindByQueryable(f => f.ParentId != null).Select(f => f.Name).ToArray();

                var allcategoryMappings = await uow.CategoryMappingRepository.FindLargeRecordsInListAsync(f => validFs_Type_List.Contains(f.FS_Type) && extCompanyIds.Contains(f.Company_ID), null, 1, int.MaxValue);
                var allaccountMappings = await uow.AccountMappingRepository.FindLargeRecordsInListAsync(f => extCompanyIds.Contains(f.Company_ID), null, 1, int.MaxValue);
                var customCoulmns = allcategoryMappings.GroupBy(f => f.FS_Type).Select(f => f.Key).ToList();
                var fiscalCalendars = uow.FiscalCalendarRepository.FindByQueryable(x => extCompanies.Select(e => e.EntityUID).Contains(x.EntityGroup));

                var datesList = dtDates.AsEnumerable();

                var selectedStartDate = DateTime.ParseExact(Convert.ToString(datesList.First()["StartDay"]), "yyyy-MM-dd", CultureInfo.InvariantCulture).Date;
                var selectedEndDate = DateTime.ParseExact(Convert.ToString(datesList.Last()["EndDay"]), "yyyy-MM-dd", CultureInfo.InvariantCulture).Date;

                var allGeneralLedgerQuery = await uow.GeneralLedgerRepository.FindLargeRecordsInListAsyncByQuery(
                              selectedStartDate, selectedEndDate,
                              companyInfoPKIds, companyInfoIds);

                #region TotalTransactions
                var beginningMonthStartDate = CommonService.GetStartDateOfPreviousMonth(model.startDate);
                dynamic totalTransactionObj = new ExpandoObject();
                dynamic totalTransactionList = new List<ExpandoObject>();
                var acoountNumbersCalculations = new List<AccountNumbersCalculation>();
                var acoountNumbersCalculationObj = new AccountNumbersCalculation();

                var allByTrialBalanceList = await uow.TrialBalanceRepository.FindLargeRecordsInListAsync(x =>
                     (x.StartPeriod.Date == beginningMonthStartDate
                     || x.EndPeriod.Date == DateTime.ParseExact(model.endDate, DataConstants.DATEFORMAT, CultureInfo.InvariantCulture).Date) &&
                     model.companyIds.Contains(x.CompanyId) &&
                     x.TenantId == model.tenantId, null, 1, int.MaxValue);
                var beginningBalnceMonthTb = allByTrialBalanceList.Where(f => f.StartPeriod.Date == beginningMonthStartDate.Date);
                var endBalnceMonthTb = allByTrialBalanceList.Where(f => f.EndPeriod.Date == DateTime.ParseExact(model.endDate, DataConstants.DATEFORMAT, CultureInfo.InvariantCulture).Date);

                object lockObject = new object();

                foreach (var nwzComp in extCompanies)
                {
                    var compTransactions = allGeneralLedgerQuery.Where(f => f.CompanyId == nwzComp.ExtCompanyId);
                    acoountNumbersCalculations = new List<AccountNumbersCalculation>();
                    totalTransactionObj = new ExpandoObject();
                    totalTransactionObj.Entity = nwzComp.CompanyId;
                    totalTransactionObj.TotalTransactions = compTransactions.Count();
                    totalTransactionObj.TotalTransactionsImported = totalTransactionObj.TotalTransactions;
                    var groupByAccountNumber = compTransactions.GroupBy(f => f.AccountNumber);
                    var tbBeginningBalanceList = beginningBalnceMonthTb.Where(f => f.CompanyId == nwzComp.ExtCompanyId);
                    var tbEndBalanceList = endBalnceMonthTb.Where(f => f.CompanyId == nwzComp.ExtCompanyId);

                    var companyFiscals = fiscalCalendars.Where(x => x.Entity_UID == nwzComp.CompanyId).ToList();
                    var companyAccountMappings = allaccountMappings.Where(x => x.Company_ID == nwzComp.CompanyId);
                    foreach (var item in groupByAccountNumber)
                    {
                        var accountNumber = allaccountMappings.FirstOrDefault(x => item.First()?.AccountNumber == x.Account_UID);
                        acoountNumbersCalculationObj = new AccountNumbersCalculation();
                        acoountNumbersCalculationObj.AccountNumber = accountNumber?.Account_UID ?? string.Empty;
                        acoountNumbersCalculationObj.BegginingBalance = 0.00m;
                        acoountNumbersCalculationObj.EndBalance = 0.00m;
                        var tbAccntBegginBal = tbBeginningBalanceList.Where(f => f.AccountNumber == acoountNumbersCalculationObj.AccountNumber).FirstOrDefault();
                        if (tbAccntBegginBal != null)
                        {
                            var afc = companyFiscals.FirstOrDefault(x => x.date_key.Date == selectedStartDate.Date);
                            var fs = companyAccountMappings.FirstOrDefault(x => x.Account_UID == acoountNumbersCalculationObj.AccountNumber);
                            if (tbAccntBegginBal != null)
                            {
                                if (fs?.FS == "PL" && afc?.fp == 1)
                                    acoountNumbersCalculationObj.BegginingBalance = 0;
                                else
                                    acoountNumbersCalculationObj.BegginingBalance = CommonService.CalculateDebitCreditDifferenceInDecimal(tbAccntBegginBal.Debit != null ? (decimal)tbAccntBegginBal.Debit : 0.00m, tbAccntBegginBal.Credit != null ? (decimal)tbAccntBegginBal.Credit : 0.00m);
                            }
                        }
                        var tbAccntEndBal = tbEndBalanceList.Where(f => f.AccountNumber == acoountNumbersCalculationObj.AccountNumber).LastOrDefault();
                        if (tbAccntEndBal != null)
                        {
                            acoountNumbersCalculationObj.EndBalance = CommonService.CalculateDebitCreditDifferenceInDecimal(tbAccntEndBal.Debit != null ? (decimal)tbAccntEndBal.Debit : 0.00m, tbAccntEndBal.Credit != null ? (decimal)tbAccntEndBal.Credit : 0.00m);
                        }
                        var debits = item.Where(x => !string.IsNullOrEmpty(x.Debit)).Select(x => Convert.ToDecimal(x.Debit)).ToArray();
                        var credits = item.Where(x => !string.IsNullOrEmpty(x.Credit)).Select(x => Convert.ToDecimal(x.Credit)).ToArray();
                        acoountNumbersCalculationObj.NetTransactions = CommonService.CalculateDebitCreditDifferenceInDecimal(debits, credits);
                        if (acoountNumbersCalculationObj.BegginingBalance + acoountNumbersCalculationObj.NetTransactions == acoountNumbersCalculationObj.EndBalance)
                        {
                            acoountNumbersCalculationObj.Check = true;
                        }
                        else
                        {
                            acoountNumbersCalculationObj.Check = false;
                        }
                        acoountNumbersCalculationObj.AccountNumber = accountNumber?.New_Account_UID ?? string.Empty;
                        acoountNumbersCalculations.Add(acoountNumbersCalculationObj);

                    }
                    totalTransactionObj.AccountNumberBalances = acoountNumbersCalculations;
                    totalTransactionList.Add(totalTransactionObj);
                }
                #endregion
                var allLocations = uow.LocationRepository.FindByQueryable(x => extCompanyIds.Contains(x.Company_ID)).ToList();
                var glList = new List<QuickbooksGeneralLedger>();
                string? extCompany;
                var categoryMappings = new List<NwzCategoryMapping>();
                var accountMappings = new List<NwzAccountMapping>();

                #region main


                var fc = new FiscalCalendar();
                foreach (var companyInfo in extCompanies.OrderBy(f => f.CompanyId))
                {
                    dynamic tbMonthCompany = new List<ExpandoObject>();

                    var companyGlLists = allGeneralLedgerQuery.Where(x => x.StartPeriod.Date >= selectedStartDate &&
                                        x.EndPeriod.Date <= selectedEndDate &&
                                        x.CompanyId == companyInfo.ExtCompanyId).GroupBy(x => x.AccountNumber);

                    extCompany = companyInfo?.CompanyId;
                    categoryMappings = allcategoryMappings.Where(f => f.Company_ID == extCompany).ToList();
                    accountMappings = allaccountMappings.Where(f => f.Company_ID == extCompany).ToList();
                    var compLocations = allLocations.Where(f => f.Company_ID == extCompany).ToList();
                    var companyFiscals = fiscalCalendars.Where(f => f.Entity_UID == extCompany).ToList();
                    lockObject = new object(); // Create a lock object outside the loop

                    Parallel.ForEach(companyGlLists, glData =>
                    {
                        companyByMonthObj = new ExpandoObject();
                        dynamic list = new List<ExpandoObject>();
                        dynamic tbMonth = new ExpandoObject();
                        var accountNumber = allaccountMappings.FirstOrDefault(x => glData.Key == x.Account_UID);

                        if (accountNumber != null && !string.IsNullOrEmpty(accountNumber.New_Account_UID))
                        {
                            tbMonth.AccountNumber = accountNumber?.New_Account_UID;
                        }
                        else
                        {
                            tbMonth.AccountNumber = null;
                        }
                        foreach (var accountList in glData)
                        {
                            companyByMonthObj.Entity = string.Join(" ,", entities);
                            companyByMonthObj.CustomColumnNames = customCoulmns;


                            if (accountList.Date == "Beginning Balance")
                            {
                                fc = companyFiscals.FirstOrDefault(x => x.date_key.Date == selectedStartDate.Date);
                            }
                            else
                            {
                                fc = companyFiscals.FirstOrDefault(x => x.date_key.Date == DateTime.ParseExact(accountList.Date, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date);
                            }

                            tbMonth.CompanyEntity = extCompany;

                            object lockObject = new object(); // Create a lock object for thread-safe access to 'list'
                            var staticDynamiclists = new List<StaticColumnNames>();
                            bool isCustomColumnAdded = false;
                            decimal debit = string.IsNullOrEmpty(accountList.Debit) ? 0.0m : Convert.ToDecimal(accountList.Debit);
                            decimal credit = string.IsNullOrEmpty(accountList.Credit) ? 0.0m : Convert.ToDecimal(accountList.Credit);
                            dynamic gl = new ExpandoObject();

                            // Populate gl object with data
                            gl.TenantId = accountList.TenantId;
                            gl.CompanyId = accountList.CompanyId;
                            gl.StartPeriod = accountList.StartPeriod.ToString("yyyy-MM-dd");
                            gl.EndPeriod = accountList.EndPeriod.ToString("yyyy-MM-dd");
                            gl.CompanyName = companyInfo != null ? (!string.IsNullOrEmpty(companyInfo.Name) ? companyInfo.Name : accountList.CompanyName) : accountList.CompanyName;
                            gl.Credit = accountList.Credit;
                            gl.Debit = accountList.Debit;
                            gl.CompanyEntity = extCompany;
                            gl.Amount = CommonService.CalculateDebitCreditDifferenceInDecimal(debit, credit);
                            gl.Name = accountList.Name;
                            gl.Account = accountList.Account;
                            gl.Balance = accountList.Balance;
                            gl.Split = accountList.Split;
                            gl.Memo_Description = accountList.Memo_Description;
                            gl.Doc_Num = accountList.Doc_Num;
                            gl.Transaction_Type = accountList.Transaction_Type;
                            gl.Date = accountList.Date;
                            gl.ReportBasis = accountList.ReportBasis;
                            gl.Currency = accountList.Currency;
                            gl.TxnID = accountList.TxnID;
                            gl.Class = accountList.Class;
                            gl.AccountNumber = accountNumber?.New_Account_UID ?? null;
                            gl.AccountDescription = accountList.AccountName;
                            gl.Department = "";
                            gl.Description = "";
                            gl.Location = "";
                            gl.Entity = companyInfo?.EntityUID;
                            gl.Period = fc?.fp.ToString() ?? string.Empty;
                            gl.Entity_Group = companyInfo?.Entity_Group;
                            gl.Quarter = fc?.Quarter ?? string.Empty;
                            gl.Year = fc?.fy.ToString() ?? string.Empty;
                            gl.YearMonth = fc?.fiscal_year_month.ToString() ?? string.Empty;
                            gl.CustomColumnNames = customCoulmns;

                            if (!string.IsNullOrEmpty(gl.AccountNumber))
                            {
                                var getAccountMapping = accountMappings.FirstOrDefault(a => a.Account_UID == gl.AccountNumber);
                                if (getAccountMapping != null)
                                {
                                    isCustomColumnAdded = true;

                                    if (!string.IsNullOrEmpty(getAccountMapping.Location_ID))
                                    {
                                        gl.Location = compLocations.FirstOrDefault(cl => cl.Location_ID == getAccountMapping.Location_ID)?.Location_Name;
                                    }

                                    var getCategoryMapping = categoryMappings
                                        .Where(cm => cm.Category_UID == getAccountMapping.Category_UID && !string.IsNullOrEmpty(cm.FS_Type))
                                        .GroupBy(cm => cm.FS_Type)
                                        .ToList();

                                    foreach (var categoryMap in getCategoryMapping)
                                    {
                                        var staticDynamicObjs = new StaticColumnNames();
                                        staticDynamicObjs.ColumnName = categoryMap.Key;
                                        staticDynamicObjs.ColumnValue = string.IsNullOrEmpty(categoryMap.FirstOrDefault()?.FS_ID) ? "MISSING" : categoryMap.FirstOrDefault()?.FS_ID;
                                        staticDynamiclists.Add(staticDynamicObjs);
                                    }
                                }
                            }
                            if (!isCustomColumnAdded)
                            {
                                foreach (var column in customCoulmns)
                                {
                                    var staticDynamicObjss = new StaticColumnNames();
                                    staticDynamicObjss.ColumnName = column;
                                    staticDynamicObjss.ColumnValue = "MISSING";
                                    staticDynamiclists.Add(staticDynamicObjss);
                                }
                            }
                            gl.CustomColumns = staticDynamiclists;

                            // Add 'gl' to the 'list' in a thread-safe manner using a lock
                            lock (lockObject)
                            {
                                list.Add(gl);
                            }
                        }
                        tbMonth.Data = list;
                        lock (lockObject)
                        {
                            tbMonthCompany.Add(tbMonth);
                        }
                    });
                    tbMonthCompany = ((IEnumerable<dynamic>)tbMonthCompany).OrderBy(f => f.AccountNumber).ToList();
                    companyByMonthObj.Data = tbMonthCompany;
                    companiesByMonthList.Add(companyByMonthObj);
                }
                #endregion

                resultObj.GlList = companiesByMonthList;
                resultObj.BalanceTransactions = totalTransactionList;
                return resultObj;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public async Task<string[]?> GetGLReportPaginationColumnsAsync(ReportDataDto model)
        {
            try
            {
                using (AppUnitOfWork uow = new AppUnitOfWork(_context))
                {

                    var companyInfos = uow.RefreshTokenRepository.Find(x => x.TenantId == model.tenantId && model.companyIds.Contains(x.CompanyId)).ToList();
                    var companyInfoIds = companyInfos.Select(f => f.CompanyId).ToArray();

                    var extCompanies = uow.NwzCompanyRepository.Find(f => !string.IsNullOrEmpty(f.ExtCompanyId) && companyInfoIds.Contains(f.ExtCompanyId.Trim()), null, 1, int.MaxValue).ToList();
                    var extCompanyIds = extCompanies.Select(f => f.CompanyId).ToArray();
                    var validFs_Type_List = uow.FSTypeRepository.FindByQueryable(f => f.ParentId != null).Select(f => f.Name).ToArray();

                    var allcategoryMappings = uow.CategoryMappingRepository.Find(f => validFs_Type_List.Contains(f.FS_Type) && extCompanyIds.Contains(f.Company_ID), null, 1, int.MaxValue);
                    var customCoulmns = allcategoryMappings.GroupBy(f => f.FS_Type).Select(f => f.Key).ToList();
                    return customCoulmns.ToArray();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private long ParseAccountNumber(string accountNumber)
        {
            if (string.IsNullOrEmpty(accountNumber))
            {
                return long.MaxValue;
            }

            if (long.TryParse(accountNumber, out long longValue))
            {
                return longValue;
            }

            return long.MaxValue;
        }

        public async Task<dynamic> GetGeneralLedgerReportByPagingAsync(ReportDataDto model)
        {
            try
            {
                using (AppUnitOfWork uow = new AppUnitOfWork(_context))
                {

                    var viewModel = new ReportResultDto();
                    dynamic companiesByMonthList = new List<ExpandoObject>();
                    dynamic companyByMonthObj = new List<ExpandoObject>();

                    var staticColumnlist = new List<string>();
                    var staticDynamiclist = new List<StaticColumnNames>();
                    var staticDynamicObj = new StaticColumnNames();

                    if (string.IsNullOrEmpty(model.startDate))
                    {
                        var financialStartMonth = await _companyService.GetCompanyStartFinancYRByCompanyId(uow, model.companyId, model.tenantId);
                        if (financialStartMonth > 0)
                        {
                            model.startDate = CommonService.GetStartDateOfFincByDate(financialStartMonth, model.endDate);
                        }
                    }

                    viewModel.StartDate = model.startDate;
                    viewModel.EndDate = model.endDate;

                    if (model.companyIds == null || model.companyIds.Count() == 0)
                    {
                        model.companyIds = uow.QBOImportRequestsRepository
                                .Find(request => request.UniqueRequestNumber == model.UniqueRequestNumber && request.ReportId == model.reportId, null, 1, int.MaxValue)
                                .Select(request => request.CompanyId).Distinct()
                                .ToArray();
                    }

                    DataTable dtDates = CommonService.GetMonthsBetweenDates(Convert.ToDateTime(model.startDate), Convert.ToDateTime(model.endDate));

                    viewModel.IsDataImported = true;

                    var companyInfos = uow.RefreshTokenRepository.Find(x => x.TenantId == model.tenantId && model.companyIds.Contains(x.CompanyId)).ToList();
                    var companyInfoIds = companyInfos.Select(f => f.CompanyId).ToArray();

                    var extCompanies = uow.NwzCompanyRepository.Find(f => !string.IsNullOrEmpty(f.ExtCompanyId) && companyInfoIds.Contains(f.ExtCompanyId.Trim()), null, 1, int.MaxValue).ToList();
                    var extCompanyIds = extCompanies.Select(f => f.CompanyId).ToArray();
                    var entities = extCompanies.Select(f => f.EntityUID).Distinct().ToArray();

                    var validFs_Type_List = uow.FSTypeRepository.FindByQueryable(f => f.ParentId != null).Select(f => f.Name).ToArray();

                    var allcategoryMappings = uow.CategoryMappingRepository.Find(f => validFs_Type_List.Contains(f.FS_Type) && extCompanyIds.Contains(f.Company_ID), null, 1, int.MaxValue);
                    var allaccountMappings = uow.AccountMappingRepository.Find(f => extCompanyIds.Contains(f.Company_ID), null, 1, int.MaxValue);
                    var customCoulmns = allcategoryMappings.GroupBy(f => f.FS_Type).Select(f => f.Key).ToList();
                    var OrderColumn = model.sorting.Split(" ")[0];
                    BaseRepository<QuickbooksGeneralLedger, AppDbContext>.LargeRecordsResult<QuickbooksGeneralLedger> gLlist;
                    bool isAscending = model.sorting.Contains("asc", StringComparison.InvariantCultureIgnoreCase);
                    Func<IQueryable<QuickbooksGeneralLedger>, IOrderedQueryable<QuickbooksGeneralLedger>> orderByExpression;

                    switch (OrderColumn)
                    {
                        case "Entity":
                        case "Company_Entity":
                        case "CompanyEntity":
                            string[] qbCompanyIds = null;
                            if (isAscending)
                            {
                                qbCompanyIds = extCompanies.OrderBy(f => f.CompanyId).Select(f => f.ExtCompanyId).ToArray();
                            }
                            else
                            {
                                qbCompanyIds = extCompanies.OrderByDescending(f => f.CompanyId).Select(f => f.ExtCompanyId).ToArray();
                            }
                            var qbCompanyIdsCsv = string.Join(",", qbCompanyIds);
                            orderByExpression = x => x.OrderBy(m => qbCompanyIds == null ? 0 : AppDbContext.ArrayIndexOf(qbCompanyIdsCsv, m.CompanyId));
                            break;
                        case "AccountNumber":
                            orderByExpression = x => isAscending ?
                                x.OrderBy(m => AppDbContext.ParseAccountNumber(m.AccountNumber))
                             : x.OrderByDescending(m => AppDbContext.ParseAccountNumber(m.AccountNumber));
                            break;
                        case "Doc_Num":
                            orderByExpression = x => isAscending ?
                                x.OrderBy(m => m.Doc_Num)
                             : x.OrderByDescending(m => m.Doc_Num);
                            break;
                        case "Date":
                            orderByExpression = x => isAscending ?
                                x.OrderBy(m => m.Date.Trim()) :
                                x.OrderByDescending(m => m.Date.Trim());
                            break;
                        case "Period":
                            orderByExpression = x => isAscending ?
                                x.OrderBy(m => m.StartPeriod) :
                                x.OrderByDescending(m => m.StartPeriod);
                            break;
                        default:
                            orderByExpression = null; // No specific order
                            break;
                    }

                    gLlist = await uow.GeneralLedgerRepository.FindLargeRecordsByPaging(x =>
                        (x.StartPeriod.Date >= DateTime.ParseExact(model.startDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date &&
                        x.EndPeriod.Date <= DateTime.ParseExact(model.endDate, "yyyy-MM-dd", CultureInfo.InvariantCulture).Date) &&
                        companyInfos.Select(x => x.Id).Contains(x.CompanyPkId),
                        orderByExpression,
                        model.skipSize, model.pageSize, model.search);

                    var fiscalCalendars = uow.FiscalCalendarRepository.FindByQueryable(x => extCompanies.Select(e => e.EntityUID).Contains(x.EntityGroup)).ToList();
                    var allLocations = uow.LocationRepository.FindByQueryable(x => extCompanyIds.Contains(x.Company_ID)).ToList();
                    int total = gLlist.TotalRecords;
                    dynamic data = new ExpandoObject();

                    List<ExpandoObject> list = new List<ExpandoObject>();
                    foreach (var f in gLlist.Data)
                    {
                        var getextCompany = extCompanies.Where(m => m.ExtCompanyId == f.CompanyId).FirstOrDefault();
                        var extCompany = getextCompany?.CompanyId;
                        var fc = fiscalCalendars.Where(x => x.Entity_UID == extCompany && x.date_key.Date == f.StartPeriod.Date).FirstOrDefault();
                        var accountNumber = allaccountMappings.Where(x => f.AccountNumber == x.Account_UID).FirstOrDefault();
                        var isCsutomColmAdded = false;
                        staticDynamiclist = new List<StaticColumnNames>();
                        dynamic gl = new ExpandoObject();
                        gl.TenantId = f.TenantId;
                        gl.CompanyId = f.CompanyId;
                        gl.StartPeriod = f.StartPeriod.ToString("yyyy-MM-dd");
                        gl.EndPeriod = f.EndPeriod.ToString("yyyy-MM-dd");
                        gl.CompanyName = getextCompany != null ? !string.IsNullOrEmpty(getextCompany.Name) ? getextCompany.Name : f.CompanyName : f.CompanyName;
                        gl.Entity_Group = getextCompany?.Entity_Group;
                        gl.Credit = f.Credit;
                        gl.Debit = f.Debit;
                        gl.CompanyEntity = extCompany;
                        gl.Amount = f.Amount;
                        gl.Name = f.Name;
                        gl.Account = f.Account;
                        gl.Balance = f.Balance;
                        gl.Split = f.Split;
                        gl.Memo_Description = f.Memo_Description;
                        gl.Doc_Num = f.Doc_Num;
                        gl.Transaction_Type = f.Transaction_Type;
                        gl.Date = f.Date;
                        gl.ReportBasis = f.ReportBasis;
                        gl.Currency = f.Currency;
                        gl.TxnID = f.TxnID;
                        gl.Class = f.Class;
                        gl.AccountNumber = accountNumber?.New_Account_UID ?? "MISSING";
                        gl.AccountDescription = f.AccountName;
                        gl.Department = "";
                        gl.Description = "";
                        gl.Entity = extCompany;
                        gl.Period = f.StartPeriod.ToString("yyyy-MM");
                        gl.CustomColumnNames = customCoulmns;

                        gl.Location = "";
                        gl.Period = fc?.fp.ToString() ?? string.Empty;
                        gl.Quarter = fc?.Quarter ?? string.Empty;
                        gl.Year = fc?.fy.ToString() ?? string.Empty;
                        gl.YearMonth = fc?.fiscal_year_month.ToString() ?? string.Empty;
                        gl.CustomColumnNames = customCoulmns;
                        var newDate = string.Empty;
                        if (!string.IsNullOrEmpty(f.Amount) && !string.IsNullOrEmpty(f.Date))
                        {
                            int year = int.Parse(f.Date.Substring(0, 4));
                            int day = int.Parse(f.Date.Substring(0, 2));
                            int month = int.Parse(f.Date.Substring(5, 2));

                            DateTime date = new DateTime(year, month, day);
                            newDate = date.ToString("yyyy-MM-dd");
                        }
                        else if (!string.IsNullOrEmpty(f.Date))
                            newDate = f.Date;
                        else
                            newDate = string.Empty;
                        gl.CalculateDate = newDate;
                        if (!string.IsNullOrEmpty(gl.AccountNumber))
                        {
                            var getAccountMapping = allaccountMappings.Where(m => m.Account_UID == gl.AccountNumber && m.Company_ID == extCompany).FirstOrDefault();
                            if (getAccountMapping != null)
                            {
                                isCsutomColmAdded = true;
                                if (!string.IsNullOrEmpty(getAccountMapping.Location_ID))
                                {
                                    gl.Location = allLocations.Where(m => m.Company_ID == extCompany && m.Location_ID == getAccountMapping.Location_ID).FirstOrDefault()?.Location_Name;
                                }
                                var getCategoryMapping = allcategoryMappings.Where(m => m.Company_ID == extCompany && m.Category_UID == getAccountMapping.Category_UID && !string.IsNullOrEmpty(m.FS_Type)).GroupBy(m => m.FS_Type).ToList();
                                foreach (var column in customCoulmns)
                                {
                                    var categoryMap = getCategoryMapping.Where(m => m.Key == column).FirstOrDefault();
                                    if (categoryMap != null)
                                    {
                                        staticDynamicObj = new StaticColumnNames();
                                        staticDynamicObj.ColumnName = categoryMap.Key;
                                        staticDynamicObj.ColumnValue = (categoryMap.FirstOrDefault()?.FS_ID == "" || categoryMap.FirstOrDefault()?.FS_ID == null) ? "MISSING" : categoryMap.FirstOrDefault()?.FS_ID;
                                        staticDynamiclist.Add(staticDynamicObj);
                                        ((IDictionary<string, object>)gl)[categoryMap.Key] = staticDynamicObj.ColumnValue;
                                    }
                                    else
                                    {
                                        ((IDictionary<string, object>)gl)[column] = "MISSING";
                                    }
                                }
                            }
                        }

                        if (isCsutomColmAdded == false)
                        {
                            foreach (var column in customCoulmns)
                            {
                                ((IDictionary<string, object>)gl)[column] = "MISSING";
                            }
                        }
                        gl.CustomColumns = staticDynamiclist;
                        list.Add(gl);
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

        #endregion
    }
}
