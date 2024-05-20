using Hangfire;
using LazyCache;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json;
using SpreadsheetLight;
using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using System.Security.Principal;
using System.Text;
using System.Threading.Tasks;

namespace DemoProject.Services.ChartsOfAccounts
{
    public class ChartsOfAccountsService : IAppService
    {
        #region fields
        private readonly AppDbContext _context;
        private readonly OAuthService _oAuthService;
        private readonly QuickBookApiService _quickBookApiService;
        private readonly QuickbookCommonService _quickbookCommonService;
        #endregion

        #region constructor

        public ChartsOfAccountsService(AppDbContext context, OAuthService oAuthService, QuickBookApiService quickBookApiService, QuickbookCommonService quickbookCommonService)
        {
            _context = context;
            _quickBookApiService = quickBookApiService;
            _oAuthService = oAuthService;
            _quickbookCommonService = quickbookCommonService;

        }
        #endregion

        #region methods

        public async Task ImportChartOfAccounts(ImportReportDto model)
        {
            if (model.CompanyId != null && model.CompanyId.Length > 0)
            {
                using (AppUnitOfWork uow = new AppUnitOfWork(_context))
                {
                    var companies = await uow.RefreshTokenRepository.FindByQueryable(r => r.TenantId == model.TenantId && model.CompanyId.Contains(r.CompanyId)).ToListAsync();
                    var requestList = new List<QBOImportRequest>();
                    foreach (var item in companies)
                    {
                        var importRequest = new QBOImportRequest() { CompanyId = item.CompanyId, CompanyName = item.CompanyName, TenantId = model.TenantId, ReportId = model.ReportId, UniqueRequestNumber = model.UniqueRequestNumber };
                        requestList.Add(importRequest);
                        BackgroundJob.Schedule(() => FetchChartOfAccounts(model, item.CompanyId, item.CompanyName), TimeSpan.FromSeconds(20));
                    }
                    if (requestList.Count > 0)
                    {
                        uow.QBOImportRequestsRepository.Insert(requestList);
                        await uow.CommitAsync();
                    }
                }
            }
        }

        public async Task FetchChartOfAccounts(ImportReportDto model, string companyId, string companyName)
        {
            try
            {
                DataTable dtDates = CommonService.GetMonthsBetweenDates(Convert.ToDateTime(model.StartDate), Convert.ToDateTime(model.EndDate));
                using (AppUnitOfWork uow = new AppUnitOfWork(_context))
                {
                    if (await _quickbookCommonService.IsCompanyTokenValid(uow, companyId, model.TenantId))
                    {
                        var isAnySelectedMonthHasData = false;
                        var startDate = ""; var endDate = "";
                        foreach (var monthRow in dtDates.AsEnumerable())
                        {
                            startDate = Convert.ToString(monthRow["StartDay"]);
                            endDate = Convert.ToString(monthRow["EndDay"]);
                            if (await FetchChartOfAccountsData(uow, companyId, model.TenantId, model.ReportId, model.UniqueRequestNumber, startDate, endDate))
                            {
                                isAnySelectedMonthHasData = true;
                            }
                            break;
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
                        await _quickBookApiService.QBOSendResponseToCallBackURI(model, "Success", "Chart Of Accounts", companyName, companyId, isAnySelectedMonthHasData, false, false, true, null, isDataImportedForAllRequests, model.CompanyId, null, entityName, importReportIds);
                    }
                    else
                    {
                        await _quickBookApiService.QBOSendResponseToCallBackURI(model, "Error", "Chart Of Accounts", companyName, companyId, false, false, true, true);
                    }
                }
            }
            catch (Exception ex)
            {
                await _quickBookApiService.QBOSendResponseToCallBackURI(model, "Error", "Chart Of Accounts", companyName, companyId, false);
                throw;
            }
        }

        public async Task<bool> FetchChartOfAccountsData(AppUnitOfWork uow, string companyId, int tenantId, int reportId, string uniqueRequestNumber, string startDate, string endDate)
        {

            //check if any data exist against cselected company and delete it
            var isAlreadyDataExist = uow.ChartOfAccountsRepository.Find(x => x.TenantId == tenantId && x.CompanyId == companyId, null, 1, int.MaxValue).ToList(); ;
            var alreadyExistedEntries = uow.ImportedReportsInfoRepository.Find(x => x.TenantId == tenantId && x.CompanyId == companyId && x.ReportId == reportId, null, 1, int.MaxValue).ToList();

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
                startDate = new DateTime(currentDate.Year, currentDate.Month, 1).ToString("yyyy-MM-dd");
                endDate = new DateTime(currentDate.Year, currentDate.Month, 1).ToString("yyyy-MM-dd");

                var importDataInfo = new ImportedReportsInfo(startDate, endDate, companyId, tenantId, reportId, uniqueRequestNumber);
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

        public async Task<ReportResultDto> GetChartOfAccountsAsync(ReportDataDto model)
        {
            try
            {
                using (AppUnitOfWork uow = new AppUnitOfWork(_context))
                {
                    var reportImportedDataInfo = uow.ImportedReportsInfoRepository.Find(x =>
                    x.TenantId == model.tenantId && x.ReportId == model.reportId && model.companyIds.Contains(x.CompanyId));

                    var viewModel = new ReportResultDto();
                    viewModel.IsDataImported = reportImportedDataInfo.Any();

                    var allChartOfAccnts = await uow.ChartOfAccountsRepository.FindLargeRecordsInListAsync(x =>
                        model.companyIds.Contains(x.CompanyId) &&
                        x.TenantId == model.tenantId, null, 1, int.MaxValue, model.search);

                    var extCompanies = uow.NwzCompanyRepository.Find(f => !string.IsNullOrEmpty(f.ExtCompanyId) && model.companyIds.Contains(f.ExtCompanyId.Trim()), null, 1, int.MaxValue).ToList();
                    var extCompanyIds = extCompanies.Select(f => f.CompanyId).ToArray();
                    var companyEntities = extCompanies.Select(f => f.EntityUID).Distinct().ToArray();
                    dynamic coaCompList = new List<ExpandoObject>();
                    viewModel.CompanyEntityName = string.Join(" ,", companyEntities);
                    var fiscalCalendars = uow.FiscalCalendarRepository.FindByQueryable(x => extCompanies.Select(e => e.EntityUID).Contains(x.EntityGroup)).ToList();
                    var allAccountMappings = await uow.AccountMappingRepository.FindLargeRecordsInListAsync(f => extCompanyIds.Contains(f.Company_ID), null, 1, int.MaxValue);
                    var validFs_Type_List = uow.FSTypeRepository.FindByQueryable(f => f.ParentId != null).Select(f => f.Name).ToArray();

                    var allCategoryMappings = await uow.CategoryMappingRepository.FindLargeRecordsInListAsync(f => validFs_Type_List.Contains(f.FS_Type) && extCompanyIds.Contains(f.Company_ID), null, 1, int.MaxValue);
                    var allLocations = uow.LocationRepository.FindByQueryable(x => extCompanyIds.Contains(x.Company_ID)).ToList();

                    foreach (var compId in model.companyIds)
                    {
                        var nwzXomp = extCompanies.Where(f => f.ExtCompanyId == compId).FirstOrDefault();
                        var nwzCompId = nwzXomp?.CompanyId;
                        dynamic coaCompObj = new ExpandoObject();
                        dynamic coaData = new List<ExpandoObject>();
                        var companyCoaList = allChartOfAccnts.Where(f => f.CompanyId == compId).ToList();
                        coaCompObj.Entity = nwzCompId;
                        var accountMappings = allAccountMappings.Where(f => f.Company_ID == nwzCompId).ToList();
                        var categoryMappings = allCategoryMappings.Where(f => f.Company_ID == nwzCompId).ToList();
                        var compLocations = allLocations.Where(f => f.Company_ID == nwzCompId).ToList();
                        Parallel.ForEach(companyCoaList, item =>
                        {
                            var accountNumber = accountMappings.Where(x => item.AccountNumber == x.Account_UID).FirstOrDefault();
                            var fc = fiscalCalendars.Where(x => x.date_key.Date == item.StartPeriod.Date).FirstOrDefault();
                            dynamic coaObj = new ExpandoObject();
                            coaObj.TenantId = item.TenantId;
                            coaObj.CompanyId = item.CompanyId;
                            coaObj.CompanyEntity = nwzCompId;
                            coaObj.StartPeriod = item.StartPeriod.ToString("yyyy-MM-dd");
                            coaObj.EndPeriod = item.EndPeriod.ToString("yyyy-MM-dd");
                            coaObj.CompanyName = item.CompanyName;
                            coaObj.AccountNumber = item.AccountNumber;
                            coaObj.NewAccountNumber = accountNumber?.New_Account_UID ?? string.Empty;
                            coaObj.FullName = item.FullName;
                            coaObj.CashFlowClassification = item.CashFlowClassification;
                            coaObj.AccountType = item.AccountType;
                            coaObj.SpecialAccountType = item.SpecialAccountType;
                            coaObj.Description = item.Description;
                            coaObj.Balance = item.Balance;
                            coaObj.CurrencyRefFullName = item.CurrencyRefFullName;
                            coaObj.CreatedOn = item.CreatedOn;
                            coaObj.Entity = nwzCompId;
                            coaObj.Period = fc?.fp.ToString() ?? string.Empty;
                            coaObj.Entity_Group = nwzXomp?.Entity_Group;
                            coaObj.Quarter = fc?.Quarter ?? string.Empty;
                            coaObj.Year = fc?.fy.ToString() ?? string.Empty;
                            coaObj.Date = fc?.date_key.ToString() ?? string.Empty;

                            coaObj.Entity = "";
                            coaObj.FS_ID = "";
                            coaObj.FS_ID_Description = "";
                            coaObj.FS_Type = "";
                            coaObj.Subcategory1_UID = "";
                            coaObj.Subcategory2_UID = "";
                            coaObj.Subcategory3_UID = "";
                            coaObj.Subcategory4_UID = "";
                            coaObj.Subcategory5_UID = "";
                            coaObj.Location = "";
                            coaObj.Currency = "";
                            coaObj.NB_C_Sort_Code = "";
                            coaObj.Error_Check = "";
                            coaObj.FinancialReport = "";

                            if (!string.IsNullOrEmpty(coaObj.AccountNumber))
                            {
                                var getAccountMapping = accountMappings.Where(x => item.AccountNumber == x.Account_UID).FirstOrDefault();
                                if (getAccountMapping != null)
                                {
                                    if (!string.IsNullOrEmpty(getAccountMapping.Location_ID))
                                    {
                                        coaObj.Location = compLocations.Where(f => f.Location_ID == getAccountMapping.Location_ID).FirstOrDefault()?.Location_Name;
                                    }
                                    coaObj.Entity = getAccountMapping.Entity_ID;
                                    coaObj.FS_ID = getAccountMapping.FS_ID;
                                    coaObj.FS_Type = getAccountMapping.FS;
                                    coaObj.Subcategory1_UID = getAccountMapping.Subcategory1_UID;
                                    coaObj.Subcategory2_UID = getAccountMapping.Subcategory2_UID;
                                    coaObj.Subcategory3_UID = getAccountMapping.Subcategory3_UID;
                                    coaObj.Subcategory4_UID = getAccountMapping.Subcategory4_UID;
                                    coaObj.Subcategory5_UID = getAccountMapping.Subcategory5_UID;
                                    coaObj.Currency = getAccountMapping.Currency;
                                    coaObj.NB_C_Sort_Code = getAccountMapping.NB_C_Sort_Code;
                                    coaObj.Error_Check = getAccountMapping.Error_Check;
                                    var getCategoryMapping = categoryMappings.Where(f => f.Category_UID == getAccountMapping.Category_UID && !string.IsNullOrEmpty(f.FS_Type)).GroupBy(f => f.FS_Type).FirstOrDefault();
                                    if (getCategoryMapping != null)
                                    {

                                        coaObj.FinancialReport = getCategoryMapping.Key;
                                        coaObj.FS_ID_Description = getCategoryMapping.FirstOrDefault().Name;
                                    }
                                }
                            }

                            coaData.Add(coaObj);
                        });
                        coaCompObj.Data = coaData;
                        dynamic mappedUnmappedObj = new ExpandoObject();
                        mappedUnmappedObj.Entity = coaCompObj.Entity;
                        mappedUnmappedObj.TotalAccounts = coaData.Count;
                        mappedUnmappedObj.TotalAccountsImported = mappedUnmappedObj.TotalAccounts;
                        mappedUnmappedObj.TotalMappedCount = mappedUnmappedObj.TotalMappedCount = ((IEnumerable<dynamic>)coaData).Where(f => !string.IsNullOrEmpty(f.FS_ID)).Count();
                        mappedUnmappedObj.TotalUnMappedCount = mappedUnmappedObj.TotalAccounts - mappedUnmappedObj.TotalMappedCount;

                        coaCompObj.MappedUnmappedCount = mappedUnmappedObj;
                        coaCompList.Add(coaCompObj);
                    }
                    viewModel.Data = coaCompList;
                    return viewModel;
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
