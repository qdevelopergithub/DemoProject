using AutoMapper;
using Hangfire;
using LazyCache;
using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;
using SpreadsheetLight;
using System.Dynamic;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;

namespace DemoProject.Services.QuickBooksOnline
{
    public class QuickbookOnlineExcelService : IAppService
    {
        #region fields
        private readonly AppDbContext _context;
        private readonly ChartsOfAccountsService _chartsOfAccountsService;
        private readonly GeneralLedgerService _generalLedgerService;
        private readonly TrialBalanceService _trialBalanceService;
        private readonly QuickBookApiService _quickBookApiService;
        private readonly IMapper _mapper;
        private Dictionary<string, int> dictSpecialCharacters = new Dictionary<string, int>();
        #endregion

        #region constructor

        public QuickbookOnlineExcelService(
            AppDbContext context, ChartsOfAccountsService chartsOfAccountsService,
            GeneralLedgerService generalLedgerService, TrialBalanceService trialBalanceService,
            QuickBookApiService quickBookApiService, IMapper mapper
            )
        {
            _context = context;
            _chartsOfAccountsService = chartsOfAccountsService;
            _generalLedgerService = generalLedgerService;
            _trialBalanceService = trialBalanceService;
            _quickBookApiService = quickBookApiService;
            _mapper = mapper;
            GetSpecialCharsDictionary();
        }
        #endregion

        #region methods

        public async Task CreateExcelReportRequest(ReportDataDto model, string webroot, string baseUrl)
        {
            BackgroundJob.Schedule(() => GetCombinedReportString(model, webroot, baseUrl), TimeSpan.FromSeconds(5));
            RecurringJob.AddOrUpdate(() => RemoveInactiveImportRecords(), "0 8 * * *");
        }

        public async Task GetCombinedReportString(ReportDataDto model, string webroot, string baseUrl)
        {
            try
            {
                using (AppUnitOfWork uow = new AppUnitOfWork(_context))
                {
                    SLDocument sl = new SLDocument();
                    bool isAnyWorkSheetAdded = false;
                    var entityName = uow.NwzCompanyRepository.FindByQueryable(x => model.companyIds.Contains(x.ExtCompanyId)).FirstOrDefault()?.EntityUID;

                    #region COA
                    var coaReport = model.Reports.Where(f => f.Code == DataConstants.CHARTOFACCOUNTS && f.IsActive == true).FirstOrDefault();
                    if (coaReport != null)
                    {
                        var response = await _chartsOfAccountsService.GetChartOfAccountsAsync(uow, model);
                        sl = await CreateCOASheet(response, sl);
                        isAnyWorkSheetAdded = true;
                    }
                    #endregion

                    #region GL
                    var glReport = model.Reports.Where(f => f.Code == DataConstants.GENERALLEDGER && f.IsActive == true).FirstOrDefault();
                    if (glReport != null)
                    {
                        var response = await _generalLedgerService.GetGeneralLedgerReportAsync(uow, model);

                        if (isAnyWorkSheetAdded)
                            sl.AddWorksheet(DataConstants.GENERALLEDGER);
                        else
                            isAnyWorkSheetAdded = true;
                        sl = await CreateGeneralLedgerSheet(response, model.startDate, model.endDate, sl);
                    }
                    #endregion

                    #region TB
                    var tbReport = model.Reports.Where(f => f.Code == DataConstants.TRIALBALANCE && f.IsActive == true).FirstOrDefault();
                    if (tbReport != null)
                    {
                        #region trial balance report
                        var response = await _trialBalanceService.GetTrialReportsAsync(uow, model);
                        if (isAnyWorkSheetAdded)
                            sl.AddWorksheet(DataConstants.TRIALBALANCE);
                        else
                            isAnyWorkSheetAdded = true;
                        sl = await CreateTrialBalanceSheet(response, model.startDate, model.endDate, sl);
                        #endregion


                        #region TB count exception
                        var responseTBExceptional = await _trialBalanceService.GetTBInvalidExceptionReportsAsync(uow, model);
                        sl = await CreateTrialBalanceExceptionSheet(responseTBExceptional, sl);
                        #endregion
                    }
                    #endregion

                    string folder = $"Excels_{DateTime.Now.ToString(DataConstants.DATEFORMAT)}";
                    string fileName = $"REPORT-{Guid.NewGuid()}.xlsx";
                    var reportPeriod = $"{model.startDate}-{model.endDate}";
                    var url = $"{baseUrl}/{folder}/{fileName}";
                    var path = Path.Combine(webroot, folder);
                    if (!Directory.Exists(path))
                        Directory.CreateDirectory(path);
                    var file = path + $"\\{fileName}";
                    sl.SaveAs(file);
                    sl.Dispose();
                    await _quickBookApiService.QBOSendResponseToCallBackURI(model.tenantId, model.UserId, model.CallBackBaseUrl, model.CallBackApiUrl, url, model.startDate, model.endDate, entityName);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public async Task<string> CreateExcelRequestByReport(ReportDataDto model, string webroot, string baseUrl)
        {
            var path = await GetReportUrl(model, webroot, baseUrl);
            RecurringJob.AddOrUpdate(() => RemovePreviousFiles(webroot), "0 8 * * *");
            return path;
        }

        public async Task<string> GetReportUrl(ReportDataDto model, string webroot, string baseUrl)
        {
            using (AppUnitOfWork uow = new AppUnitOfWork(_context))
            {
                SLDocument sl = new SLDocument();
                var ifSubReportCode = string.Empty;
                var reportCode = string.Empty;
                var entityName = uow.NwzCompanyRepository.FindByQueryable(x => model.companyIds.Contains(x.ExtCompanyId)).FirstOrDefault()?.EntityUID;
                if (!string.IsNullOrEmpty(model.SubReportCode))
                {
                    ifSubReportCode = model.SubReportCode;
                    reportCode = model.SubReportCode;
                }
                else
                {
                    reportCode = model.reportName;
                }

                if (!string.IsNullOrWhiteSpace(ifSubReportCode))
                {
                    switch (ifSubReportCode)
                    {
                        #region TB
                        case "TRIALBALANCE":
                            var response = await _trialBalanceService.GetTrialReportsAsync(uow, model);
                            sl = await CreateTrialBalanceSheet(response, model.startDate, model.endDate, sl);
                            break;
                        #endregion

                        #region TB count exception
                        case "TBACCOUNTEXCEPTIONAL":
                            var responseTBExceptional = await _trialBalanceService.GetTBInvalidExceptionReportsAsync(uow, model);
                            sl = await CreateTrialBalanceExceptionSheet(responseTBExceptional, sl);
                            break;
                        #endregion

                        default:
                            break;
                    }
                }
                else
                {
                    switch (model.reportName)
                    {
                        #region COA
                        case "CHARTOFACCOUNTS":
                            var responseCOA = await _chartsOfAccountsService.GetChartOfAccountsAsync(uow, model);
                            sl = await CreateCOASheet(responseCOA, sl);
                            break;
                        #endregion

                        #region GL
                        case "GENERALLEDGER":
                            var responseGL = await _generalLedgerService.GetGeneralLedgerReportAsync(uow, model);
                            sl = await CreateGeneralLedgerSheet(responseGL, model.startDate, model.endDate, sl);
                            break;
                        #endregion

                        default:
                            break;
                    }
                }
                string folder = $"Excels_{DateTime.Now.ToString(DataConstants.DATEFORMAT)}";
                var path = Path.Combine(webroot, folder);
                if (!Directory.Exists(path))
                    Directory.CreateDirectory(path);
                string fileName = $"REPORT-{Guid.NewGuid()}.xlsx";
                var reportPeriod = $"{model.startDate}-{model.endDate}";
                var url = $"{baseUrl}/{folder}/{fileName}";
                var file = path + $"\\{fileName}";
                sl.SaveAs(file);
                sl.Dispose();
                return url;
            }
        }

        public async Task<bool> RemovePreviousFiles(string webroot)
        {
            try
            {
                string folder = $"Excels_{DateTime.Now.AddDays(-1).ToString(DataConstants.DATEFORMAT)}";
                var path = Path.Combine(webroot, folder);
                if (Directory.Exists(path))
                {
                    DirectoryInfo directory = new DirectoryInfo(path);
                    directory.Delete(true);
                }
                return true;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public async Task<bool> RemoveInactiveImportRecords()
        {
            try
            {
                using (AppUnitOfWork uow = new AppUnitOfWork(_context))
                {
                    #region GL
                    var glRecords = uow.GeneralLedgerRepository.FindLargeRecordsInQuery(null, null, null, null, true).Where(x => x.IsDeleted);
                    var removedGL = _mapper.Map<List<QuickbookRemovedGeneralLedger>>(glRecords);
                    uow.RemovedGeneralLedgerRepository.Insert(removedGL);
                    uow.GeneralLedgerRepository.Delete(glRecords);
                    #endregion

                    #region TB
                    var tlRecords = uow.TrialBalanceRepository.FindLargeRecordsInQuery(null, null, null, null, true).Where(x => x.IsDeleted);
                    var removedTB = _mapper.Map<List<QuickbookRemovedTrialBalance>>(tlRecords);
                    uow.RemovedTrialBalanceRepository.Insert(removedTB);
                    uow.TrialBalanceRepository.Delete(tlRecords);
                    #endregion

                    #region COA
                    var coaRecords = uow.ChartOfAccountsRepository.FindLargeRecordsInQuery(null, null, null, null, true).Where(x => x.IsDeleted);
                    var removedCOA = _mapper.Map<List<ChartOfAccountsRemovedEntity>>(coaRecords);
                    uow.RemovedChartOfAccountsRepository.Insert(removedCOA);
                    uow.ChartOfAccountsRepository.Delete(coaRecords);
                    #endregion

                    await uow.CommitAsync();
                }
                return false;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private async Task<SLDocument> CreateCOASheet(ReportResultDto result, SLDocument sl)
        {
            var companyName = result.CompanyEntityName;
            string jsonString = JsonConvert.SerializeObject(result.Data);
            var data = JsonConvert.DeserializeObject<ChartOfAccountReponse[]>(jsonString);
            if (data != null)
            {
                var createdOn = data.Any() && data.First().Data.Any() ? data.First()?.Data.First()?.CreatedOn.ToString(DataConstants.DATEFORMAT) : string.Empty;
                sl.SetCellValue("A1", "ENTITY:");
                sl.SetCellValue("B1", companyName.ToUpper());
                sl.SetCellValue("A2", "Last Updated:");
                sl.SetCellValue("B2", createdOn);
                int Index = 3;
                sl.SetCellValue("A" + Index, "Entity");
                sl.SetCellValue("B" + Index, "ACCT_ID");
                sl.SetCellValue("C" + Index, "Account_Description");
                sl.SetCellValue("D" + Index, "GCOA");
                sl.SetCellValue("E" + Index, "FS_TYPE");
                sl.SetCellValue("F" + Index, "Financial Report");
                sl.SetCellValue("G" + Index, "FS_ID");
                sl.SetCellValue("H" + Index, "FS_ID_Description");
                sl.SetCellValue("I" + Index, "FS_Sub category");
                sl.SetCellValue("J" + Index, "FS_Subcategory_2");
                sl.SetCellValue("K" + Index, "FS_Subcategory_3");
                sl.SetCellValue("L" + Index, "FS_Subcategory_4");
                sl.SetCellValue("M" + Index, "FS_Subcategory_5");
                sl.SetCellValue("N" + Index, "Location");
                sl.SetCellValue("O" + Index, "Currency");
                sl.SetCellValue("P" + Index, "NB_C_Sort_Code");
                sl.SetCellValue("Q" + Index, "Error_Check");
                Index++;
                foreach (var item in data)
                {
                    foreach (var collection in item.Data.OrderBy(x => x.AccountNumber))
                    {
                        dynamic fsId = string.IsNullOrEmpty(collection.FS_ID) ? "MISSING" : Convert.ToDouble(collection.FS_ID);
                        sl.SetCellValue("A" + Index, collection.CompanyEntity);
                        sl.SetCellValue("B" + Index, collection.AccountNumber);
                        if (string.IsNullOrEmpty(collection.NewAccountNumber))
                        {
                            sl.SetCellValue("D" + Index, "MISSING");
                        }
                        else
                        {
                            sl.SetCellValue("D" + Index, Convert.ToDouble(collection.NewAccountNumber));
                        }
                        sl.SetCellValue("C" + Index, collection.FullName);
                        sl.SetCellValue("E" + Index, collection.FS_Type);
                        sl.SetCellValue("F" + Index, collection.FinancialReport);
                        sl.SetCellValue("G" + Index, fsId);
                        sl.SetCellValue("H" + Index, collection.FS_ID_Description);
                        sl.SetCellValue("I" + Index, collection.Subcategory1_UID);
                        sl.SetCellValue("J" + Index, collection.Subcategory2_UID);
                        sl.SetCellValue("K" + Index, collection.Subcategory3_UID);
                        sl.SetCellValue("L" + Index, collection.Subcategory4_UID);
                        sl.SetCellValue("M" + Index, collection.Subcategory5_UID);
                        sl.SetCellValue("N" + Index, collection.Location);
                        sl.SetCellValue("O" + Index, collection.Currency);
                        sl.SetCellValue("P" + Index, collection.NB_C_Sort_Code);
                        sl.SetCellValue("Q" + Index, collection.Error_Check);
                        Index++;
                    }
                    Index++;
                }

                sl.RenameWorksheet("Sheet1", "COA REPORT");

                #region MappedUnMappedRegion
                sl.AddWorksheet("COAPROOF");

                sl.SetCellValue("A1", "Entity");
                sl.SetCellValue("A2", "Total number of accounts");
                sl.SetCellValue("A3", "Total number of accounts imported");
                sl.SetCellValue("A4", "Total accounts mapped");
                sl.SetCellValue("A5", "Total accounts unmapped");
                var headerColumnIndexCOA = 2;

                foreach (var item1 in data)
                {
                    sl.SetCellValue(ExcelColumnFromNumber(headerColumnIndexCOA) + "1", item1.Entity);
                    sl.SetCellValue(ExcelColumnFromNumber(headerColumnIndexCOA) + "2", item1.MappedUnmappedCount.TotalAccounts);
                    sl.SetCellValue(ExcelColumnFromNumber(headerColumnIndexCOA) + "3", item1.MappedUnmappedCount.TotalAccountsImported);
                    sl.SetCellValue(ExcelColumnFromNumber(headerColumnIndexCOA) + "4", item1.MappedUnmappedCount.TotalMappedCount);
                    sl.SetCellValue(ExcelColumnFromNumber(headerColumnIndexCOA) + "5", item1.MappedUnmappedCount.TotalUnMappedCount);
                    headerColumnIndexCOA++;
                }
                #endregion
            }
            else
            {
                sl.SetCellValue("A1", "ENTITY:");
                sl.SetCellValue("B1", " ");
                sl.SetCellValue("A2", "Last Updated:");
                sl.SetCellValue("B2", "");
                int Index = 3;
                sl.SetCellValue("A" + Index, "Entity");
                sl.SetCellValue("B" + Index, "ACCT_ID");
                sl.SetCellValue("C" + Index, "Account_Description");
                sl.SetCellValue("D" + Index, "GCOA");
                sl.SetCellValue("E" + Index, "FS_TYPE");
                sl.SetCellValue("F" + Index, "Financial Report");
                sl.SetCellValue("G" + Index, "FS_ID");
                sl.SetCellValue("H" + Index, "FS_ID_Description");
                sl.SetCellValue("I" + Index, "FS_Sub category");
                sl.SetCellValue("J" + Index, "FS_Subcategory_2");
                sl.SetCellValue("K" + Index, "FS_Subcategory_3");
                sl.SetCellValue("L" + Index, "FS_Subcategory_4");
                sl.SetCellValue("M" + Index, "FS_Subcategory_5");
                sl.SetCellValue("N" + Index, "Location");
                sl.SetCellValue("O" + Index, "Currency");
                sl.SetCellValue("P" + Index, "NB_C_Sort_Code");
                sl.SetCellValue("Q" + Index, "Error_Check");

                sl.RenameWorksheet("Sheet1", "COA Report");
                sl.AddWorksheet("COAPROOF");
                sl.SetCellValue("A1", "Entity");
                sl.SetCellValue("A2", "Total number of accounts");
                sl.SetCellValue("A3", "Total number of accounts imported");
                sl.SetCellValue("A4", "Total accounts mapped");
                sl.SetCellValue("A5", "Total accounts unmapped");
            }
            return sl;
        }

        private async Task<SLDocument> CreateGeneralLedgerSheet(GeneralLedgerReponse resultObj, string startDate, string endDate, SLDocument sl)
        {
            string jsonString = JsonConvert.SerializeObject(resultObj);

            var result = JsonConvert.DeserializeObject<GLReportResponse>(jsonString);
            var entity = result.GlList.FirstOrDefault()?.Entity;
            var customColumns = result.GlList.FirstOrDefault()?.CustomColumnNames;
            var companyName = entity;
            double debitTotal = 0;
            double creditTotal = 0;
            double amountTotal = 0;

            sl.SetCellValue("A1", "ENTITY:");
            sl.SetCellValue("B1", companyName?.ToUpper());
            sl.SetCellValue("A2", "PERIOD:");
            sl.SetCellValue("B2", startDate + " to " + endDate);
            int indexGL = 3;
            sl.SetCellValue("A" + indexGL, "Entity_Group");
            sl.SetCellValue("B" + indexGL, "Entity");
            sl.SetCellValue("C" + indexGL, "Entity_Name");
            sl.SetCellValue("D" + indexGL, "Account_Number_And_Name");
            sl.SetCellValue("E" + indexGL, "Account#");
            sl.SetCellValue("F" + indexGL, "Account");
            sl.SetCellValue("G" + indexGL, "Date");
            sl.SetCellValue("H" + indexGL, "Transaction_Type");
            sl.SetCellValue("I" + indexGL, "Transaction_Number");
            sl.SetCellValue("J" + indexGL, "Name");
            sl.SetCellValue("K" + indexGL, "Reference_Number");
            sl.SetCellValue("L" + indexGL, "Memo");
            sl.SetCellValue("M" + indexGL, "Description");
            sl.SetCellValue("N" + indexGL, "Location");
            sl.SetCellValue("O" + indexGL, "Class");
            sl.SetCellValue("P" + indexGL, "Department");
            sl.SetCellValue("Q" + indexGL, "Debit");
            sl.SetCellValue("R" + indexGL, "Credit");
            sl.SetCellValue("S" + indexGL, "Balance");
            sl.SetCellValue("T" + indexGL, "Amount");
            sl.SetCellValue("U" + indexGL, "Date");
            sl.SetCellValue("V" + indexGL, "Year_Month");
            sl.SetCellValue("W" + indexGL, "Period");
            sl.SetCellValue("X" + indexGL, "Account");
            sl.SetCellValue("Y" + indexGL, "Description");
            int columnNumber = 25;
            if (customColumns != null && customColumns.Count > 0)
            {
                foreach (var customCl in customColumns)
                {
                    columnNumber++;
                    var column = ExcelColumnFromNumber(columnNumber);
                    sl.SetCellValue(column + indexGL.ToString(), customCl);
                }
            }
            int iteration = 0;
            foreach (var item in result.GlList)
            {
                var groupedDatas = item.Data.GroupBy(x => x.Account).OrderBy(x => x).ToList();
                foreach (var groupedData in groupedDatas)
                {
                    foreach (var glRecord in groupedData)
                    {
                        var records = glRecord.Data.GroupBy(x => x.AccountNumber).OrderBy(x => x.Key).ToList();
                        foreach (var rows in records)
                        {
                            var collection = rows.FirstOrDefault();
                            var customDataValue = collection?.CustomColumns.Where(x => customColumns != null && customColumns.Contains(x?.columnName)).DistinctBy(x => x?.columnName).ToList();
                            var fs_intValue = string.Empty;
                            var fs_conValue = string.Empty;
                            if (customDataValue.Count > 0)
                            {
                                foreach (var customColumnValue in customDataValue)
                                {
                                    fs_conValue = customDataValue.FirstOrDefault(c => c.columnName.Contains("FS CON"))?.columnValue;
                                    fs_intValue = customDataValue.FirstOrDefault(c => c.columnName.Contains("FS INT"))?.columnValue;
                                }
                            }
                            double prevBalance = 0;

                            ///header row before beginning
                            indexGL++;
                            columnNumber = 25;
                            sl.SetCellValue("A" + indexGL, collection?.Entity_Group);
                            sl.SetCellValue("B" + indexGL, collection?.CompanyEntity);
                            sl.SetCellValue("C" + indexGL, collection?.CompanyName);
                            sl.SetCellValue("D" + indexGL, collection?.Account);
                            var begBalanceRecord = rows.Where(x => x.Date == DataConstants.BeginningBalance).FirstOrDefault();
                            var allRecords = rows.Where(x => x.Date != DataConstants.BeginningBalance).OrderBy(f => f.Period);
                            if (begBalanceRecord != null)
                            {
                                indexGL++;
                                columnNumber = 25;
                                var amount = string.IsNullOrEmpty(begBalanceRecord.Amount) ? 0 : Convert.ToDouble(begBalanceRecord.Amount);
                                prevBalance = string.IsNullOrEmpty(begBalanceRecord.Balance) ? 0 : Convert.ToDouble(begBalanceRecord.Balance);
                                sl.SetCellValue("A" + indexGL, collection?.Entity_Group);
                                sl.SetCellValue("B" + indexGL, collection?.CompanyEntity);
                                sl.SetCellValue("C" + indexGL, collection?.CompanyName);
                                sl.SetCellValue("G" + indexGL, begBalanceRecord.Date);
                                sl.SetCellValue("S" + indexGL, prevBalance);
                                sl.SetCellStyle("S" + indexGL, new SLStyle { FormatCode = DataConstants.AmountFormat });
                                sl.SetCellValue("T" + indexGL, amount);
                                sl.SetCellStyle("T" + indexGL, new SLStyle { FormatCode = amount > 0 ? DataConstants.AmountFormat : DataConstants.AmountNegativeFormat });
                                if (collection?.AccountNumber == null)
                                    sl.SetCellValue("X" + indexGL, "MISSING");
                                else
                                    sl.SetCellValue("X" + indexGL, (double)collection?.AccountNumber);
                                sl.SetCellValue("Y" + indexGL, collection?.AccountDescription);
                                if (collection?.CustomColumns != null)
                                {
                                    int ColumnCount = 0;
                                    foreach (var customCl in collection?.CustomColumnNames)
                                    {
                                        columnNumber++;
                                        if (customColumns != null && ColumnCount <= customColumns.Count)
                                        {
                                            var column = ExcelColumnFromNumber(columnNumber);
                                            if (customCl.Contains("FS INT"))
                                            {
                                                sl.SetCellValue(column + indexGL.ToString(), string.IsNullOrEmpty(fs_intValue) || fs_intValue == "MISSING" ? 0 : Convert.ToDouble(fs_intValue));
                                            }
                                            if (customCl.Contains("FS CON"))
                                            {
                                                sl.SetCellValue(column + indexGL.ToString(), string.IsNullOrEmpty(fs_conValue) || fs_conValue == "MISSING" ? 0 : Convert.ToDouble(fs_conValue));
                                            }
                                            ColumnCount++;
                                        }
                                    }
                                }
                            }
                            foreach (var row in allRecords)
                            {
                                if (!string.IsNullOrEmpty(row.Entity_Group))
                                {
                                    indexGL++;
                                    columnNumber = 25;
                                    double debit = string.IsNullOrEmpty(row.Debit) ? 0 : Convert.ToDouble(row.Debit);
                                    double credit = string.IsNullOrEmpty(row.Credit) ? 0 : Convert.ToDouble(row.Credit);
                                    double amount = string.IsNullOrEmpty(row.Amount) ? 0 : Convert.ToDouble(row.Amount);
                                    sl.SetCellValue("A" + indexGL, row.Entity_Group);
                                    sl.SetCellValue("B" + indexGL, row.CompanyEntity);
                                    sl.SetCellValue("C" + indexGL, row.CompanyName);
                                    if (row.AccountNumber == null)
                                    {
                                        sl.SetCellValue("E" + indexGL, "MISSING");
                                        sl.SetCellValue("X" + indexGL, "MISSING");
                                    }
                                    else
                                    {
                                        sl.SetCellValue("E" + indexGL, Convert.ToDouble(row.AccountNumber));
                                        sl.SetCellValue("X" + indexGL, Convert.ToDouble(row.AccountNumber));
                                    }
                                    sl.SetCellValue("F" + indexGL, row.Account);
                                    sl.SetCellValue("G" + indexGL, Convert.ToDateTime(row.Date).ToString("yyyy-MM-dd"));
                                    sl.SetCellValue("H" + indexGL, row.Transaction_Type);
                                    sl.SetCellValue("I" + indexGL, row.Doc_Num);
                                    sl.SetCellValue("J" + indexGL, row.Name);
                                    sl.SetCellValue("K" + indexGL, string.Empty);
                                    sl.SetCellValue("L" + indexGL, ReplaceSpecialCharacters(row.Memo_Description));
                                    sl.SetCellValue("M" + indexGL, row.Description);
                                    sl.SetCellValue("N" + indexGL, row.Location);
                                    sl.SetCellValue("O" + indexGL, row.Class?.ToString());
                                    sl.SetCellValue("P" + indexGL, row.Department);
                                    sl.SetCellValue("Q" + indexGL, debit);
                                    sl.SetCellStyle("Q" + indexGL, new SLStyle { FormatCode = DataConstants.AmountFormat });
                                    sl.SetCellValue("R" + indexGL, credit);
                                    sl.SetCellStyle("R" + indexGL, new SLStyle { FormatCode = DataConstants.AmountFormat });
                                    sl.SetCellValue("T" + indexGL, amount);
                                    sl.SetCellStyle("T" + indexGL, new SLStyle { FormatCode = amount > 0 ? DataConstants.AmountFormat : DataConstants.AmountNegativeFormat });
                                    sl.SetCellValue("U" + indexGL, "=IF(T" + indexGL + "=0,\"\",IF(G" + indexGL + "=\"\",\"\",DATE(LEFT(G" + indexGL + ",4),MID(G" + indexGL + ",6,2),RIGHT(G" + indexGL + ",2))))");
                                    sl.SetCellValue("V" + indexGL, row.YearMonth);
                                    sl.SetCellValue("W" + indexGL, string.IsNullOrEmpty(row.Period) ? 0 : Convert.ToDouble(row.Period));
                                    sl.SetCellValue("Y" + indexGL, row.AccountDescription);
                                    prevBalance = prevBalance + debit - credit;
                                    sl.SetCellValue("S" + indexGL, string.IsNullOrEmpty(row.Balance) ? 0 : prevBalance);
                                    sl.SetCellStyle("S" + indexGL, new SLStyle { FormatCode = DataConstants.AmountFormat });

                                    debitTotal += debit;
                                    creditTotal += credit;
                                    amountTotal += amount;
                                    if (row.CustomColumns != null)
                                    {
                                        int ColumnCount = 0;
                                        foreach (var customCl in row.CustomColumnNames)
                                        {
                                            if (customColumns != null && ColumnCount <= customColumns.Count)
                                            {
                                                columnNumber++;

                                                var column = ExcelColumnFromNumber(columnNumber);
                                                string customValue = customCl.Contains("FS INT") ? fs_intValue : customCl.Contains("FS CON") ? fs_conValue : null;

                                                if (!string.IsNullOrEmpty(customValue) && customValue != "MISSING")
                                                {
                                                    sl.SetCellValue(column + indexGL.ToString(), Convert.ToDouble(customValue));
                                                }
                                                else
                                                {
                                                    sl.SetCellValue(column + indexGL.ToString(), 0);
                                                }
                                                ColumnCount++;
                                            }
                                        }
                                    }
                                }
                            }
                            indexGL++;
                            sl.SetCellValue("A" + indexGL, collection?.Entity_Group);
                            sl.SetCellValue("B" + indexGL, collection?.CompanyEntity);
                            sl.SetCellValue("C" + indexGL, collection?.CompanyName);
                            sl.SetCellValue("D" + indexGL, "Total For " + collection?.Account);
                            sl.SetCellValue("Q" + indexGL, debitTotal);
                            sl.SetCellStyle("Q" + indexGL, new SLStyle { FormatCode = DataConstants.AmountFormat });
                            sl.SetCellValue("R" + indexGL, creditTotal);
                            sl.SetCellStyle("R" + indexGL, new SLStyle { FormatCode = DataConstants.AmountFormat });
                            sl.SetCellValue("T" + indexGL, amountTotal);
                            sl.SetCellStyle("T" + indexGL, new SLStyle { FormatCode = amountTotal > 0 ? DataConstants.AmountFormat : DataConstants.AmountNegativeFormat });
                            debitTotal = 0;
                            creditTotal = 0;
                            amountTotal = 0;
                        }
                    }
                }
            }
            sl.RenameWorksheet("Sheet1", $"{DataConstants.GENERALLEDGER} REPORT");

            #region TotalBalance

            sl.AddWorksheet(DataConstants.GENERALLEDGER + " PROOF");
            sl.SetCellValue("A1", "Entity");
            sl.SetCellValue("A2", "Number of transactions");
            sl.SetCellValue("A3", "Number of transactions imported");

            var headerColumnIndexgl = 2;

            foreach (var item in result.BalanceTransactions)
            {
                var albphabetCloumn = ExcelColumnFromNumber(headerColumnIndexgl);
                sl.SetCellValue(albphabetCloumn + "1", item.Entity);
                sl.SetCellValue(albphabetCloumn + "2", item.TotalTransactions);
                sl.SetCellValue(albphabetCloumn + "3", item.TotalTransactionsImported);
                headerColumnIndexgl++;
            }

            var entityColmn = ExcelColumnFromNumber(headerColumnIndexgl++);
            var listOfAccntColmn = ExcelColumnFromNumber(headerColumnIndexgl++);
            var tbBeginBalColmn = ExcelColumnFromNumber(headerColumnIndexgl++);
            var netTransBalColmn = ExcelColumnFromNumber(headerColumnIndexgl++);
            var tbEndBalColmn = ExcelColumnFromNumber(headerColumnIndexgl++);
            var checkColmn = ExcelColumnFromNumber(headerColumnIndexgl);

            sl.SetCellValue(entityColmn + "1", "Entity");
            sl.SetCellValue(listOfAccntColmn + "1", "List of Accounts");
            sl.SetCellValue(tbBeginBalColmn + "1", "TB beginning balance");
            sl.SetCellValue(netTransBalColmn + "1", "Net transactions");
            sl.SetCellValue(tbEndBalColmn + "1", "TB Ending Balance");
            sl.SetCellValue(checkColmn + "1", "Check");
            var rowCount = 2;
            foreach (var item in result.BalanceTransactions)
            {
                foreach (var comp in item.AccountNumberBalances)
                {
                    headerColumnIndexgl++;
                    sl.SetCellValue(entityColmn + rowCount.ToString(), item.Entity);
                    if (string.IsNullOrEmpty(comp.AccountNumber))
                        sl.SetCellValue(listOfAccntColmn + rowCount.ToString(), "MISSING");
                    else
                        sl.SetCellValue(listOfAccntColmn + rowCount.ToString(), Convert.ToDecimal(comp.AccountNumber));
                    sl.SetCellValue(tbBeginBalColmn + rowCount.ToString(), comp.BegginingBalance);
                    sl.SetCellValue(netTransBalColmn + rowCount.ToString(), comp.NetTransactions);
                    sl.SetCellValue(tbEndBalColmn + rowCount.ToString(), comp.EndBalance);
                    sl.SetCellValue(checkColmn + rowCount.ToString(), comp.Check);
                    rowCount++;
                }
            }

            #endregion
            return sl;
        }

        private async Task<SLDocument> CreateTrialBalanceSheet(TBReportResponse data, string startDate, string endDate, SLDocument sl)
        {
            string jsonString = JsonConvert.SerializeObject(data);

            // Deserialize the JSON to the specified class
            var responseTBReport = JsonConvert.DeserializeObject<TrialBalanaceResponse>(jsonString);
            var companyName = responseTBReport.TrialBalanceData?.FirstOrDefault()?.Entity_Name;
            var customColumnsTB = responseTBReport.TrialBalanceData?.FirstOrDefault()?.Data.FirstOrDefault()?.Data.FirstOrDefault()?.CustomColumnNames;

            sl.SetCellValue("A1", "ENTITY:");
            sl.SetCellValue("B1", companyName?.ToUpper());
            sl.SetCellValue("A2", "PERIOD :");
            sl.SetCellValue("B2", startDate + '-' + endDate);
            int indexTBR = 3;
            sl.SetCellValue("A" + indexTBR, "Period");
            sl.SetCellValue("B" + indexTBR, "Entity");
            sl.SetCellValue("C" + indexTBR, "Account Number");
            sl.SetCellValue("D" + indexTBR, "Account Description");
            sl.SetCellValue("E" + indexTBR, "FS Type");
            sl.SetCellValue("F" + indexTBR, "Debit");
            sl.SetCellValue("G" + indexTBR, "Credit");
            sl.SetCellValue("H" + indexTBR, "Net");
            sl.SetCellValue("I" + indexTBR, "Adjusting Entry");
            sl.SetCellValue("J" + indexTBR, "Adjusted TB");
            int columnNumberTB = 10;
            if (customColumnsTB != null && customColumnsTB.Count > 0)
            {
                foreach (var customCl in customColumnsTB)
                {
                    columnNumberTB++;
                    var column = ExcelColumnFromNumber(columnNumberTB);
                    sl.SetCellValue(column + indexTBR.ToString(), customCl);
                }
            }
            indexTBR++;
            foreach (var item in responseTBReport.TrialBalanceData)
            {
                foreach (var reportData in item.Data)
                {
                    foreach (var collection in reportData.Data)
                    {
                        sl.SetCellValue("A" + indexTBR, collection.Period);
                        sl.SetCellValue("B" + indexTBR, collection.Company_Entity);
                        if (string.IsNullOrEmpty(collection.AccountNumber))
                            sl.SetCellValue("C" + indexTBR, "MISSING");
                        else
                            sl.SetCellValue("C" + indexTBR, Convert.ToDouble(collection.AccountNumber));
                        sl.SetCellValue("D" + indexTBR, collection.AccountDescription);
                        sl.SetCellValue("E" + indexTBR, collection.FS_Type);
                        sl.SetCellValue("F" + indexTBR, string.IsNullOrEmpty(collection.Debit) ? 0 : Convert.ToDouble(collection.Debit));
                        sl.SetCellStyle("F" + indexTBR, new SLStyle { FormatCode = DataConstants.AmountFormat });
                        sl.SetCellValue("G" + indexTBR, string.IsNullOrEmpty(collection.Credit) ? 0 : Convert.ToDouble(collection.Credit));
                        sl.SetCellStyle("G" + indexTBR, new SLStyle { FormatCode = DataConstants.AmountFormat });
                        sl.SetCellValue("H" + indexTBR, collection.Net);
                        sl.SetCellStyle("H" + indexTBR, new SLStyle { FormatCode = collection.Net > 0 ? DataConstants.AmountFormat : DataConstants.AmountNegativeFormat });
                        sl.SetCellValue("I" + indexTBR, collection.AdjustingEntry);
                        sl.SetCellStyle("I" + indexTBR, new SLStyle { FormatCode = collection.AdjustingEntry > 0 ? DataConstants.AmountFormat : DataConstants.AmountNegativeFormat });
                        sl.SetCellValue("J" + indexTBR, collection.AdjustingTb);
                        sl.SetCellStyle("J" + indexTBR, new SLStyle { FormatCode = collection.AdjustingTb > 0 ? DataConstants.AmountFormat : DataConstants.AmountNegativeFormat });
                        columnNumberTB = 10;

                        if (collection.CustomColumns != null)
                        {
                            int ColumnCount = 0;
                            if (customColumnsTB != null)
                            {
                                foreach (var customCl in customColumnsTB)
                                {
                                    if (ColumnCount <= customColumnsTB.Count)
                                    {
                                        columnNumberTB++;
                                        var column = ExcelColumnFromNumber(columnNumberTB);
                                        string valueToSet = collection.FS_ID; 
                                        if (!string.IsNullOrEmpty(valueToSet) && valueToSet != "MISSING")
                                        {
                                            sl.SetCellValue(column + indexTBR.ToString(), Convert.ToDouble(valueToSet));
                                        }
                                        else
                                        {
                                            sl.SetCellValue(column + indexTBR.ToString(), "MISSING");
                                        }
                                        ColumnCount++;
                                    }
                                }
                            }
                        }
                        indexTBR++;
                    }
                }
            }
            sl.RenameWorksheet("Sheet1", $"{DataConstants.TRIALBALANCE} REPORT");
            #region MappedUnMappedRegion

            sl.AddWorksheet("TBPROOF");
            sl.SetCellValue("A1", "Entity");
            sl.SetCellValue("A2", "Total number of accounts");
            sl.SetCellValue("A3", "Total net Balance");
            sl.SetCellValue("A4", "Number of accounts mapped");
            sl.SetCellValue("A5", "Number of accounts unmapped");
            var headerColumnIndex = 2;

            foreach (var item in responseTBReport.MappedUnMappedData)
            {
                var albphabetCloumn = ExcelColumnFromNumber(headerColumnIndex);
                sl.SetCellValue(albphabetCloumn + "1", item.Entity);
                sl.SetCellValue(albphabetCloumn + "2", item.TotalAccounts);
                sl.SetCellValue(albphabetCloumn + "3", item.TotalNetBalance);
                sl.SetCellValue(albphabetCloumn + "4", item.TotalMappedAccounts);
                sl.SetCellValue(albphabetCloumn + "5", item.TotalUnMappedAccounts);
                headerColumnIndex++;
            }

            sl.SetCellValue(ExcelColumnFromNumber(headerColumnIndex) + "1", "Entity");
            var rowCount = 2; var isEntitiesPrinted = false; var entityCount = 0;
            foreach (var item in responseTBReport.TrialBalanceData)
            {
                entityCount = item.PeriodBalanceData.Count();
                if (!isEntitiesPrinted)
                {
                    foreach (var comp in item.PeriodBalanceData)
                    {
                        headerColumnIndex++;
                        sl.SetCellValue(ExcelColumnFromNumber(headerColumnIndex) + "1", comp.Entity);
                    }
                    isEntitiesPrinted = true;
                    headerColumnIndex = headerColumnIndex - entityCount;
                }
                var isPeriodPrinted = false; var nextColumn = 1;
                foreach (var comp in item.PeriodBalanceData)
                {
                    if (!isPeriodPrinted)
                    {
                        sl.SetCellValue(ExcelColumnFromNumber(headerColumnIndex) + rowCount.ToString(), "Period" + comp.FcPeriod.ToString() + " balance");
                        isPeriodPrinted = true;
                        headerColumnIndex++;
                    }
                    sl.SetCellValue(ExcelColumnFromNumber(headerColumnIndex) + rowCount.ToString(), comp.Balance);
                    headerColumnIndex++;
                }
                headerColumnIndex = headerColumnIndex - (entityCount + 1);//1 is period column index
                rowCount++;
            }
            #endregion
            return sl;
        }

        private async Task<SLDocument> CreateTrialBalanceExceptionSheet(ReportResultDto model, SLDocument sl)
        {
            int indexTBE = 1;
            sl.AddWorksheet("TBEXCEPTIONS");
            sl.SetCellValue("A" + indexTBE, "Entity");
            sl.SetCellValue("B" + indexTBE, "AccountNumber");
            sl.SetCellValue("C" + indexTBE, "Account_Description");
            indexTBE++;
            if (model != null)
            {
                string jsonString = JsonConvert.SerializeObject(model);

                var data = JsonConvert.DeserializeObject<GetTrailBalanceExceptionReportDto>(jsonString);
                foreach (var collection in data.Data)
                {
                    sl.SetCellValue("A" + indexTBE, collection.Entity);
                    sl.SetCellValue("B" + indexTBE, collection.AccountNumber);
                    sl.SetCellValue("C" + indexTBE, collection.AccountDescription);
                    indexTBE++;
                }
            }
            else
            {
                sl.AddWorksheet("TBEXCEPTIONS");
                sl.SetCellValue("A" + indexTBE, "Entity");
                sl.SetCellValue("B" + indexTBE, "AccountNumber");
                sl.SetCellValue("C" + indexTBE, "Account_Description");
            }
            return sl;
        }

        // replace special chars with char code
        private string ReplaceSpecialCharacters(string input)
        {
            string result = string.Empty;
            Regex regex = new Regex(@"[^\w\s]");
            var specialCharacters = regex.Matches(input).Select(x => x.Value);
            var specialChars = specialCharacters.Distinct().ToList();
            var matchResults = dictSpecialCharacters.Where(x => specialChars.Contains(x.Key));
            input = input.Replace("\u0002", "");
            if (specialChars.Any())
            {
                foreach (var specialChar in specialChars)
                {
                    var spcChar = $"'{specialChar.ToString()}'";
                    var charCode = Convert.ToInt32(spcChar[1]);
                    input = input.Replace(spcChar[1].ToString(), $"SFValCHAROPENBRACES{charCode.ToString()}CLOSEBRACESEFVal");

                }
                input = input.Replace("SFVal", "\"&").Replace("EFVal", "&\"")
                        .Replace("OPENBRACES", "(").Replace("CLOSEBRACES", ")");
                result = String.Format("={0}", $"\"{input}\"");
            }
            else
            {
                result = input;
            }
            return result;
        }

        // get special characters dictionary
        private Dictionary<string, int> GetSpecialCharsDictionary()
        {
            for (int i = 33; i <= 64; i++)
            {
                if (i > 47 && i < 58)
                {
                    continue;
                }
                char character = (char)i;
                string characterAsString = character.ToString();
                dictSpecialCharacters.Add(characterAsString, i);
            }
            return dictSpecialCharacters;
        }

        public static string ExcelColumnFromNumber(int column)
        {
            string columnString = "";
            decimal columnNumber = column;
            while (columnNumber > 0)
            {
                decimal currentLetterNumber = (columnNumber - 1) % 26;
                char currentLetter = (char)(currentLetterNumber + 65);
                columnString = currentLetter + columnString;
                columnNumber = (columnNumber - (currentLetterNumber + 1)) / 26;
            }
            return columnString;
        }

        #endregion
    }
}