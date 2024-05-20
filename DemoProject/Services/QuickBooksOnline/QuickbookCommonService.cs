using Hangfire;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Security.Principal;
using System.Text;
using System.Threading.Tasks;

namespace DemoProject.Services.QuickBooksOnline
{
    public class QuickbookCommonService : IAppService
    {
        #region fields
        private readonly AppDbContext _context;
        private readonly OAuthService _oAuthService;
        private readonly QuickBookApiService _quickBookApiService;
        #endregion

        #region constructor

        public QuickbookCommonService(AppDbContext context,OAuthService oAuthService, QuickBookApiService quickBookApiService)
        {
            _context = context;
            _quickBookApiService = quickBookApiService;
            _oAuthService = oAuthService;

        }
        #endregion

        #region methods

        public async Task<List<InValidTokenCompanyDto>> GetInvalidTokenCompanies(ImportReportDto model)
        {
            var invalidTokenCompList = new List<InValidTokenCompanyDto>();
            if (model.CompanyId != null && model.CompanyId.Length > 0)
            {
                using (AppUnitOfWork uow = new AppUnitOfWork(_context))
                {
                    var companies = await uow.RefreshTokenRepository.FindByQueryable(r => r.TenantId == model.TenantId && model.CompanyId.Contains(r.CompanyId)).ToListAsync();
                    foreach (var item in companies)
                    {
                        var url = string.Format(ExternalUrl.IsTokenValid,item.CompanyId, item.Token);
                        var data = await _quickBookApiService.CallQuickBookApi<ValidTokenDto>(url);
                        if (!data.Status)
                        {
                            invalidTokenCompList.Add(new InValidTokenCompanyDto {CompanyId=item.CompanyId,CompanyName=item.CompanyName });
                        }
                    }
                }
            }
                    return invalidTokenCompList;
        }
        #endregion
    }
}
