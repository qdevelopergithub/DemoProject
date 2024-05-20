using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace DemoProject.Controller
{
    [Route("api/[controller]")]
    [ApiController]
    public class BaseApiController : ControllerBase
    {
        protected void LogError(Type type, Exception ex)
        {
        }

        protected IActionResult ApiResponse(bool status, string message, object result = null, string code = null)
        {
            if (result != null)
            {
                return Ok(new { Status = status, Message = message, Result = result, Code = code });
            }
            else
            {
                return Ok(new { Status = status, Message = message, Code = code });
            }
        }
    }
}
