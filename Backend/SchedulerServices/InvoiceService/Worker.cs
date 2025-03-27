using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Data.SqlClient;
using RestSharp;
using System.Data;
using System.Globalization;
using System.Text;
using System.Xml.Linq;

namespace InvoiceService
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly string _connectionString = string.Empty;

        private readonly string apiUrl = string.Empty;
        private readonly string username = string.Empty;
        private readonly string password = string.Empty;
        private readonly string _logFolderPath;
        /// private static string[]? fields;

        public Worker(ILogger<Worker> logger, IConfiguration configuration)
        {
            _logger = logger;
            // Get the log folder path from the configuration
            _logFolderPath = configuration["Logging:LogFolderPath"];

            // Ensure that the directory exists
            if (!Directory.Exists(_logFolderPath))
            {
                Directory.CreateDirectory(_logFolderPath);
            }
            // Reading the connection string from appsettings.json
            _connectionString = configuration.GetConnectionString("DefaultConnection");
            apiUrl = configuration["AppSettings:ApiUrl"];
            username = configuration["AppSettings:Username"];
            password = configuration["AppSettings:Password"];
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);

                // Call API and store data into the database
                await FetchAndStoreData();

                // Wait for some time before calling again
                await Task.Delay(TimeSpan.FromHours(2), stoppingToken); // Delay for 10 minutes (adjust as needed)
            }
        }

        private static DataTable ParseCsvToDataTable(string csvData)
        {
            // Create a DataTable to hold the parsed CSV data
            DataTable dataTable = new DataTable();

            try
            {
                // Create a StringReader to simulate a stream from the csvData string
                using (var stringReader = new StringReader(csvData))
                {
                    using (var csv = new CsvReader(stringReader, new CsvConfiguration(CultureInfo.InvariantCulture)
                    {
                        // Handle newlines inside quoted fields
                        AllowComments = true,
                        HasHeaderRecord = true, // If your CSV has headers
                        BadDataFound = null, // Ignore bad data
                        MissingFieldFound = null // Ignore missing fields
                    }))
                    {
                        // Read the header row (column names)
                        csv.Read();
                        csv.ReadHeader();

                        // Create DataTable columns from header
                        foreach (var header in csv.HeaderRecord)
                        {
                            dataTable.Columns.Add(header); // Add column to DataTable
                        }

                        // Read records and populate DataTable
                        while (csv.Read())
                        {
                            DataRow row = dataTable.NewRow();
                            foreach (var header in csv.HeaderRecord)
                            {
                                row[header] = csv.GetField(header); // Populate row with field values
                            }
                            dataTable.Rows.Add(row);
                        }
                    }
                }
            }
            catch (Exception ex)
            {

                Console.WriteLine($"Error while parsing the csv to datatable {ex.Message.ToString()}");
            }

            return dataTable;
        }
        private async Task FetchAndStoreData()
        {
            try
            {

                var options = new RestClientOptions("https://fa-evjh-saasfaprod1.fa.ocs.oraclecloud.com")
                {
                    MaxTimeout = -1,
                };
                var client = new RestClient(options);
                var request = new RestRequest("/xmlpserver/services/PublicReportService?WSDL", Method.Post);
                request.AddHeader("Content-Type", "application/soap+xml");

                DateTime dateTime = await GetLastRunDatetimeAsync();// You can pass any DateTime value here
                DateTime CurrentDateTime = DateTime.Now;
                var body = GetBody(dateTime);
                //Console.WriteLine(body);
                request.AddParameter("application/xml", body, ParameterType.RequestBody);
                //request.AddStringBody(body, DataFormat.Xml);
                // Send the request and get the response



                var response = await client.ExecuteAsync(request);

                if (response.IsSuccessful)
                {
                    string responseBody = response.Content;

                    // Parse the XML response
                    XDocument doc = XDocument.Parse(responseBody);

                    // Namespace handling for the XML response
                    XNamespace ns = "http://xmlns.oracle.com/oxp/service/PublicReportService";

                    // Extract the reportBytes and reportContentType
                    var reportBytesBase64 = doc.Descendants(ns + "reportBytes").FirstOrDefault()?.Value;
                    var reportContentType = doc.Descendants(ns + "reportContentType").FirstOrDefault()?.Value;
                    string data = string.Empty;

                    // Check if reportBytes exists
                    if (!string.IsNullOrEmpty(reportBytesBase64))
                    {
                        // Decode the Base64-encoded reportBytes
                        byte[] reportBytes = Convert.FromBase64String(reportBytesBase64);
                        // Check if the content is plain text and decode it accordingly
                        if (reportContentType.Contains("text/plain"))
                        {
                            data = Encoding.UTF8.GetString(reportBytes);

                        }
                        else
                        {
                            _logger.LogWarning("The content is not text.");
                        }
                    }
                    else
                    {
                       _logger.LogWarning("No reportBytes found in the XML.");
                    }

                    if(data.Length<10)
                    {
                        _logger.LogWarning($"Empty Data set Found {data.Replace("\n","")}.");
                        await UpdateLastRunDatetime(CurrentDateTime);
                        return;
                    }
                    // DataTable dataTable = ConvertCsvToDataTable(data);
                    DataTable dataTable = ParseCsvToDataTable(data);
                    if (dataTable != null && dataTable.Rows.Count > 0)
                    {
                        // Store the data into the SQL Server database
                        bool isDBOperationSuccess = await StoreDataToDatabase(dataTable);
                        if (isDBOperationSuccess)
                        {
                            await UpdateLastRunDatetime(CurrentDateTime);
                        }

                    }

                    //var data = JsonSerializer.Deserialize<ApiResponse>(responseBody);
                    if (string.IsNullOrEmpty(data))
                        return;

                }
                else
                {
                    // Log failed response
                   _logger.LogError($"API request failed. Status code: {response.StatusCode}");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"An error occurred: {ex.Message}");
            }
        }



        private string GetBody(DateTime dateTime)
        {
            StringBuilder sb = new();
            sb.AppendLine("<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:pub=\"http://xmlns.oracle.com/oxp/service/PublicReportService\">");
            sb.AppendLine(" <soapenv:Header/>");
            sb.AppendLine(" <soapenv:Body>");
            sb.AppendLine(" <pub:runReport>");
            sb.AppendLine(" <pub:reportRequest>");
            sb.AppendLine(" <pub:attributeFormat>csv</pub:attributeFormat>");
            sb.AppendLine(" <pub:parameterNameValues>");
            sb.AppendLine(" <pub:item>");
            sb.AppendLine(" <pub:name>p_trx_number</pub:name>");
            sb.AppendLine(" <pub:values>");
            sb.AppendLine(" <pub:item></pub:item>");
            sb.AppendLine(" </pub:values>");
            sb.AppendLine(" <pub:name>p_last_rundate</pub:name>");
            sb.AppendLine(" <pub:values>");
            sb.AppendLine($" <pub:item>{dateTime:yyyy-MM-dd HH:mm}</pub:item>");
            sb.AppendLine(" </pub:values>");
            sb.AppendLine(" </pub:item>");
            sb.AppendLine(" </pub:parameterNameValues>");
            sb.AppendLine(" <pub:reportAbsolutePath>/Custom/Integrations/JBM AR Invoice Print.xdo</pub:reportAbsolutePath>");
            sb.AppendLine(" </pub:reportRequest>");
            sb.AppendLine($" <pub:userID>{username}</pub:userID>");
            sb.AppendLine($" <pub:password>{password}</pub:password>");
            sb.AppendLine(" </pub:runReport>");
            sb.AppendLine(" </soapenv:Body>");
            sb.AppendLine(" </soapenv:Envelope>");

            return sb.ToString();
        }

        private async Task<bool> StoreDataToDatabase(DataTable data)
        {
            int? txnNumber = 0;
            int? prvTrxNumber = 0;
            try
            {
                using (SqlConnection connection = new SqlConnection(_connectionString))
                {
                    await connection.OpenAsync();

                    // Check if the SalesInvoiceFile already exists

                    foreach (DataRow row in data.Rows)
                    {
                        if (string.IsNullOrEmpty(Convert.ToString(row["SALES_ORDER"])))
                        {
                            continue;
                        }

                        txnNumber = Convert.ToInt32(row["TRX_NUMBER"]);
                        if (prvTrxNumber == txnNumber)
                        {
                            continue;
                        }
                        int invoiceId = 0;
                        invoiceId = await InvoiceExists(connection, txnNumber);
                        if (invoiceId <= 0)
                        {
                            // Create new invoice file
                            int newInvoiceId = await InsertInvoiceData(connection, row);
                            if (newInvoiceId > 0)
                            {
                                #region ListLineOfItems

                                // Filter the DataTable using a DataView
                                DataView view = new DataView(data);

                                // Apply a filter (e.g., select only HR department)
                                view.RowFilter = "TRX_NUMBER =  '" + txnNumber + "'";
                                DataTable filteredTable = view.ToTable();

                                #endregion
                                await InsertInvoiceLineItems(connection, newInvoiceId, filteredTable);
                            };
                        }
                        else
                        {
                            //If invoice already exist
                            //update the invoiceStatus,
                            await UpdateInvoiceStatus(connection, invoiceId, row);
                            // Filter the DataTable using a DataView
                            DataView view = new DataView(data);

                            // Apply a filter (e.g., select only HR department)
                            view.RowFilter = "TRX_NUMBER =  '" + txnNumber + "'";
                            DataTable filteredTable = view.ToTable();


                            await UpsertInvoiceLineItems(connection, invoiceId, filteredTable);
                        }

                        prvTrxNumber = txnNumber;

                    }
                }
                return true;
                // Console.WriteLine("Data successfully inserted into the database.");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error storing data to the database: {ex.Message} and the txnno={txnNumber}");
                return false;
            }
        }


        private async Task<int> InsertInvoiceData(SqlConnection connection, DataRow row)
        {
            int Id = 0;
            var sql = @" 
                           INSERT INTO [dbo].[Invoice]
                            ([BillToPartyId],[BillToCustomerName],[CfBillToSiteName],[BillToLocationId],[BillToAddress1],[BillToAddress2],[BillToAddress3],[BillToAddress4]
                            ,[BillToCity],[BillToState],[BillToPostalCode],[BillToCountry]
                            ,[ShipToPartyId],[ShipToPartySiteId],[ShipToCustomerName],[ShipCustSiteName],[ShipToLocationId],[ShipToAddress1],[ShipToAddress2],[ShipToAddress3],[ShipToAddress4]
                            ,[ShipToCity],[ShipToState],[ShipToPostalCode],[ShipToCountry]
                            ,[TrxNumber],[TrxDate],[TermName],[ShipDateActual],[SalesOrder]
                            ,[PrimarySalesRepName],[ShipVia],[PurchaseOrderNumber],[BillToCustomerNumber],[InternalNotes]
                            ,[TaxAmount],[FreightAmount],[TotalAmount],[DiscountTakenEarned],[AmountApplied],[AmountDueRemaining]
                            ,[Status],[DueDate],[CfFromDate],[CfToDate],[TrxType],[TotalNet]
                            ,[CreatedDate])
                            VALUES
                            (@BillToPartyId,@BillToCustomerName,@CfBillToSiteName,@BillToLocationId,@BillToAddress1,@BillToAddress2,@BillToAddress3,@BillToAddress4
                            ,@BillToCity,@BillToState,@BillToPostalCode,@BillToCountry
                            ,@ShipToPartyId,@ShipToPartySiteId,@ShipToCustomerName,@ShipCustSiteName,@ShipToLocationId,@ShipToAddress1,@ShipToAddress2,@ShipToAddress3,@ShipToAddress4
                            ,@ShipToCity,@ShipToState,@ShipToPostalCode,@ShipToCountry
                            ,@TrxNumber,@TrxDate,@TermName,@ShipDateActual,@SalesOrder
                            ,@PrimarySalesRepName,@ShipVia,@PurchaseOrderNumber,@BillToCustomerNumber,@InternalNotes
                            ,@TaxAmount,@FreightAmount,@TotalAmount,@DiscountTakenEarned,@AmountApplied,@AmountDueRemaining
                            ,@Status,@DueDate,@CfFromDate,@CfToDate,@TrxType,@TotalNet
                            ,@CreatedDate)
                            SELECT @Id = SCOPE_IDENTITY();";

            try
            {
                using var command = new SqlCommand(sql, connection);
                // Add parameters in the specified table sequence
                command.Parameters.AddWithValue("@BillToPartyId", Convert.ToInt64(row["BILL_TO_PARTY_ID"] ?? 0));
                command.Parameters.AddWithValue("@BillToCustomerName", Convert.ToString(row["BILL_TO_CUSTOMER_NAME"]) ?? string.Empty);
                command.Parameters.AddWithValue("@CfBillToSiteName", Convert.ToString(row["CF_BILL_TO_SITE_NAME"]) ?? string.Empty);
                command.Parameters.AddWithValue("@BillToLocationId", Convert.ToInt64(row["BILL_TO_LOCATION_ID"] ?? 0));
                command.Parameters.AddWithValue("@BillToAddress1", Convert.ToString(row["BILL_TO_ADDRESS1"]) ?? string.Empty);
                command.Parameters.AddWithValue("@BillToAddress2", Convert.ToString(row["BILL_TO_ADDRESS2"]) ?? string.Empty);
                command.Parameters.AddWithValue("@BillToAddress3", Convert.ToString(row["BILL_TO_ADDRESS3"]) ?? string.Empty);
                command.Parameters.AddWithValue("@BillToAddress4", Convert.ToString(row["BILL_TO_ADDRESS4"]) ?? string.Empty);
                command.Parameters.AddWithValue("@BillToCity", Convert.ToString(row["BILL_TO_CITY"]) ?? string.Empty);
                command.Parameters.AddWithValue("@BillToState", Convert.ToString(row["BILL_TO_STATE"]) ?? string.Empty);
                command.Parameters.AddWithValue("@BillToPostalCode", Convert.ToString(row["BILL_TO_POSTAL_CODE"]) ?? string.Empty);
                command.Parameters.AddWithValue("@BillToCountry", Convert.ToString(row["BILL_TO_COUNTRY"]) ?? string.Empty);

                command.Parameters.AddWithValue("@ShipToPartyId", Convert.ToString(row["SHIP_TO_PARTY_ID"]) == "" ? 0 : Convert.ToInt64(row["SHIP_TO_PARTY_ID"]));
                command.Parameters.AddWithValue("@ShipToPartySiteId", Convert.ToString(row["SHIP_TO_PARTY_SITE_ID"]) == "" ? 0 : Convert.ToInt64(row["SHIP_TO_PARTY_SITE_ID"]));
                command.Parameters.AddWithValue("@ShipToCustomerName", Convert.ToString(row["SHIP_TO_CUSTOMER_NAME"]) ?? string.Empty);
                command.Parameters.AddWithValue("@ShipCustSiteName", Convert.ToString(row["SHIP_CUST_SITE_NAME"]) ?? string.Empty);
                command.Parameters.AddWithValue("@ShipToLocationId", Convert.ToString(row["SHIP_TO_LOCATION_ID"]) == "" ? 0 : Convert.ToInt64(row["SHIP_TO_LOCATION_ID"]));
                command.Parameters.AddWithValue("@ShipToAddress1", Convert.ToString(row["SHIP_TO_ADDRESS1"]) ?? string.Empty);
                command.Parameters.AddWithValue("@ShipToAddress2", Convert.ToString(row["SHIP_TO_ADDRESS2"]) ?? string.Empty);
                command.Parameters.AddWithValue("@ShipToAddress3", Convert.ToString(row["SHIP_TO_ADDRESS3"]) ?? string.Empty);
                command.Parameters.AddWithValue("@ShipToAddress4", Convert.ToString(row["SHIP_TO_ADDRESS4"]) ?? string.Empty);
                command.Parameters.AddWithValue("@ShipToCity", Convert.ToString(row["SHIP_TO_CITY"]) ?? string.Empty);
                command.Parameters.AddWithValue("@ShipToState", Convert.ToString(row["SHIP_TO_STATE"]) ?? string.Empty);
                command.Parameters.AddWithValue("@ShipToPostalCode", Convert.ToString(row["SHIP_TO_POSTAL_CODE"]) ?? string.Empty);
                command.Parameters.AddWithValue("@ShipToCountry", Convert.ToString(row["SHIP_TO_COUNTRY"]) ?? string.Empty);

                command.Parameters.AddWithValue("@TrxNumber", Convert.ToInt32(row["TRX_NUMBER"]));
                command.Parameters.AddWithValue("@TrxDate", Convert.ToDateTime(row["TRX_DATE"]));
                command.Parameters.AddWithValue("@TermName", Convert.ToString(row["TERM_NAME"]));

                command.Parameters.AddWithValue("@ShipDateActual", (row["SHIP_DATE_ACTUAL"] == DBNull.Value || string.IsNullOrEmpty(row["SHIP_DATE_ACTUAL"].ToString())) ? DBNull.Value : Convert.ToDateTime(row["SHIP_DATE_ACTUAL"]));
                command.Parameters.AddWithValue("@SalesOrder", Convert.ToInt32(row["SALES_ORDER"]));
                command.Parameters.AddWithValue("@PrimarySalesRepName", Convert.ToString(row["PRIMARY_SALESREP_NAME"]));
                command.Parameters.AddWithValue("@ShipVia", Convert.ToString(row["SHIP_VIA1"]));
                command.Parameters.AddWithValue("@PurchaseOrderNumber", Convert.ToString(row["PURCHASE_ORDER_NUMBER"]));
                command.Parameters.AddWithValue("@BillToCustomerNumber", Convert.ToString(row["BILL_TO_CUSTOMER_NUMBER"]));
                command.Parameters.AddWithValue("@InternalNotes", Convert.ToString(row["INTERNAL_NOTES"]));
                command.Parameters.AddWithValue("@TaxAmount", Convert.ToDecimal(row["TAX_AMOUNT"] ?? (object)DBNull.Value));
                command.Parameters.AddWithValue("@FreightAmount", Convert.ToDecimal(row["FREIGHT_AMOUNT"] ?? (object)DBNull.Value));
                command.Parameters.AddWithValue("@TotalAmount", Convert.ToDecimal(row["TOTAL_AMOUNT"] ?? (object)DBNull.Value));
                command.Parameters.AddWithValue("@DiscountTakenEarned", Convert.ToDecimal(row["DISCOUNT_TAKEN_EARNED"] ?? (object)DBNull.Value));
                command.Parameters.AddWithValue("@AmountApplied", Convert.ToDecimal(row["AMOUNT_APPLIED"] ?? (object)DBNull.Value));
                command.Parameters.AddWithValue("@AmountDueRemaining", Convert.ToDecimal(row["AMOUNT_DUE_REMAINING"] ?? (object)DBNull.Value));
                command.Parameters.AddWithValue("@Status", Convert.ToString(row["STATUS"]) ?? string.Empty);
                command.Parameters.AddWithValue("@DueDate", Convert.ToDateTime(row["DUE_DATE"]));
                command.Parameters.AddWithValue("@CfFromDate", Convert.ToDateTime(row["CF_FROM_DATE"]));
                command.Parameters.AddWithValue("@CfToDate", Convert.ToDateTime(row["CF_TO_DATE"]));
                command.Parameters.AddWithValue("@TrxType", Convert.ToString(row["TRX_TYPE"]) ?? string.Empty);
                command.Parameters.AddWithValue("@TotalNet", Convert.ToDecimal(row["LINE_AMOUNT"] ?? (object)DBNull.Value));
                command.Parameters.AddWithValue("@CreatedDate", Convert.ToDateTime(row["TRX_DATE"]));

                var outputIdParam = new SqlParameter("@Id", SqlDbType.Int) { Direction = ParameterDirection.Output };
                command.Parameters.Add(outputIdParam);

                await command.ExecuteNonQueryAsync();
                Id = (int)outputIdParam.Value;
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error in InsertInvoiceData = {ex.Message.ToString()}");
            }

            return Id; // Return the newly created ID
        }

        private async Task InsertInvoiceLineItems(SqlConnection connection, int invoiceId, DataTable lineItems)
        {
            var query = "INSERT INTO InvoiceLineItem (InvoiceID, LineNumber, ItemNumber,CfPacking, LineDescription, UnitOfMeasureName, Quantity, UnitPrice, ExtendedAmount) " +
                         "VALUES (@InvoiceID, @LineNumber, @ItemNumber,@CfPacking, @LineDescription, @UnitOfMeasureName, @Quantity, @UnitPrice, @ExtendedAmount)";

            //await connection.OpenAsync();
            try
            {
                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    // Add parameters to avoid SQL injection
                    command.Parameters.Clear();
                    command.Parameters.Add("@InvoiceID", SqlDbType.Int);
                    command.Parameters.Add("@LineNumber", SqlDbType.Int);
                    command.Parameters.Add("@ItemNumber", SqlDbType.VarChar, 100);
                    command.Parameters.Add("@CfPacking", SqlDbType.VarChar, 50);
                    command.Parameters.Add("@LineDescription", SqlDbType.VarChar, 255);
                    command.Parameters.Add("@UnitOfMeasureName", SqlDbType.VarChar, 100);
                    command.Parameters.Add("@Quantity", SqlDbType.Decimal).Precision = 18;
                    command.Parameters["@Quantity"].Scale = 2;
                    command.Parameters.Add("@UnitPrice", SqlDbType.Decimal).Precision = 18;
                    command.Parameters["@UnitPrice"].Scale = 2;
                    command.Parameters.Add("@ExtendedAmount", SqlDbType.Decimal).Precision = 18;
                    command.Parameters["@ExtendedAmount"].Scale = 2;
                    //command.Parameters.Add("@LineAmount", SqlDbType.Decimal).Precision = 18;
                    //command.Parameters["@LineAmount"].Scale = 2;
                    foreach (DataRow row in lineItems.Rows)
                    {
                        command.Parameters["@InvoiceID"].Value = invoiceId;
                        command.Parameters["@LineNumber"].Value = Convert.ToInt32(row["LINE_NUMBER"]);
                        command.Parameters["@ItemNumber"].Value = Convert.ToString(row["ITEM_NUMBER"]);
                        command.Parameters["@CfPacking"].Value = Convert.ToString(row["CF_PACKING"]);
                        command.Parameters["@LineDescription"].Value = Convert.ToString(row["LINE_DESCRIPTION"]);
                        command.Parameters["@UnitOfMeasureName"].Value = Convert.ToString(row["UNIT_OF_MEASURE_NAME"]);
                        command.Parameters["@Quantity"].Value = Convert.ToDecimal(row["QUANTITY"]);
                        command.Parameters["@UnitPrice"].Value = Convert.ToDecimal(row["UNIT_PRICE"]);
                        command.Parameters["@ExtendedAmount"].Value = Convert.ToDecimal(row["EXTENDED_AMOUNT"]);
                        //command.Parameters["@LineAmount"].Value = Convert.ToDecimal(row["LINE_AMOUNT"]);

                        await command.ExecuteNonQueryAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error InsertInvoiceLineItems ex={ex.Message.ToString()} ");
            }
        }

        private async Task UpdateInvoiceStatus(SqlConnection connection, int invoiceId, DataRow row)
        {

            var query = @"update Invoice set 
                          Status=@invoiceStatus
                        , TaxAmount=@TaxAmount
                        , FreightAmount=@FreightAmount
                        , TotalAmount=@TotalAmount
                        , DiscountTakenEarned=@DiscountTakenEarned
                        , AmountApplied=@AmountApplied
                        , AmountDueRemaining=@AmountDueRemaining 
                         where Id=@InvoiceID; 
                   ";
            try
            {
                using var command = new SqlCommand(query, connection);
                command.Parameters.AddWithValue("@InvoiceID", invoiceId);
                command.Parameters.AddWithValue("@invoiceStatus", Convert.ToString(row["STATUS"]));
                command.Parameters.AddWithValue("@TaxAmount", Convert.ToDecimal(row["TAX_AMOUNT"]));
                command.Parameters.AddWithValue("@FreightAmount", Convert.ToDecimal(row["FREIGHT_AMOUNT"]));
                command.Parameters.AddWithValue("@TotalAmount", Convert.ToDecimal(row["TOTAL_AMOUNT"]));
                command.Parameters.AddWithValue("@DiscountTakenEarned", Convert.ToDecimal(row["DISCOUNT_TAKEN_EARNED"]));
                command.Parameters.AddWithValue("@AmountApplied", Convert.ToDecimal(row["AMOUNT_APPLIED"]));
                command.Parameters.AddWithValue("@AmountDueRemaining", Convert.ToDecimal(row["AMOUNT_DUE_REMAINING"]));
                command.Parameters.AddWithValue("@TrxNumber", Convert.ToInt32(row["TRX_NUMBER"]));
                await command.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError($"UpdateInvoiceStatus error ={ex.Message.ToString()}");
            }
        }

        private async Task UpsertInvoiceLineItems(SqlConnection connection, int? invoiceId, DataTable lineItems)
        {
            var query = @"
                                IF EXISTS (SELECT 1 FROM InvoiceLineItem WHERE LineNumber = @LineNumber AND InvoiceId = @InvoiceID) 
                                BEGIN
                                    UPDATE InvoiceLineItem 
                                    SET 
                                        ItemNumber = @ItemNumber, 
                                        LineDescription = @LineDescription, 
                                        UnitOfMeasureName = @UnitOfMeasureName, 
                                        Quantity = @Quantity, 
                                        UnitPrice = @UnitPrice, 
                                        ExtendedAmount = @ExtendedAmount
                                    WHERE LineNumber = @LineNumber AND InvoiceID = @InvoiceID
                                END
                                ELSE 
                                BEGIN
                                    INSERT INTO InvoiceLineItem (InvoiceID, LineNumber, ItemNumber, LineDescription, UnitOfMeasureName, Quantity, UnitPrice, ExtendedAmount) 
                                    VALUES ( @InvoiceID, @LineNumber, @ItemNumber, @LineDescription, @UnitOfMeasureName, @Quantity, @UnitPrice, @ExtendedAmount)
                                END";
            //await connection.OpenAsync();
            using var command = new SqlCommand(query, connection);
            command.Parameters.Clear();
            command.Parameters.Add("@InvoiceID", SqlDbType.Int);
            command.Parameters.Add("@LineNumber", SqlDbType.Int);
            command.Parameters.Add("@ItemNumber", SqlDbType.VarChar, 100);
            command.Parameters.Add("@CfPacking", SqlDbType.VarChar, 50);
            command.Parameters.Add("@LineDescription", SqlDbType.VarChar, 255);
            command.Parameters.Add("@UnitOfMeasureName", SqlDbType.VarChar, 100);
            command.Parameters.Add("@Quantity", SqlDbType.Decimal).Precision = 18;
            command.Parameters["@Quantity"].Scale = 2;
            command.Parameters.Add("@UnitPrice", SqlDbType.Decimal).Precision = 18;
            command.Parameters["@UnitPrice"].Scale = 2;
            command.Parameters.Add("@ExtendedAmount", SqlDbType.Decimal).Precision = 18;
            command.Parameters["@ExtendedAmount"].Scale = 2;
            //command.Parameters.Add("@LineAmount", SqlDbType.Decimal).Precision = 18;
            //command.Parameters["@LineAmount"].Scale = 2;


            try
            {
                foreach (DataRow row in lineItems.Rows)
                {
                    command.Parameters["@InvoiceID"].Value = invoiceId;
                    command.Parameters["@LineNumber"].Value = Convert.ToInt32(row["LINE_NUMBER"]);
                    command.Parameters["@ItemNumber"].Value = Convert.ToString(row["ITEM_NUMBER"]);
                    command.Parameters["@CfPacking"].Value = Convert.ToString(row["CF_PACKING"]);
                    command.Parameters["@LineDescription"].Value = Convert.ToString(row["LINE_DESCRIPTION"]);
                    command.Parameters["@UnitOfMeasureName"].Value = Convert.ToString(row["UNIT_OF_MEASURE_NAME"]);
                    command.Parameters["@Quantity"].Value = Convert.ToDecimal(row["QUANTITY"]);
                    command.Parameters["@UnitPrice"].Value = Convert.ToDecimal(row["UNIT_PRICE"]);
                    command.Parameters["@ExtendedAmount"].Value = Convert.ToDecimal(row["EXTENDED_AMOUNT"]);
                    //command.Parameters["@LineAmount"].Value = Convert.ToDecimal(row["LINE_AMOUNT"]);
                    await command.ExecuteNonQueryAsync();
                }
            }
            catch (Exception ex)
            {

                _logger.LogError($"Error in UpsertInvoiceLineItems = {ex.Message.ToString()}");
            }
        }

        private async Task<int> InvoiceExists(SqlConnection connection, int? trxNumber)
        {
            int invoiceId = 0;
            string query = "SELECT Id FROM Invoice(NOLOCK) WHERE TrxNumber = @TrxNumber";
            using (var command = new SqlCommand(query, connection))
            {
                command.Parameters.AddWithValue("@TrxNumber", trxNumber.Value);
                var result = await command.ExecuteScalarAsync();
                if (result != null)
                {
                    invoiceId = Convert.ToInt32(result);
                }
            }
            return invoiceId;
        }

        private async Task<DateTime> GetLastRunDatetimeAsync()
        {
            string query = "SELECT LastRun FROM SCHEDULERLASTRUNTIME (NOLOCK) WHERE schedulerName='Invoice'";

            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                await connection.OpenAsync();

                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    var result = await command.ExecuteScalarAsync();
                    return Convert.ToDateTime(result);
                }
            }
        }

        private async Task<int> UpdateLastRunDatetime(DateTime currentDatetime)
        {
            string query = "Update SchedulerLastRunTime set LastRun=@lastRun where  schedulerName='Invoice'"; // Example query

            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                await connection.OpenAsync();

                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    command.Parameters.AddWithValue("@lastRun", currentDatetime);
                    // Execute the update command asynchronously and return the number of rows affected
                    int rowsAffected = await command.ExecuteNonQueryAsync();
                    return rowsAffected;
                }
            }
        }
    }
}