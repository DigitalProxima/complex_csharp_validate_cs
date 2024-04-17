using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using iNETConnector;
using System.Globalization;
using System.Diagnostics;
using System.Linq;

namespace SAP_Validation
{
    public class ValidateData
    {
        #region PROPERTIES AND DECLARATIONS        
        private const bool csDebugMode = false;

        SAPConnection SAPConn = null;
        DataTable MapperData = null;
        public DataTable dtCoreData = null;         // Built from MapperData. Contains only required data for validations in row form.
        public int XLRow;
        private int XLRowLast = 0;

        public List<FieldInfo> FieldList = new List<FieldInfo>();
        Dictionary<string, DataTable> SAPCacheTables = new Dictionary<string, DataTable>();
        Dictionary<int, string> LineItemMessages = new Dictionary<int, string>();

        bool IsAccountBlockFound = false;
        bool IsPostKeyBlockFound = false;
        bool IsControllingAreaFound = false;
        bool IsCustomerFound = false;
        bool IsVendorFound = false;
        bool IsProfitCenterFound = false;
        bool IsCostElementFound = false;
        bool IsCostCenterFound = false;
        int _ACSLimit = 38000;

        // Cache name constants
        private const string T_COMPANYCODE = "COMPANY_CODE";
        private const string T_CURRENCY = "CURRENCY";
        private const string T_DOCTYPE = "DOCTYPE";
        private const string T_CONTROLAREA = "CONTROL_AREA";
        private const string T_POSTINGKEY = "POSTING_KEY";
        private const string T_ACCOUNT = "ACCOUNT";
        private const string T_COSTELEM = "COST_ELEMENT";
        private const string T_COSTCNTR = "COST_CENTER";
        private const string T_PROFITCNTR = "PROFIT_CENTER";
        private const string T_CUSTOMER = "CUSTOMER";
        private const string T_VENDOR = "VENDOR";
        private const string T_NONE = "NONE";
        private const string T_MSGDETAIL = "MSG_DETAIL";
        private const string F_VALIDDATA = "ValidData";

        const string csHEADER = "HDV";
        const string csDETAIL = "LDV";
        //End

        const string csE = "E:";
        const string csW = "W:";

        // Status return code values
        public enum DataLevel { HEADER, DETAIL }
        public enum ReturnStatus { INITIAL, VALID, INVALID, NODATA, ERROR }
        private DataTable MsgTextDetail = new DataTable();
        private List<string> ListMsgID = new List<string>();

        Dictionary<int, string> HeaderMessages = new Dictionary<int, string>();
        Dictionary<int, string> LineMessages = new Dictionary<int, string>();

        // Hold current BUKRS and KTOPL and other temp values.
        // Used for several data extracts and validations
        private string currBUKRS = string.Empty;
        private string currKTOPL = string.Empty;
        private string currBUDAT = string.Empty;

        public enum JoinType
        {
            Inner = 0,
            Left = 1
        }

        public enum TCode
        {
            FB01,
            FBV1
        }

        TCode TranCode;

        public Dictionary<int, LineLevelDetail> Messages
        {
            get; internal set;
        }

        public ValidateData(SAPConnection sapConn)
        {
            SAPConn = sapConn;
        }

        public bool IsValidationFail
        {
            get; internal set;
        }

        public string HeadervalidationStatus
        {
            get; internal set;
        }

        public string HeadervalidationMessage
        {
            get; internal set;
        }

        public class LineLevelDetail
        {
            public string ValidationStatus { get; set; }
            public string ValidationMessage { get; set; }
            public bool IsHeader { get; set; }
            public int XLRow { get; set; }      //Crew Reynolds    PRUN-1790   02/05/21    Added XLRow where message needs to be placed.
        }

        public class FieldInfo
        {
            internal string FieldName { private set; get; }
            internal TCode[] TCode { private set; get; }
            internal string FieldStatus { private set; get; }

            public FieldInfo(string fieldName, string fieldStatus, TCode[] tCode)
            {
                FieldName = fieldName;
                FieldStatus = fieldStatus;
                TCode = tCode;
            }
        }
        #endregion

        #region INTERNAL CLASSES

        internal class ValidationRequest
        {
            //Crew Reynolds     PRUN-1790   01/12/21    Created VaidationRequest class for passing data to and from the ValidateField() method.
            //Crew Reynolds     PRUN-1790   02/04/21    ValidationRequest class modified to use a contructor for property initilization per Anand/Jigar - PR standard 
            //Crew Reynolds     PRUN-1790   02/26/21    Added to override the ValidateField()'s errors so that the caller can determine the error message

            internal ValidationRequest(int xlRow, DataLevel fieldLevel, string fieldName, string fieldValue, string fieldDescription, string extractKey, bool checkRequired, bool checkValue, bool reportErrors)
            {
                //Crew Reynolds PRUN-1790  02/04/21    ValidationRequest class modified to use a contructor for property initilization per Anand/Jigar - PR standard 

                // Set the internal properties from the constructor params
                Row = xlRow;
                FieldLevel = fieldLevel;
                FieldName = fieldName.Trim();
                FieldValue = fieldValue.Trim();
                FieldDescription = fieldDescription.Trim();
                ExtractKey = extractKey;
                CheckRequired = checkRequired;
                CheckValue = checkValue;
                ReportErrors = reportErrors;                // Crew Reynolds    PRUN-1790   02/26/21    Added to override the ValidateField()'s errors so that the caller can determine the error message
                FieldStatus = ReturnStatus.INITIAL;

                //Crew Reynolds PRUN-1760 02/09/21 Mod to use use global constants
                if (fieldLevel == DataLevel.HEADER)
                    StatusCode = csHEADER;
                else
                    StatusCode = csDETAIL;
            }

            // Initialized by contructor
            public int Row { get; }
            public DataLevel FieldLevel { get; }
            public string FieldName { get; }
            public string FieldValue { get; set; }
            public string ExtractKey { get; }
            public bool CheckRequired { get; }
            public bool CheckValue { get; }
            public bool ReportErrors { get; }
            public string StatusCode { get; }

            // Retun properties from validation methods
            public string FieldDescription { get; set; }
            public ReturnStatus FieldStatus { get; set; }
        }

        internal class DataRequest
        {
            //Crew Reynolds PRUN-1790   01/12/21    Created DataRequest class for passing dat to and from the data extract classes
            //Crew Reynolds PRUN-1790   02/04/21    DataRequest class modified to use a contructor for property initilization Anand/Jigar - PR standard 
            internal DataRequest(string fieldName, string fieldValue, string extractKey)
            {
                //Crew Reynolds PRUN-1790   02/04/21    DataRequest class modified to use a contructor for property initilization Anand/Jigar - PR standard 

                // Set the internal properties from the constructor params
                FieldName = fieldName;
                FieldValue = fieldValue;
                ExtractKey = extractKey;
                FieldStatus = ReturnStatus.INITIAL;
            }
            public string FieldName { get; }
            public string FieldValue { get; }
            public string ExtractKey { get; }
            public ReturnStatus FieldStatus { get; set; }
        }
        #endregion

        #region VALIDATION METHODS
        public void Validate(DataTable mapperData, int xlRow, string strTCode)
        {
            // TEMP LICENSE OVERRIDE NOTES
            // Break on LicenseInfo.cs  "LicenseInfo.SetLicenseInfo();"
            // Run these two stements in the immediate window:
            // License.Status.KeyValueList["User"]="MyUser"
            // License.Status.KeyValueList["COMPANY"] = "MyCompany"
            // Continue

            IsAccountBlockFound = true;
            IsPostKeyBlockFound = true;
            IsControllingAreaFound = true;
            IsCustomerFound = true;
            IsVendorFound = true;
            IsProfitCenterFound = true;
            IsCostElementFound = true;
            IsCostCenterFound = true;

            XLRow = xlRow;

            // Crew Reynolds    PRUN-1790   02/26/21    Clear out any global variables before each Validate() call from TXRunner
            currBUKRS = string.Empty;
            currKTOPL = string.Empty;
            currBUDAT = string.Empty;
            dtCoreData = null;

            TranCode = (TCode)Enum.Parse(typeof(TCode), strTCode);
            DefaultMsgList();

            DataView dvXLRow = null;
            FieldList = GetProcessFileFields();

            if (false)   // Set to true for data extract tests
            {
                // Debug mode redirect
                DataExtractTests();
            }
            else
            {
                // Production: Execute all data validations
                try
                {
                    IsValidationFail = false;
                    Messages = new Dictionary<int, LineLevelDetail>();
                    MapperData = mapperData;

                    dtCoreData = BuildCoreData(mapperData, xlRow);      //Crew Reynolds pull out the useful field data out of mapperData for validation

                    // Validate all header level fields
                    if (!ValidateHeaderLevelFields(xlRow))
                    {
                        // Hard error. Likely no BUKRS. Exit.
                        MapperData.DefaultView.RowFilter = string.Empty;
                        return;
                    }

                    // Check the document balance   
                    bool docBalance = ValidateDocumentBalance(dtCoreData);

                    // Validate all line level fields, 1 XLROW at a time
                    //for (int i = xlRow; i < XLRowLast; i++) 
                    dvXLRow = new DataView(dtCoreData);
                    dvXLRow.RowFilter = "LEVEL = 'L'";
                    foreach (DataRow drValue in dvXLRow.ToTable(true, "XLROW").Rows)
                    {
                        if (Convert.ToInt32(drValue["XLROW"]) != 0)
                        {
                            ValidateLineLevelFields(Convert.ToInt32(drValue["XLROW"]));
                        }
                    }
                    dvXLRow = null;
                }
                catch (Exception ex)
                {
                    throw new Exception("Error while validating data: " + ex.Message);
                }
                finally
                {
                    MapperData.DefaultView.RowFilter = string.Empty;
                }
            }
        }
        private DataTable BuildCoreData(DataTable md, int xlRow)
        {
            // Build DataTable CoreData to hold just the data fields and values needed for validation

            // CHANGE LOG
            //Crew Reynolds    PRUN-1790   01/28/21    Created BuildCoreData to produce a validation friendly version of MapperData.

            string lineCol = string.Empty; ;
            DataTable dt = new DataTable();
            DataRow dtMapRow;
            int xlRowIndex = xlRow;
            int breakColCount = 0;
            string lineBreakCol = string.Empty;
            string fnBuild = string.Empty;
            int fnDelimiter = 0;
            int xlRowLast = 0;

            try
            {
                // Create DataTable to hold core data for validation
                //dt.Clear();
                dt.Columns.Add("INDEX", System.Type.GetType("System.Int32"));
                dt.Columns.Add("XLROW", System.Type.GetType("System.Int32"));
                dt.Columns.Add("XLCOL", System.Type.GetType("System.String"));
                dt.Columns.Add("LEVEL", System.Type.GetType("System.String"));
                dt.Columns.Add("FNAM", System.Type.GetType("System.String"));
                dt.Columns.Add("DESCR", System.Type.GetType("System.String"));
                dt.Columns.Add("FVAL", System.Type.GetType("System.String"));
                dt.Columns.Add("TYPE", System.Type.GetType("System.String"));
                dt.Columns.Add("LENGTH", System.Type.GetType("System.String"));

   
                // Find the first line level entry in MapperData. The first line level row's Excel column is the break column.

                // Transfer the MapperData to CoreData such that the header and line rows are properly marked
                XLRowLast = xlRow;
                md.DefaultView.RowFilter = TableDefaults.F_SRCVALUETYPE + " = '2' OR " + TableDefaults.F_SRCVALUETYPE + " = '7'";
                for (int i = 0; i < md.DefaultView.Count; i++)
                {
                    //Crew Reynolds PRUN-1790   02/10/21    Modified where dtRow comes from
                    dtMapRow = md.DefaultView[i].Row;
                    DataRow dtRow = dt.NewRow();
                    //dtRow = md.DefaultView[i].Row;

                    // Get the name of the field removing the source portion. ex. BKPF-BLART =  BLART
                    fnBuild = dtMapRow[TableDefaults.F_FNAM].ToString();
                    fnDelimiter = 0; fnDelimiter = fnBuild.IndexOf('-');
                    fnBuild = fnBuild.Substring(fnDelimiter + 1);

                    var FieldExist = FieldList.AsEnumerable().Where(x => x.FieldName == fnBuild).ToList();
                    if (FieldExist.Count > 0)
                        dtRow["LEVEL"] = Convert.ToString(FieldExist[0].FieldStatus);
                    else
                        continue; //If Field Not Found in Dictionary then Skip Field
                    //End

                    dtRow["INDEX"] = i;

                    if (Convert.ToString(md.DefaultView[i][TableDefaults.F_TMPFIELD]) == string.Empty)
                        dtRow["XLROW"] = XLRow;
                    else
                        dtRow["XLROW"] = Convert.ToInt32(md.DefaultView[i][TableDefaults.F_OVAL]);
                    //dtRow["XLROW"] = xlRowIndex;
                    //End

                    dtRow["XLCOL"] = md.DefaultView[i][TableDefaults.F_FIELDVALUE];
                    dtRow["FNAM"] = fnBuild;
                    dtRow["DESCR"] = md.DefaultView[i][TableDefaults.F_DESCRIPTION];
                    dtRow["FVAL"] = md.DefaultView[i][TableDefaults.F_FVAL];
                    dtRow["TYPE"] = md.DefaultView[i][TableDefaults.F_FIELDTYPE];
                    dtRow["LENGTH"] = md.DefaultView[i][TableDefaults.F_FIELDLEN];

                    dt.Rows.Add(dtRow);
                    dtRow = null;
                }
            }
            catch (Exception ex)
            {
                dt = null;
            }

            xlRowLast = xlRowIndex;
            return dt;
        }
        private List<FieldInfo> GetProcessFileFields()
        {
            List<FieldInfo> fieldInfo = new List<FieldInfo>();

            fieldInfo.Add(new FieldInfo("BLDAT", "H", new TCode[] { TCode.FB01, TCode.FBV1 })); //Document Date in Document
            fieldInfo.Add(new FieldInfo("BUDAT", "H", new TCode[] { TCode.FB01, TCode.FBV1 })); //Posting Date in the Document
            fieldInfo.Add(new FieldInfo("BUKRS", "H", new TCode[] { TCode.FB01, TCode.FBV1 })); //Company Code
            fieldInfo.Add(new FieldInfo("WAERS", "H", new TCode[] { TCode.FB01, TCode.FBV1 })); //Currency Key
            fieldInfo.Add(new FieldInfo("BLART", "H", new TCode[] { TCode.FB01, TCode.FBV1 })); //Document Type
            fieldInfo.Add(new FieldInfo("BKTXT", "H", new TCode[] { TCode.FB01, TCode.FBV1 })); //Document Header Text
            fieldInfo.Add(new FieldInfo("XBLNR", "H", new TCode[] { TCode.FB01, TCode.FBV1 })); //Reference Document Number
            fieldInfo.Add(new FieldInfo("MONAT", "H", new TCode[] { TCode.FB01, TCode.FBV1 })); //Fiscal Period
            fieldInfo.Add(new FieldInfo("XBWAE", "H", new TCode[] { TCode.FB01, TCode.FBV1 }));
            fieldInfo.Add(new FieldInfo("STGRD", "H", new TCode[] { TCode.FB01, TCode.FBV1 })); //Reversal Date
            fieldInfo.Add(new FieldInfo("STODT", "H", new TCode[] { TCode.FB01, TCode.FBV1 })); //Reversal Reason

            fieldInfo.Add(new FieldInfo("NEWBS", "L", new TCode[] { TCode.FB01, TCode.FBV1 })); //Posting Key for the Next Line Item
            fieldInfo.Add(new FieldInfo("MWSKZ", "L", new TCode[] { TCode.FB01, TCode.FBV1 })); //Sales Tax Code
            fieldInfo.Add(new FieldInfo("KOSTL", "L", new TCode[] { TCode.FB01, TCode.FBV1 })); //Cost Center
            fieldInfo.Add(new FieldInfo("NEWKO", "L", new TCode[] { TCode.FB01, TCode.FBV1 })); //Account or Matchcode for the Next Line Item
            fieldInfo.Add(new FieldInfo("WRBTR", "L", new TCode[] { TCode.FB01, TCode.FBV1 })); //Amount in document currency
            fieldInfo.Add(new FieldInfo("SGTXT", "L", new TCode[] { TCode.FB01, TCode.FBV1 })); //Item Text
            fieldInfo.Add(new FieldInfo("MENGE", "L", new TCode[] { TCode.FB01, TCode.FBV1 })); //Quantity
            fieldInfo.Add(new FieldInfo("MEINS", "L", new TCode[] { TCode.FB01, TCode.FBV1 })); //Base Unit of Measure
            fieldInfo.Add(new FieldInfo("NEWBK", "L", new TCode[] { TCode.FB01, TCode.FBV1 })); //(cross-company number)
            fieldInfo.Add(new FieldInfo("DOC_ITEM", "L", new TCode[] { TCode.FB01, TCode.FBV1 })); //DOC_ITEM
            fieldInfo.Add(new FieldInfo("KSTAR", "L", new TCode[] { TCode.FB01, TCode.FBV1 })); //Cost Center Elements
            fieldInfo.Add(new FieldInfo("PRCTR", "L", new TCode[] { TCode.FB01, TCode.FBV1 })); //Profit Center
            fieldInfo.Add(new FieldInfo("KUNNR", "L", new TCode[] { TCode.FB01, TCode.FBV1 })); //Customer
            fieldInfo.Add(new FieldInfo("LIFNR", "L", new TCode[] { TCode.FB01, TCode.FBV1 })); //Venddor
            fieldInfo.Add(new FieldInfo("WMWST", "L", new TCode[] { TCode.FB01, TCode.FBV1 })); //WMWST
            fieldInfo.Add(new FieldInfo("RKE", "L", new TCode[] { TCode.FB01, TCode.FBV1 })); //RKE

            return fieldInfo = fieldInfo.AsEnumerable().Where(x => x.TCode.Contains(TranCode)).ToList();
        }
        private bool ValidateDocumentBalance(DataTable cd)
        {
            // Build DataTable CoreDataSheet to hold just the data fields and values needed for validation in sheet format (row/col)

            // CHANGE LOG
            // Crew Reynolds    PRUN-1790   02/18/21    Added
            // TODO - revisit this logic during Stage 2 to ensure that that there are no other rules to apply

            DataTable dt = new DataTable();
            DataRow dtRow = null;
            DataRow dtNewRow = null;
            Double docBalance = 0;
            bool balanceError = false;
            int rowCount = 0;
            DataTable dtPostKey = new DataTable();

            try
            {
                // Build the DataTable rows and columns
                dt.Columns.Add("XLROW", System.Type.GetType("System.String"));
                dt.Columns.Add("NEWBS", System.Type.GetType("System.String"));
                dt.Columns.Add("SHKZG", System.Type.GetType("System.String"));
                dt.Columns.Add("WRBTR", System.Type.GetType("System.Double"));
                dt.Columns.Add("WMWST", System.Type.GetType("System.Double"));
                int lastIndex = 0;

                cd.DefaultView.RowFilter = "LEVEL = 'L' AND FNAM IN ( 'NEWBS', 'WRBTR', 'WMWST' )";
                rowCount = cd.DefaultView.Count;

                if (rowCount > 0)
                {
                    if (IsPostKeyBlockFound)
                    {
                        dtPostKey = PostingKeyFetch();
                        IsPostKeyBlockFound = false;
                    }
                }

                int NEWBSCount = 0;

                for (int i = 0; i < rowCount; i++)
                {
                    dtRow = cd.DefaultView[i].Row;
                    int currXLROW = Convert.ToInt32(dtRow["XLROW"]);

                    if (i == 0)
                    {
                        dtNewRow = dt.NewRow();
                        lastIndex = currXLROW;
                        dtNewRow["XLROW"] = dtRow["XLROW"];
                    }

                    if (Convert.ToString(dtRow["FNAM"]) == "NEWBS")
                        NEWBSCount++;

                    if (currXLROW > lastIndex || NEWBSCount > 1)
                    {
                        NEWBSCount = 1;
                        lastIndex = currXLROW;
                        dt.Rows.Add(dtNewRow);
                        dtNewRow = dt.NewRow();
                        dtNewRow["XLROW"] = dtRow["XLROW"];
                    }

                    try
                    {
                        switch (dtRow["FNAM"])
                        {
                            case "NEWBS":
                                {
                                    dtNewRow["NEWBS"] = dtRow["FVAL"];
                                    var drBSCHL = dtPostKey.AsEnumerable().Where(dRow => dRow.Field<string>("BSCHL") == Convert.ToString(dtRow["FVAL"])).FirstOrDefault();
                                    if (drBSCHL != null) dtNewRow["SHKZG"] = drBSCHL["SHKZG"].ToString();
                                    break;
                                }
                            case "WRBTR":
                                {
                                    dtNewRow["WRBTR"] = dtRow["FVAL"];
                                    break;
                                }
                            case "WMWST":
                                {
                                    dtNewRow["WMWST"] = dtRow["FVAL"];
                                    break;
                                }
                        }
                    }
                    catch { }
                }

                if (dtNewRow != null) dt.Rows.Add(dtNewRow);

                // Calculate the document balance
                // dt.DefaultView.RowFilter = "";

                foreach (DataRow row in dt.Rows)
                {
                    double lineAmount = 0.00;
                    double taxAmount = 0.00;

                    Double.TryParse(row["WRBTR"].ToString(), out lineAmount);
                    Double.TryParse(row["WMWST"].ToString(), out taxAmount);

                    // S = Debit, H = Credit
                    switch (row["SHKZG"])
                    {
                        // Crew Reynolds    PRUN-1790   02/03/21    Added support for Tax Amount (WMWST) in balance calculation
                        case "H":
                            {
                                docBalance = Math.Round(docBalance + lineAmount + taxAmount, 2);
                                break;
                            }
                        case "S":
                            {
                                docBalance = Math.Round(docBalance - lineAmount - taxAmount, 2);
                                break;
                            }
                        default:
                            {
                                balanceError = true;
                                break;
                            }
                    }
                }
            }
            catch (Exception ex)
            {
                balanceError = true;
            }
            finally
            {
                dt = null;
                dtRow = null;
                dtNewRow = null;
                dtPostKey = null;
            }

            // If there is no error then report the balance
            string validationStatus = string.Empty;
            string validationMessage = string.Empty;

            if (!balanceError)
            {
                if (docBalance > 0)
                {
                    validationStatus = csE + csHEADER;
                    validationMessage = "Document out of balance: cr " + Math.Abs(docBalance);
                    ErrorMessages(XLRow, validationStatus, validationMessage, true);
                }

                if (docBalance < 0)
                {
                    validationStatus = csE + csHEADER;
                    validationMessage = "Document out of balance: db " + Math.Abs(docBalance);
                    ErrorMessages(XLRow, validationStatus, validationMessage, true);
                }
            }

            return balanceError;
        }
        private bool ValidateHeaderLevelFields(int xlRow)
        {
            // Run through all of the header fields and validate

            // Change Log
            // Crew Reynolds    PRUN-1790   02/04/21    Implemented ValidationRequest initialization by construtor

            string validationStatus = string.Empty;
            string validationMessage = string.Empty;
            bool rc = true;
            DataTable dtExtract;
            DataRow dtRow;
            ValidationRequest vr;
            DataView dvCoreData;

            if (Options.OtherOption.ApplicationLog) LogManage.WriteAppLog("VD-" + csHEADER + "-01: Header level validation started.");

            try
            {
                // Pull just the header rows from dtCoreData
                //Crew Reynolds    PRUN-1790   02/09/21    Added dvCoreData / dtCopy for iteration instead of using dtCoreData
                dtCoreData.DefaultView.RowFilter = "LEVEL = 'H' AND XLROW = '" + xlRow + "'";
                dvCoreData = dtCoreData.DefaultView;
            }
            catch (Exception ex)
            {
                validationStatus = csE + csHEADER + "-R" + xlRow.ToString();
                validationMessage = "Error accessing header row: " + ex.Message;
                ErrorMessages(xlRow, validationStatus, validationMessage, true);
                return false;
            }

            // Check to nake sure we have user data and a non-blank BURKS field
            try
            {
                if (dtCoreData.Rows.Count == 0)
                {
                    // Unable to access user data
                    validationStatus = csE + csHEADER;
                    validationMessage = "Missing user data for validation.";
                    ErrorMessages(xlRow, validationStatus, validationMessage, true);

                    validationMessage = "Document validation cannot continue.";
                    ErrorMessages(xlRow, validationStatus, validationMessage, true);

                    dtCoreData.DefaultView.RowFilter = "";
                    //dtCopy = null;
                    dvCoreData = null;
                    return false;     // Hard stop
                }
                else
                {
                    // Crew Reynolds    PRUN-1790   02/23/21    Check for BUKRS first - required. Cannot continue if it is missing or invalid.
                    //dtCoreData.DefaultView.RowFilter = "LEVEL = 'H' AND FNAM = 'BUKRS'";
                    //if (dtCoreData.DefaultView.Count > 0 && dtCoreData.DefaultView[0].Row["FVAL"].ToString() == "")

                    DataView dataView = new DataView(dtCoreData);
                    dataView.RowFilter = "LEVEL = 'H' AND FNAM = 'BUKRS'";
                    if (dataView.Count > 0 && dataView[0].Row["FVAL"].ToString() == "")
                    {
                        // We MUST have a Company Code (BUKRS) in the header or we cannot proceed
                        validationStatus = csE + csHEADER;
                        validationMessage = "Company Code (BUKRS) is a required field.";
                        ErrorMessages(xlRow, validationStatus, validationMessage, true);

                        validationMessage = "Document validation cannot continue.";
                        ErrorMessages(xlRow, validationStatus, validationMessage, true);

                        dataView.RowFilter = "";
                        //dtCoreData.DefaultView.RowFilter = "";
                        //dtCopy = null;
                        dvCoreData = null;
                        return false;     // Hard stop
                    }
                    //dtCoreData.DefaultView.RowFilter = "";
                    dataView.RowFilter = "";

                    // Step through each dtCoreData header row and validate the field
                    for (int i = 0; i < dvCoreData.Count; i++)
                    {
                        dtRow = dvCoreData[i].Row; //Crew Reynolds PRUN-1790   02/09/21    Mofified to use dvCoreData
                        string fieldName = dtRow["FNAM"].ToString();
                        string fieldValue = dtRow["FVAL"].ToString();
                        string fieldDescription = dtRow["DESCR"].ToString();

                        switch (fieldName)
                        {
                            case "BUKRS": // Validate Company Code: BUKRS
                                {
                                    vr = new ValidationRequest(xlRow, DataLevel.HEADER, fieldName, fieldValue, fieldDescription, T_COMPANYCODE, true, true, true);
                                    dtExtract = ValidateField(vr);

                                    if (vr.FieldStatus == ReturnStatus.INVALID || dtExtract == null)
                                    {
                                        // We MUST have a Company Code (BUKRS) in the header or we cannot proceed
                                        validationStatus = csE + csHEADER + "-" + dtRow["XLCOL"].ToString() + xlRow.ToString();
                                        validationMessage = "Document validation cannot continue.";
                                        ErrorMessages(xlRow, validationStatus, validationMessage, true);    //Crew Reynolds PRUN-1760 02/09/21 Modified to use validationStatus var
                                        vr = null;

                                        // Crew Reynolds    PRUN-1790   03/08/21    Added code to clean currBUKRS and currKTOPL due to invalid data
                                        currBUKRS = string.Empty;
                                        currKTOPL = string.Empty;
                                        return false;     // Hard stop
                                    }
                                    else
                                    {
                                        // Good company code (BUKRS). We need to hold on to the KTOPL value for later validations.
                                        // Pull the header level Company data based on header BUKRS
                                        //                                        dtRow = dtCoreData.DefaultView[0].Row;
                                        // Crew Reynolds    PRUN-1790   03/08/20    Modified to correctly pull out the BUKRS ands KTOPL fields
                                        dtExtract.DefaultView.RowFilter = "BUKRS = '" + vr.FieldValue + "'";
                                        currBUKRS = dtExtract.DefaultView[0].Row["BUKRS"].ToString();
                                        currKTOPL = dtExtract.DefaultView[0].Row["KTOPL"].ToString();
                                    }
                                    vr = null;
                                    break;
                                }

                            case "BLART": // Validate Docment Type: BLART
                                {
                                    vr = new ValidationRequest(xlRow, DataLevel.HEADER, fieldName, fieldValue, fieldDescription, T_DOCTYPE, true, true, true);
                                    dtExtract = ValidateField(vr);
                                    vr = null;
                                    break;
                                }

                            case "WAERS": // Validate Currency: WAERS
                                {
                                    vr = new ValidationRequest(xlRow, DataLevel.HEADER, fieldName, fieldValue, fieldDescription, T_CURRENCY, true, true, true);
                                    dtExtract = ValidateField(vr);
                                    vr = null;
                                    break;
                                }

                            case "BLDAT": // Validate Document Date
                                {
                                    vr = new ValidationRequest(xlRow, DataLevel.HEADER, fieldName, fieldValue, fieldDescription, T_NONE, true, false, true);
                                    dtExtract = ValidateField(vr);
                                    vr = null;
                                    break;
                                }

                            case "BUDAT": // Validate Posting Date
                                {
                                    vr = new ValidationRequest(xlRow, DataLevel.HEADER, fieldName, fieldValue, fieldDescription, T_NONE, true, false, true);
                                    dtExtract = ValidateField(vr);

                                    currBUDAT = "";
                                    if (vr.FieldStatus == ReturnStatus.VALID) currBUDAT = fieldValue;

                                    vr = null;
                                    break;
                                }

                            case "XBWAE": // Only transfer document in document currency when posting
                                {
                                    // Crew Reynolds    PRUN-1790   02/03/21    Check the Validate XBWAE for FBV1 only. If FBV1, value must be blank or X
                                    // Validate XBWAE only for FBV1 Park Document TCODE
                                    fieldValue = fieldValue.ToUpper().Trim();

                                    if (TranCode == TCode.FBV1)
                                    {
                                        fieldValue = fieldValue.Trim().ToUpper();
                                        if (fieldValue != "X" && fieldValue != "")
                                        {
                                            // XBWAE field must either be blank or X
                                            validationStatus = csE + csHEADER + "-" + dtRow["XLCOL"].ToString() + xlRow.ToString();
                                            validationMessage = fieldDescription + " (" + fieldName + " must either be blank or X."; ;
                                            ErrorMessages(xlRow, validationStatus, validationMessage, true);
                                        }
                                        else
                                        {
                                            // XBWAE field can only be used when parking document
                                            validationStatus = csE + csHEADER + "-" + dtRow["XLCOL"].ToString() + xlRow.ToString();
                                            validationMessage = fieldDescription + " (" + fieldName + " is only valid when parking a document"; ;
                                            ErrorMessages(xlRow, validationStatus, validationMessage, true);

                                        }
                                    }
                                    break;
                                }

                            default: // All other header fields
                                {
                                    // Not required or validated for data. Just a basic field data check occurs
                                    vr = new ValidationRequest(xlRow, DataLevel.HEADER, fieldName, fieldValue, fieldDescription, T_NONE, false, false, true);
                                    dtExtract = ValidateField(vr);
                                    vr = null;
                                    break;
                                }
                        }
                    }
                    dtCoreData.DefaultView.RowFilter = string.Empty;
                }
            }
            catch (Exception ex)
            {
                validationStatus = csE + csHEADER;
                validationMessage = "Validation attempt failed: " + ex.Message;
                ErrorMessages(xlRow, validationStatus, validationMessage, true);
                return false;
            }
            finally
            {
                dvCoreData = null;
                vr = null;
            }

            if (Options.OtherOption.ApplicationLog) LogManage.WriteAppLog("VD-" + csHEADER + "-01: Header level validation completed.");

            return rc;
        }
        private void ValidateLineLevelFields(int xlRow)
        {
            // This method will handle all of the validations for a single line entry.

            // Change Log
            // Crew Reynolds    PRUN-1790   01/11/21    Created the ValidateLine method.
            // Crew Reynolds	PRUN-1790	01/12/21    Modified all calls to data extracts to use the new DataRequest class
            // Crew Reynolds	PRUN-1790	01/13/21    Modified to use the new ValidationRequest class
            // Crew Reynolds	PRUN-1790	01/13/21    Factored out common code to new ValidateField() method

            string validationStatus;
            string validationMessage;

            List<string> fieldsChecked = new List<string>();  // Hold a list of fields that have been checked
            ValidationRequest vr;
            DataTable dt;
            DataRow dtRow;

            // Hold key data that is used multiple times in line validations.
            DataTable dtPostingKey;
            DataTable dtAccount;

            string valFieldName = string.Empty;

            bool isHeader = false;

            if (Options.OtherOption.ApplicationLog) LogManage.WriteAppLog("VD-" + csDETAIL + "-01: Validate line (" + xlRow.ToString() + ")");

            #region Validate required number of lines [done]
            // Crew Reynolds    PRUN-1760   02/11/21 Added validation
            // There must be at least 2 lines
            try
            {
                //dtCoreData.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow;
                dtCoreData.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "'";

                if (dtCoreData.DefaultView.Count < 2)
                {
                    validationStatus = csE + csDETAIL;
                    validationMessage = "At least 2 detail lines are required.";
                    ErrorMessages(xlRow, validationStatus, validationMessage, isHeader);
                }
            }
            catch
            {
            }
            finally
            {
                dtCoreData.DefaultView.RowFilter = "";
            }
            #endregion

            #region Validate NEWBK as a valid company if present (cross-company use) [Stage 1 done]
            // Check for a NEWBK (cross-company number) to validate
            try
            {
                valFieldName = "NEWBK";
                dtCoreData.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "' AND FNAM = '" + valFieldName + "'";

                if (dtCoreData.DefaultView.Count > 0)
                {
                    // Pull the line level Company data based on NEWBK if one is present
                    dtRow = dtCoreData.DefaultView[0].Row;

                    if (dtRow["FVAL"].ToString() != "")
                    {
                        // Validate the NEWBK value
                        vr = new ValidationRequest(xlRow, DataLevel.DETAIL, valFieldName, dtRow["FVAL"].ToString(), dtRow["DESCR"].ToString(), T_COMPANYCODE, false, true, true);
                        if (!fieldsChecked.Contains(valFieldName)) fieldsChecked.Add(valFieldName);
                        dt = ValidateField(vr);

                        if (vr.FieldStatus == ReturnStatus.VALID)
                        {
                            // NEWBK value cannot be entered on the first line
                            if (xlRow == 1)
                            {
                                validationStatus = csE + csDETAIL + "-" + dtRow["XLCOL"].ToString() + xlRow; //Crew Reynolds PRUN-1760 02/09/21 Modified to use const csDETAIL
                                validationMessage = vr.FieldDescription + " (" + vr.FieldName + ") for the first line of each document must be blank.";    // #490 New Company Code (NEWBK) for the first line of each document must be blank
                                ErrorMessages(xlRow, validationStatus, validationMessage, isHeader);
                            }
                            else
                            {
                                // Update the current company values from the NEWBK validation
                                currBUKRS = dtRow["BUKRS"].ToString();
                                currKTOPL = dtRow["KTOPL"].ToString();
                            }
                        }
                        else
                        {
                            // An invalid NEWBK value was found
                            validationStatus = csE + csDETAIL + "-" + dtRow["XLCOL"].ToString() + xlRow;
                            validationMessage = dtRow["DESCR"].ToString() + " (" + vr.FieldName + ") for the first line of each document must be blank.";
                            ErrorMessages(xlRow, validationStatus, validationMessage, isHeader);
                        }
                    }
                }
            }
            catch
            {
                // Field not found - NEWBK not required.
            }
            finally
            {
                dtCoreData.DefaultView.RowFilter = string.Empty;
                dtRow = null;
                vr = null;
            }
            #endregion

            #region Validate Posting Code [Stage 1 done. Stage 2 pending.]
            // Validate posting key aka. NEWBS
            try
            {
                valFieldName = "NEWBS";
                dtCoreData.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "' AND FNAM = '" + valFieldName + "'";

                if (dtCoreData.DefaultView.Count > 0)
                {
                    dtRow = dtCoreData.DefaultView[0].Row;
                    vr = new ValidationRequest(xlRow, DataLevel.DETAIL, "NEWBS", dtRow["FVAL"].ToString(), dtRow["DESCR"].ToString(), T_POSTINGKEY, true, true, true);
                    if (!fieldsChecked.Contains(valFieldName)) fieldsChecked.Add(valFieldName);
                    dt = ValidateField(vr);

                    if (vr.FieldStatus == ReturnStatus.VALID)
                    {
                        // Save a copy of the posting key data for later use in other validations
                        dtPostingKey = null;
                        if (vr.FieldStatus == ReturnStatus.VALID) dtPostingKey = dt.Copy();

                        #region STAGE 2: Requires GetAssets() and GetFieldStatusGroups() 
                        // NOTE: This part is going to have to wait for Stage 2 data extracts (Assets, FSG)
                        // Do all of the posting key validation rules here (complex)
                        //string fieldSHKZG = string.Empty;
                        //'                        Case "A"                                ' asset accounts  rdt 10 - 17 - 05
                        //'                            value = ZeroTrim(value) '5 / 13 / 2008 JCS ZeroTrim Assets for validation for Domtar, ticket 2287
                        //'                            If Max_AS > 0 Then
                        //'                              If InStr(value, "-") > 0 Then
                        //'                                BinLow = 1:   BinHigh = Max_AS
                        //'
                        //'                                p1 = Left$(value, InStr(value, "-") - 1)    ' up to 12 postions, a "-", then up 4 positions
                        //'                                p2 = Mid$(value, Len(p1) + 2, Len(value))
                        //'                                If Len(p2) > 0 Then p2 = ZeroPad(p2, 4)
                        //'
                        //'                                p1 = ZeroPad(p1, 12)
                        //'                                p3 = p1 & "-" & p2              ' final comparison field
                        //'                                i = Max_AS + 1
                        //'                                Do
                        //'                                    sTmp = "":  BinMid = (BinLow + BinHigh) / 2
                        //'                                    If Len(Assets(2, BinMid)) > 0 Then
                        //'                                        sTmp = Left(Assets(2, BinMid), InStr(Assets(2, BinMid), "-") - 1)
                        //'                                        p4 = Mid$(Assets(2, BinMid), Len(sTmp) + 2)
                        //'                                        sTmp = ZeroPad(sTmp, 12) & "-" & ZeroPad(p4, 4)
                        //'                                    End If
                        //'
                        //'                                    Select Case True
                        //'                                        Case LenB(sTmp) = 0
                        //'                                            BinLow = BinMid + 1
                        //'                                        Case Assets(1, BinMid) < Curr_BUKRS
                        //'                                            BinLow = BinMid + 1
                        //'                                        Case Assets(1, BinMid) > Curr_BUKRS
                        //'                                            BinHigh = BinMid - 1
                        //'                                        Case Assets(1, BinMid) = Curr_BUKRS And p3 < sTmp
                        //'                                            BinHigh = BinMid - 1
                        //'                                        Case Assets(1, BinMid) = Curr_BUKRS And p3 > sTmp
                        //'                                            BinLow = BinMid + 1
                        //'                                        Case (Assets(1, BinMid) = Curr_BUKRS) And ((value = Assets(2, BinMid)) Or sTmp = p3)
                        //'                                            i = BinMid
                        //'                                            If Len(Assets(3, BinMid)) > 0 Then
                        //'                                               Call Add_Error(csE, curRow, NEWKO_Col, Translate(198) & " " & value & " " & Translate(215), , , "GLSU", "GLSU", "215")
                        //'                                            End If
                        //'                                            For j = 1 To Max_AC
                        //'                                                ' recon acct = acct and comp = comp
                        //'                                                If (Assets(4, BinMid) = Trim(Accounts(2, j))) _
                        //'                                                  And Assets(1, BinMid) = Trim(Accounts(1, j)) _
                        //'                                                Then
                        //'                                                    Curr_FSTAG = Trim(Accounts(3, j))
                        //'                                                    For k = 1 To Max_FSGS
                        //'                                                        If FSG_Status(1, k) = Curr_FSTVA _
                        //'                                                        And FSG_Status(2, k) = Curr_FSTAG Then
                        //'                                                           Curr_FAUS1 = FSG_Status(3, k)
                        //'                                                           Curr_KOART = "S"
                        //'                                                           Exit For
                        //'                                                        End If
                        //'                                                    Next
                        //'                                                End If
                        //'                                            Next j
                        //'                                            Exit Do
                        //'                                        Case Else
                        //''                                                BinLow = BinMid + 1
                        //'                                    End Select
                        //'                                Loop While BinLow <= BinHigh
                        //'
                        //'                                If i > Max_AS Then
                        //'                                     Call Add_Error(csE, curRow, NEWKO_Col, Translate(198) & " " & value & " " & Translate(212) & " " & Curr_BUKRS, , , "GLSU", "GLSU", "212")
                        //'                                End If
                        //'                                If Len(value) > 17 Or Len(p1) > 12 Or Len(p2) > 4 Then
                        //'                                    Call Add_Error(csE, curRow, NEWKO_Col, Translate(198) & " " & value & " " & Translate(205), , , "GLSU", "GLSU", "205")
                        //'                                End If
                        //'                                If LenB(GetCurValueByFld(curRow, "NEWBW")) = 0 Then
                        //'                                    Call Add_Error(csE, curRow, NEWKO_Col, Translate(209), , , "GLSU", "GLSU", "209")
                        //'                                End If
                        //'                              Else                                  '  rdt 07 - 05 - 06 ASSET but no data tkt 1311 | ZOP_LIVE_NOENTRY |
                        //'                                  Call Add_Error(csE, curRow, NEWKO_Col, Translate(489), , , "GLSU", "GLSU", "489")
                        //'                              End If
                        //'                            End If
                        //'
                        //'                        Case "S"
                        //'                             If Max_AC > 0 Then
                        //'                                value = ZeroTrim(value) '5 / 13 / 2008 JCS ZeroTrim Accounts for validation for Domtar, ticket 2287
                        //'                                BinLow = 1
                        //'                                BinHigh = Max_AC
                        //'                                i = Max_AC + 1
                        //'                                Do
                        //'                                    BinMid = (BinLow + BinHigh) / 2
                        //'                                    If value = Trim(Accounts(2, BinMid)) And Trim(Accounts(1, BinMid)) = Curr_BUKRS Then
                        //'                                        XBILK = Left(Accounts(8, BinMid), 1)  ' rdt 10 - 25 - 05 bug fix replaced i with binmid
                        //'                                        If Curr_FSTAG <> Trim(Accounts(3, BinMid)) Then
                        //'                                            Curr_FSTAG = Trim(Accounts(3, BinMid))
                        //'                                            For j = 1 To Max_FSGS
                        //'                                                If FSG_Status(1, j) = Curr_FSTVA And FSG_Status(2, j) = Curr_FSTAG Then
                        //'                                                    Curr_FAUS1 = FSG_Status(3, j)
                        //'                                                    Exit For
                        //'                                                End If
                        //'                                            Next
                        //'                                        End If
                        //'                                        i = BinMid
                        //'                                        Exit Do
                        //'                                    ElseIf Trim(Accounts(1, BinMid)) > Curr_BUKRS Or _
                        //'                                            ( _
                        //'                                                Trim(Accounts(1, BinMid)) = Curr_BUKRS And _
                        //'                                                ZeroPad(value, 10) < ZeroPad(Trim(Accounts(2, BinMid)), 10) _
                        //'                                            ) Then
                        //'                                        BinHigh = BinMid - 1
                        //'                                    Else
                        //'                                        BinLow = BinMid + 1
                        //'                                    End If
                        //'                                Loop While BinLow <= BinHigh
                        //'
                        //'                                If i > Max_AC Then
                        //'                                    Call Add_Error(csE, curRow, NEWKO_Col, Translate(513, value, Curr_BUKRS), , , "GLSU", "GLSU", "513") '"Account & not found in Company Code &"
                        //'                                Else
                        //'                                    If Trim(Accounts(4, i)) = "X" Then
                        //'                                        Call Add_Error(csE, curRow, NEWKO_Col, Translate(514, value, Curr_BUKRS), , , "GLSU", "GLSU", "514") '"Account &, Company Code &, is marked for deletion"
                        //'                                    End If
                        //'                                    If Left(Accounts(5, i), 1) = "X" Or Left(Accounts(7, i), 1) = "X" Then
                        //'                                        Call Add_Error(csE, curRow, NEWKO_Col, Translate(515, value, Curr_BUKRS), , , "GLSU", "GLSU", "515")  '"Account &, Company Code &, is blocked for posting"
                        //'                                    End If
                        //'                                    If Left(Accounts(6, i), 1) = "X" Then
                        //'                                        Call Add_Error(csE, curRow, NEWKO_Col, Translate(516, value), , , "GLSU", "GLSU", "516")   '"Account & is marked for deletion"
                        //'                                    End If
                        //'                                    If Trim(Accounts(11, i)) = "X" And tCode <> TCode_FBB1 Then   'Account & in Company code # is set to post automatically only.
                        //'                                       Call Add_Error(csE, curRow, NEWKO_Col, Replace(Replace(Translate(556), "#", Curr_BUKRS), "&", value))
                        //'                                    End If      ' RDT 12 / 20 / 2012 vs 5.0, 3 / 5 / 2013 not error if tcode fbb1 Fonterra #6097
                        //'                                End If  'found an account
                        //'                            End If  'if we are validating accounts(max_ac > 0)
                        //'
                        //''JCS 9 / 10 / 2010 - Reorganized Cost Element lookup so it would be available for KOSTL checking, ticket 4058, Honeywell
                        //'                            If (Max_CE > 0 Or Max_CEC > 0) And XBILK <> "X" And Max_FSGF > 0 And NEWKO_Col > 0 Then     'And Max_PAS > 0 Then
                        //'                '               If Accounts(8, I) <> "X" And Max_CE > 0 Then
                        //'                                value = Trim(UCase(NoErrorCell(Cells(curRow, NEWKO_Col))))
                        //'                                Is_Cost_Element = False
                        //'                                If Max_CEC > 0 Then
                        //'                                    PostDateStr = Format(curr_BUDAT, "YYYYMMDD")
                        //'                                    If PostDateStr = "" Then PostDateStr = Format(Now(), "YYYYMMDD")    'JCS 7 / 1 / 14 BF ticket 7190 default Posting Date(BUDAT) to Current Date if not supplied
                        //'                                    For j = 1 To Max_CEC
                        //'                                        ' 2008 - 04 - 07 JCS vvv added Trim around CEC fields, Nike, Ticket 2208
                        //'                                        If value = Trim(Cost_El_Contr(j).CostElement) And Curr_BUKRS = Trim(Cost_El_Contr(j).Company) Then
                        //'                                            If Trim(Cost_El_Contr(j).DateFrom) = "" Or Cost_El_Contr(j).DateFrom <= PostDateStr Then
                        //'                                                If Trim(Cost_El_Contr(j).DateTo) = "" Or Cost_El_Contr(j).DateTo >= PostDateStr Then
                        //'                                        ' 2008 - 04 - 07 JCS ^ ^^added Trim around CEC fields, Nike, Ticket 2208
                        //'                                                    Is_Cost_Element = True
                        //'                                                    Curr_KATYP = Cost_El_Contr(j).CEType    'JCS 9 / 10 / 2010 - Added KATYP for Cost Element Category, ticket 4058, Honeywell
                        //'                                                    Exit For
                        //'                                                End If
                        //'                                            End If
                        //'                                        End If
                        //'                                    Next
                        //'                                Else
                        //'                                    For j = 1 To Max_CE
                        //'                                        If value = Cost_Elements(2, j) And Curr_KTOPL = Cost_Elements(1, j) Then
                        //'                                            Is_Cost_Element = True
                        //'                                            Exit For
                        //'                                        End If
                        //'                                    Next
                        //'                                End If
                        //'                            End If
                        //''JCS 9 / 10 / 2010 - Reorganized Cost Element lookup so it would be available for KOSTL checking, ticket 4058, Honeywell
                        //'                        Case "D"
                        //'                            value = ZeroTrim(value) '5 / 13 / 2008 JCS ZeroTrim Customers for validation for Domtar, ticket 2287
                        //'                            If Max_CUST > 0 Then
                        //'                                For i = 1 To Max_CUST
                        //'                                    If value = Trim(Customers(i).Customer) And Trim(Customers(i).Company) = Curr_BUKRS Then
                        //''''                                        Line_LAND1 = Trim(Customers(i).Country)
                        //'                                        For j = 1 To Max_AC
                        //'                                            If Trim(Customers(i).account) = Trim(Accounts(2, j)) And Trim(Customers(i).Company) = Trim(Accounts(1, j)) Then
                        //'                                                XBILK = Left(Accounts(8, j), 1)
                        //'                                                Curr_FSTAG = Trim(Accounts(3, j))
                        //'                                                For k = 1 To Max_FSGS
                        //'                                                    If FSG_Status(1, k) = Curr_FSTVA And FSG_Status(2, k) = Curr_FSTAG Then
                        //'                                                        Curr_FAUS1 = FSG_Status(3, k)
                        //'                                                        Curr_KOART = "S"
                        //'                                                        Exit For
                        //'                                                    End If
                        //'                                                Next
                        //'                                                Exit For
                        //'                                            End If
                        //'                                        Next
                        //'                                        Exit For
                        //'                                    End If
                        //'                                Next
                        //'                                If i > Max_CUST Then
                        //'                                    Call Add_Error(csE, curRow, NEWKO_Col, Translate(217) & " " & value & " " & Translate(218) & " " & Curr_BUKRS, , , "GLSU", "GLSU", "218")
                        //'                                Else
                        //'                                    If Trim(Customers(i).BlockComp) = "X" Then
                        //'                                        Call Add_Error(csE, curRow, NEWKO_Col, Translate(217) & " " & value & " " & Translate(219) & " " & Curr_BUKRS, , , "GLSU", "GLSU", "219")
                        //'                                    End If
                        //'                                    If Trim(Customers(i).DelComp) = "X" Then
                        //'                                        Call Add_Error(csE, curRow, NEWKO_Col, Translate(217) & " " & value & " " & Translate(220) & " " & Curr_BUKRS, , , "GLSU", "GLSU", "220")
                        //'                                    End If
                        //'                                    If Trim(Customers(i).BlockCent) = "X" Then
                        //'                                        Call Add_Error(csE, curRow, NEWKO_Col, Translate(217) & " " & value & " " & Translate(221), , , "GLSU", "GLSU", "221")
                        //'                                    End If
                        //'                                    If Trim(Customers(i).DelCent) = "X" Then
                        //'                                        Call Add_Error(csE, curRow, NEWKO_Col, Translate(217) & " " & value & " " & Translate(222), , , "GLSU", "GLSU", "222")
                        //'                                    End If
                        //'                                End If
                        //'                            End If
                        //'                        Case "K"
                        //'                            If Max_VEND > 0 Then
                        //'                            value = ZeroTrim(value) '5 / 13 / 2008 JCS ZeroTrim Vendors for validation for Domtar, ticket 2287
                        //'                                For i = 1 To Max_VEND
                        //'                                    If value = Trim(Vendors(i).Vendor) And Trim(Vendors(i).Company) = Curr_BUKRS Then
                        //'''                                        Line_LAND1 = Trim(Vendors(i).Country)
                        //'                                        For j = 1 To Max_AC
                        //'                                            If Trim(Vendors(i).account) = Trim(Accounts(2, j)) And Trim(Vendors(i).Company) = Trim(Accounts(1, j)) Then
                        //'                                                XBILK = Left(Accounts(8, j), 1)
                        //'                                                Curr_FSTAG = Trim(Accounts(3, j))
                        //'                                                For k = 1 To Max_FSGS
                        //'                                                    If FSG_Status(1, k) = Curr_FSTVA And FSG_Status(2, k) = Curr_FSTAG Then
                        //'                                                        Curr_FAUS1 = FSG_Status(3, k)
                        //'                                                        Curr_KOART = "S"
                        //'                                                        Exit For
                        //'                                                    End If
                        //'                                                Next
                        //'                                                Exit For
                        //'                                            End If
                        //'                                        Next
                        //'                                        Exit For
                        //'                                    End If
                        //'                                Next
                        //'                                If i > Max_VEND Then
                        //'                                    Call Add_Error(csE, curRow, NEWKO_Col, Translate(223) & " " & value & " " & Translate(218) & " " & Curr_BUKRS)
                        //'                                Else
                        //'                                    If Trim(Vendors(i).BlockComp) = "X" Then
                        //'                                        Call Add_Error(csE, curRow, NEWKO_Col, Translate(223) & " " & value & " " & Translate(219) & " " & Curr_BUKRS)
                        //'                                    End If
                        //'                                    If Trim(Vendors(i).DelComp) = "X" Then
                        //'                                        Call Add_Error(csE, curRow, NEWKO_Col, Translate(223) & " " & value & " " & Translate(220) & " " & Curr_BUKRS)
                        //'                                    End If
                        //'                                    If Trim(Vendors(i).BlockCent) = "X" Then
                        //'                                        Call Add_Error(csE, curRow, NEWKO_Col, Translate(223) & " " & value & " " & Translate(221))
                        //'                                    End If
                        //'                                    If Trim(Vendors(i).DelCent) = "X" Then
                        //'                                        Call Add_Error(csE, curRow, NEWKO_Col, Translate(223) & " " & value & " " & Translate(222))
                        //'                                    End If
                        //'                                End If
                        //'                            End If
                        //'                    End Select
                        //End If
                        //End If
                        #endregion
                    }
                }
            }
            catch
            {
                // NEWBS is required field and it is missing from the user data
                validationStatus = csE + csDETAIL + "-R" + xlRow;
                validationMessage = "Rquired Posting Key field (NEWBS) is missing.";
                ErrorMessages(xlRow, validationStatus, validationMessage, isHeader);
            }
            finally
            {
                vr = null;
                dtRow = null;
                dtCoreData.DefaultView.RowFilter = string.Empty; //Added By Crew On 08 Feb 2021
            }
            #endregion

            #region Validate Account Number [Stage 1 done. Stage 2 pending.]
            // Validate account number (NEWKO)
            try
            {
                valFieldName = "NEWKO";
                dtCoreData.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "' AND FNAM = '" + valFieldName + "'";

                if (dtCoreData.DefaultView.Count > 0)
                {
                    dtRow = dtCoreData.DefaultView[0].Row;
                    vr = new ValidationRequest(xlRow, DataLevel.DETAIL, valFieldName, dtRow["FVAL"].ToString(), dtRow["DESCR"].ToString(), T_ACCOUNT, true, true, true);
                    if (!fieldsChecked.Contains(valFieldName)) fieldsChecked.Add(valFieldName);
                    dt = ValidateField(vr);

                    // Save a copy of the account data for later use in other validations
                    dtAccount = null;
                    if (vr.FieldStatus == ReturnStatus.VALID) dtAccount = dt.Copy();
                }
            }
            catch
            {
                // An invalid Account field value was found
                validationStatus = csE + csDETAIL + "-R" + xlRow;
                validationMessage = "Account field (NEWKO) is missing.";
                ErrorMessages(xlRow, validationStatus, validationMessage, isHeader);
            }
            finally
            {
                vr = null;
                dtRow = null;
                dtCoreData.DefaultView.RowFilter = string.Empty; //Added By Crew On 08 Feb 2021
            }
            #endregion

            #region Validate Amount [done]
            // Validate amount field (WRBTR)
            try
            {
                valFieldName = "WRBTR";
                dtCoreData.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "' AND FNAM = '" + valFieldName + "'";
                if (!fieldsChecked.Contains(valFieldName)) fieldsChecked.Add(valFieldName);
                if (dtCoreData.DefaultView.Count > 0)
                {
                    dtRow = dtCoreData.DefaultView[0].Row;

                    // Check Amount field for blank
                    if (dtRow["FVAL"].ToString() == "")
                    {
                        // Amount field cannot be blank
                        validationStatus = csE + csDETAIL + "-" + dtRow["XLCOL"].ToString() + xlRow;
                        validationMessage = dtRow["DESCR"].ToString() + " field (WRBTR) is required.";
                        ErrorMessages(xlRow, validationStatus, validationMessage, isHeader);
                    }
                    else
                    {
                        // Amount cannot be negative or non-numeric 
                        try
                        {
                            Double val = Convert.ToDouble(dtRow["FVAL"].ToString());  // if the value is non-numeric, the catch will handle it.

                            // Good number. Is it negative?
                            if (val < 0)
                            {
                                validationStatus = csE + csDETAIL + "-" + dtRow["XLCOL"].ToString() + xlRow;
                                validationMessage = "Amount field (WRBTR) cannot be negative. Use Posting Keys to reverse an amount.";
                                ErrorMessages(xlRow, validationStatus, validationMessage, isHeader);
                            }
                        }
                        catch
                        {
                            // Amount field is not numeric
                            validationStatus = csE + csDETAIL + "-" + dtRow["XLCOL"].ToString() + xlRow;
                            validationMessage = "Amount field (WRBTR) is not numeric.";
                            ErrorMessages(xlRow, validationStatus, validationMessage, isHeader);
                        }
                    }
                }
            }
            catch
            {
                // Amount field (WRBTR) is missing.
                validationStatus = csE + csDETAIL + "-R" + xlRow;
                validationMessage = "Required Amount field (WRBTR) is missing.";
                ErrorMessages(xlRow, validationStatus, validationMessage, isHeader);
            }
            finally
            {
                vr = null;
                dtRow = null;
                dtCoreData.DefaultView.RowFilter = string.Empty; //Added By Crew On 08 Feb 2021
            }
            #endregion

            #region Validate the Notes field [done]
            // Validate DOC_ITEM field (DOC_ITEM)
            try
            {
                dtCoreData.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "' AND ( FNAM = 'DOC_ITEM' OR FNAME LIKE 'DOC_ITEM%' )";

                if (dtCoreData.DefaultView.Count > 0)
                {
                    // Just checking for length of DOC_ITEM
                    dtRow = dtCoreData.DefaultView[0].Row;
                    valFieldName = dtRow["FNAM"].ToString();
                    vr = new ValidationRequest(xlRow, DataLevel.DETAIL, valFieldName, dtRow["FVAL"].ToString(), dtRow["DESCR"].ToString(), T_ACCOUNT, false, false, true);
                    if (!fieldsChecked.Contains(valFieldName)) fieldsChecked.Add(valFieldName);
                    dt = ValidateField(vr);

                    if (vr.FieldStatus == ReturnStatus.VALID)
                    {
                        // TODO   Build Validate_Text function to check for malformed text entry
                        //        See Validate_Text(value, curRow, CurCol, field, Cells(BSEG_Row +1, CurCol).value) in GLSU - minor/rare use
                    }
                }
            }
            catch
            {
            }
            finally
            {
                vr = null;
                dtRow = null;
                dtCoreData.DefaultView.RowFilter = string.Empty;
            }
            #endregion

            #region Validate PA Segment (RKE_*) fields [Stage 1 done. Stage 2 pending.]
            // Validate PA Segment (RKE_*) fields
            try
            {
                bool foundRKE_Fields = false;
                bool foundRKE_BUKRS = false;

                dtCoreData.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "' AND FNAM LIKE 'RKE%'";

                if (dtCoreData.DefaultView.Count > 0)
                {
                    dtRow = dtCoreData.DefaultView[0].Row;

                    for (int i = 0; i < dtCoreData.DefaultView.Count; i++)
                    {
                        if (dtCoreData.DefaultView[i]["FVAL"].ToString() == "RKE_BUKRS")
                        {
                            foundRKE_BUKRS = true;
                            valFieldName = dtCoreData.DefaultView[i]["FNAM"].ToString();
                            if (!fieldsChecked.Contains(valFieldName)) fieldsChecked.Add(valFieldName);
                        }
                        else
                        {
                            foundRKE_Fields = true;
                            valFieldName = dtCoreData.DefaultView[i]["FNAM"].ToString();
                            if (!fieldsChecked.Contains(valFieldName)) fieldsChecked.Add(valFieldName);
                        }
                    }
                }

                // If there are any RKE_ fields found then RKE_BUKRS must be one of them and valid
                if (foundRKE_Fields)
                {
                    if (foundRKE_BUKRS)
                    {
                        // RKE_BUKRS must be a valid company code
                        valFieldName = "RKE_BUKRS";
                        dtCoreData.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "' AND FNAM = '" + valFieldName + "'";

                        if (dtCoreData.DefaultView.Count > 0)
                        {
                            if (!fieldsChecked.Contains(valFieldName)) fieldsChecked.Add(valFieldName);  // Used to log RKE_BUKRS check

                            valFieldName = "BUKRS";
                            dtRow = dtCoreData.DefaultView[0].Row;
                            vr = new ValidationRequest(xlRow, DataLevel.DETAIL, valFieldName, dtRow["FVAL"].ToString(), dtRow["DESCR"].ToString(), T_COMPANYCODE, true, true, true);
                            dt = ValidateField(vr);
                            foundRKE_BUKRS = false;
                        }
                    }

                    // foundRKE_Fields good RKE_BUKRS but no RKE-% fields
                    if (!foundRKE_BUKRS && foundRKE_Fields)
                    {
                        validationStatus = csE + csDETAIL + "-" + dtRow["XLCOL"].ToString() + xlRow;
                        validationMessage = "PA Segment (RKE_* fields) present but RKE_BUKRS is missing or invalid.";
                        ErrorMessages(xlRow, validationStatus, validationMessage, isHeader);
                    }

                    // Found RKE-% fields but RKE_BUKRS is missing or invalid
                    if (foundRKE_BUKRS && !foundRKE_Fields)
                    {
                        validationStatus = csE + csDETAIL + "-" + dtRow["XLCOL"].ToString() + xlRow;
                        validationMessage = "PA Company Code (RKE_BUKRS) must be present when using any RKE_* fields.";
                        ErrorMessages(xlRow, validationStatus, validationMessage, isHeader);
                    }
                }
            }
            catch
            {
            }
            finally
            {
                vr = null;
                dtRow = null;
                dtCoreData.DefaultView.RowFilter = string.Empty;
            }
            #endregion

            #region Validate Cost Center [Stage 1 done. Stage 2 pending.]
            // Validate cost center

            // Change Log
            // Crew Reynolds    PRUN-1790   02/17/21    Added Cost Center validation
            // Crew Reynolds    PRUN-1790   02/26/21    Completed

            try
            {
                valFieldName = "KOSTL";
                dtCoreData.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "' AND FNAM = '" + valFieldName + "'";

                // Crew Reynolds    PRUN-1790   03/03/21    Check for blank before call due to field not required and 
                if (dtCoreData.DefaultView.Count > 0)
                {
                    dtRow = dtCoreData.DefaultView[0].Row;
                    vr = new ValidationRequest(xlRow, DataLevel.DETAIL, valFieldName, dtRow["FVAL"].ToString(), dtRow["DESCR"].ToString(), T_COSTCNTR, false, true, false);
                    if (!fieldsChecked.Contains(valFieldName)) fieldsChecked.Add(valFieldName);
                    dt = ValidateField(vr);

                    if (vr.FieldStatus != ReturnStatus.VALID)
                    {
                        // Cost Center not valid
                        validationStatus = csE + vr.StatusCode + "-" + dtRow["XLCOL"].ToString() + vr.Row.ToString();
                        validationMessage = vr.FieldDescription + " (" + vr.FieldName + ") is not valid for Company " + currBUKRS;
                        ErrorMessages(vr.Row, validationStatus, validationMessage, false);
                    }
                    else
                    {
                        // Commneted By Crew On 10 Mar 2021
                        /*
                        // Good Cost Center. Check the date range to see if it is active.
                        dt.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "' AND FNAM = 'DATAB'";
                        DateTime startDate = Convert.ToDateTime(dt.Rows[0]["DATAB"].ToString(), CultureInfo.InvariantCulture);

                        dt.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "' AND FNAM = 'DATBI'";
                        DateTime endDate = Convert.ToDateTime(dt.Rows[0]["DATBI"].ToString(), CultureInfo.InvariantCulture);
                        */

                        //Changed By Crew On 10 Mar 2021
                        DateTime startDate = DateTime.Now;
                        dtCoreData.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "' AND FNAM = 'DATAB'";
                        if (dtCoreData.DefaultView.Count > 0)
                            startDate = Convert.ToDateTime(dt.Rows[0]["DATAB"].ToString(), CultureInfo.InvariantCulture);

                        DateTime endDate = DateTime.Now;
                        dtCoreData.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "' AND FNAM = 'DATBI'";
                        if (dtCoreData.DefaultView.Count > 0)
                            endDate = Convert.ToDateTime(dt.Rows[0]["DATBI"].ToString(), CultureInfo.InvariantCulture);
                        //End

                        DateTime currPostingDate = Convert.ToDateTime(dt.Rows[0]["DATAB"].ToString(), CultureInfo.InvariantCulture);

                        if (currPostingDate != null)
                            currPostingDate = Convert.ToDateTime(currPostingDate, CultureInfo.InvariantCulture);     // Good currBUDAT
                        else
                            currPostingDate = Convert.ToDateTime(DateTime.Now, CultureInfo.InvariantCulture);        // Blank currBUDAT. Use today's date.
                    }
                }
            }
            catch
            {
            }
            finally
            {
                vr = null;
                dtRow = null;
                dtCoreData.DefaultView.RowFilter = string.Empty; //Added By Crew On 08 Feb 2021
            }
            #endregion

            #region Validate Cost Elements Controlling [Stage 2. New as of 2/24/21]
            // TODO in Stage 2
            // NOTES AND VBA FOLLOW

            //try
            //{
            //    dtCoreData.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "' AND FNAM = 'KSTAR'";

            //    if (dtCoreData.DefaultView.Count > 0)
            //    {
            //        dtRow = dtCoreData.DefaultView[0].Row;
            //        vr = new ValidationRequest(xlRow, DataLevel.DETAIL, "KOSTL", dtRow["FVAL"].ToString(), dtRow["DESCR"].ToString(), T_COSTCNTR, false, true, true);
            //        dt = ValidateField(vr); 

            //        if (vr.FieldStatus == ReturnStatus.VALID)
            //        {
            //            // Good Cost Center. Check the date range to see if it is active.
            //            dtCoreData.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "' AND FNAM = 'KOSTL'";
            //            if (dtCoreData.DefaultView.Count > 0)
            //            {
            //                DateTime startDate = Convert.ToDateTime(dt.Rows[0]["DATAB"].ToString(), CultureInfo.InvariantCulture);
            //                dtCoreData.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "' AND FNAM = 'KOSTL'";

            //                DateTime endDate = Convert.ToDateTime(dt.Rows[0]["DATBI"].ToString(), CultureInfo.InvariantCulture);
            //                dtCoreData.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "' AND FNAM = 'KOSTL'";

            //                DateTime currPostingDate = Convert.ToDateTime(dt.Rows[0]["DATAB"].ToString(), CultureInfo.InvariantCulture);

            //                if (currPostingDate != null)
            //                    currPostingDate = Convert.ToDateTime(currPostingDate, CultureInfo.InvariantCulture);     // Good currBUDAT
            //                else
            //                    currPostingDate = Convert.ToDateTime(DateTime.Now, CultureInfo.InvariantCulture);  // Blank currBUDAT. Use today's date.
            //            }

            //            // TODO STAGE 2
            //            //perform makefields using 'KOKRS' csks_fields - kokrs primkey.
            //            //perform makefields using 'KOSTL' csks_fields - kostl primkey.
            //            //perform makefields using 'BUKRS' csks_fields - bukrs noindex.
            //            //perform makefields using 'DATAB' csks_fields - datab noindex.
            //            //perform makefields using 'DATBI' csks_fields - datbi noindex.
            //            //perform makefields using 'BKZKP' csks_fields - bkzkp noindex. Lock Indicator for Actual Primary Postings
            //            //perform makefields using 'BKZKS' csks_fields - bkzks noindex. Lock Indicator for Plan Primary Costs
            //            //perform makefields using 'BKZER' csks_fields - bkzer noindex.
            //            //perform makefields using 'BKZOB' csks_fields - bkzob noindex.
            //            //perform makefields using 'PKZKP' csks_fields - pkzkp noindex.
            //            //perform makefields using 'PKZKS' csks_fields - pkzks noindex.
            //            //perform makefields using 'PKZER' csks_fields - pkzer noindex.

            //            //}

            //            //// TODO Requires a new data extract or extention to GetCostCenter() to get KATYP for Cost Element Controlling data (low priority)
            //            //string listKATYP = "[11][12][50][51]";
            //            //string findKATYP = "[" + currKATYP + "]";
            //            //if (listKATYP.IndexOf(currKA) > 0)
            //            //{
            //            //    //                    If Left(Cost_Ctrs(8, i), 1) = "X" Then  'JCS 9/10/2010 - New error for Cost Center that is Locked for Revenue, ticket 4058, Honeywell
            //            //    //                        Call Add_Error(csE, curRow, CurCol, Translate(533, value), , , "GLSU", "GLSU", "533") '"Cost Center & is locked for Actual Revenues"   'JCS 9 / 10 / 2010 - New error for Cost Center that is Locked for Revenue, ticket 4058, Honeywell

            //            //    //                    If Left(Cost_Ctrs(7, i), 1) = "X" Then
            //            //    //                        Call Add_Error(csE, curRow, CurCol, Translate(520, value), , , "GLSU", "GLSU", "520") '"Cost Center & is locked for Actual Primary costs"
            //            //}

            //            //SELECT TKA02~BUKRS TKA02~GSBER CSKB~KSTAR CSKB~KATYP
            //            //         CSKB~DATAB CSKB~DATBI                                       "40
            //            //        FROM TKA02
            //            //         JOIN CSKB ON TKA02~KOKRS = CSKB~KOKRS
            //            //         APPENDING TABLE TKA02_FIELDS                                "30
            //            //         WHERE TKA02~BUKRS IN SO_BUKRS
            //            //           AND CSKB~KSTAR IN SO_KSTAR_LITE.                          "30
            //            //DELETE ADJACENT DUPLICATES FROM TKA02_FIELDS.
            //            //APPEND '[Cost Elements Controlling]' TO TEXTBUFFER.
            //            //clear textbuffer.
            //            //perform makefields using 'BUKRS' TKA02_fields - BUKRS primkey.
            //            //perform makefields using 'GSBER' TKA02_fields - GSBER primkey.
            //            //perform makefields using 'KSTAR' TKA02_fields - KSTAR noindex.
            //            //perform makefields using 'KATYP' TKA02_fields - KATYP noindex.
            //            //perform makefields using 'DATAB' TKA02_fields - DATAB noindex.       "40
            //            //perform makefields using 'DATBI' TKA02_fields - DATBI noindex.       "40

            //        }
            //    }
            //}
            //catch
            //{
            //}
            //finally
            //{
            //    vr = null;
            //    dtRow = null;
            //    dtCoreData.DefaultView.RowFilter = string.Empty; //Added By Crew On 08 Feb 2021
            //}
            #endregion

            #region Validate Cost Center Elements [Stage 1 done.]
            // Validate cost center elements
            // Crew Reynolds    PRUN-1790   02/17/21    Added Cost Element validation

            try
            {
                valFieldName = "KSTAR";
                dtCoreData.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "' AND FNAM = '" + valFieldName + "'";

                if (dtCoreData.DefaultView.Count > 0)
                {
                    dtRow = dtCoreData.DefaultView[0].Row;
                    vr = new ValidationRequest(xlRow, DataLevel.DETAIL, valFieldName, dtRow["FVAL"].ToString(), dtRow["DESCR"].ToString(), T_COSTELEM, true, true, true);
                    if (!fieldsChecked.Contains(valFieldName)) fieldsChecked.Add(valFieldName);
                    dt = ValidateField(vr);
                }
            }
            catch
            {
            }
            finally
            {
                vr = null;
                dtRow = null;
                dtCoreData.DefaultView.RowFilter = string.Empty; //Added By Crew On 08 Feb 2021
            }
            #endregion

            #region Validate Profit Center [done]
            // Validate profit center

            // Change Log
            // Crew Reynolds    PRUN-1790   02/26/21    Added Profit Center validation

            try
            {
                valFieldName = "PRCTR";
                dtCoreData.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "' AND FNAM = '" + valFieldName + "'";

                if (dtCoreData.DefaultView.Count > 0)
                {
                    dtRow = dtCoreData.DefaultView[0].Row;
                    vr = new ValidationRequest(xlRow, DataLevel.DETAIL, valFieldName, dtRow["FVAL"].ToString(), dtRow["DESCR"].ToString(), T_PROFITCNTR, false, true, true);
                    if (!fieldsChecked.Contains(valFieldName)) fieldsChecked.Add(valFieldName);
                    dt = ValidateField(vr);

                    if (vr.FieldStatus == ReturnStatus.VALID)
                    {
                        //perform makefields using 'PRCTR' cepc_fields - prctr primkey.
                        //perform makefields using 'KOKRS' cepc_fields - kokrs primkey.
                        //perform makefields using 'BUKRS' cepc_fields - bukrs primkey.
                        //perform makefields using 'LOCK_IND' cepc_fields - lock_ind noindex.

                        dt.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "' AND FNAM = 'LOCK_IND'";

                        if (dt.DefaultView[0]["FVAL"].ToString() == "X")
                        {
                            // Profit Center is locked
                            validationStatus = csE + vr.StatusCode + "-" + dtRow["XLCOL"].ToString() + vr.Row.ToString();
                            validationMessage = vr.FieldDescription + " (" + vr.FieldName + ") is locked";
                            ErrorMessages(vr.Row, validationStatus, validationMessage, false);
                        }

                        // TODO Stage 2 - Maybe - ZCUST1 data may not be available
                        //                    If GSBER_Col = 0 Then
                        //                        Call Add_Error(csE, curRow, CurCol, Translate(524, value, ZCUST1(2, i)), , , "GLSU", "GLSU", "524")   '"Profit Center & requires Business Area &"
                        //                    Else
                        //                        Call Add_Error(csE, curRow, GSBER_Col, Translate(524, value, ZCUST1(2, i)), , , "GLSU", "GLSU", "524") '"Profit Center & requires Business Area &"
                        //                    End If
                        //                Else
                        //                    If value<> ZCUST1(1, i) Then
                        //                       Call Add_Error(csE, curRow, CurCol, Translate(525, value), , , "GLSU", "GLSU", "525") '"Profit Center & doesn't have an assigned Business Area"
                        //                    End If
                        //                End If
                        //            End If
                        //        End If
                    }
                }
            }
            catch
            {
            }
            finally
            {
                vr = null;
                dtRow = null;
                dtCoreData.DefaultView.RowFilter = string.Empty; //Added By Crew On 08 Feb 2021
            }
            #endregion

            #region Validate Customer [done]
            // Validate the KUNNR (Customer key)

            try
            {
                valFieldName = "KUNNR";
                dtCoreData.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "' AND FNAM = '" + valFieldName + "'";

                if (dtCoreData.DefaultView.Count > 0)
                {
                    dtRow = dtCoreData.DefaultView[0].Row;
                    vr = new ValidationRequest(xlRow, DataLevel.DETAIL, valFieldName, dtRow["FVAL"].ToString(), dtRow["DESCR"].ToString(), T_CUSTOMER, false, true, true);
                    if (!fieldsChecked.Contains(valFieldName)) fieldsChecked.Add(valFieldName);
                    dt = ValidateField(vr);

                    if (vr.FieldStatus == ReturnStatus.VALID)
                    {
                        // Check the customer data returned for block and delete flags
                        // Company level                        
                        //(KNB1) SPERR   SPERB_B CHAR    1   0   Posting block for company code
                        //(KNB1) LOEVM   LOEVM_B CHAR    1   0   Deletion Flag for Master Record (Company Code Level)

                        dt.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "' AND FNAM = 'SPERR'";
                        if (dt.DefaultView[0]["SPERR"].ToString() == "X")
                        {
                            validationStatus = csE + csDETAIL + dtRow["XLROW"] + xlRow;
                            validationMessage = "Customer " + vr.FieldValue + " is blocked for posting in company " + currBUKRS + ".";
                            ErrorMessages(xlRow, validationStatus, validationMessage, isHeader);
                        }

                        dt.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "' AND FNAM = 'LOEVM'";
                        if (dt.DefaultView[0]["LOEVM"].ToString() == "X")
                        {
                            validationStatus = csE + csDETAIL + dtRow["XLROW"] + xlRow;
                            validationMessage = "Customer " + vr.FieldValue + " is flagged for deletion in company " + currBUKRS + ".";
                            ErrorMessages(xlRow, validationStatus, validationMessage, isHeader);
                        }

                        // Central level                        
                        //(KNA1) KNA1_SPERR   SPERB_B CHAR    1   0   Posting block for company code
                        //(KNA1) KNA1_LOEVM   LOEVM_B CHAR    1   0   Deletion Flag for Master Record (Company Code Level)

                        dt.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "' AND FNAM = 'KNA1-SPERR'";
                        if (dt.DefaultView[0]["KNA1-SPERR"].ToString() == "X")
                        {
                            validationStatus = csE + csDETAIL + dtRow["XLROW"] + xlRow;
                            validationMessage = "Customer " + vr.FieldValue + " is blocked for posting in company.";
                            ErrorMessages(xlRow, validationStatus, validationMessage, isHeader);
                        }

                        dt.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "' AND FNAM = 'KNA1-LOEVM'";
                        if (dt.DefaultView[0]["KNA1-LOEVM"].ToString() == "X")
                        {
                            validationStatus = csE + csDETAIL + dtRow["XLROW"] + xlRow;
                            validationMessage = "Customer " + vr.FieldValue + " is flagged for deletion centrally.";
                            ErrorMessages(xlRow, validationStatus, validationMessage, isHeader);
                        }
                    }
                }
            }
            catch
            {
            }
            finally
            {
                vr = null;
                dtRow = null;
                dtCoreData.DefaultView.RowFilter = string.Empty;
            }
            #endregion

            #region Validate Vendor [done]
            // Validate the LIFNR (Venddor key)

            try
            {
                valFieldName = "LIFNR";
                dtCoreData.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "' AND FNAM = '" + valFieldName + "'";

                if (dtCoreData.DefaultView.Count > 0)
                {
                    dtRow = dtCoreData.DefaultView[0].Row;
                    vr = new ValidationRequest(xlRow, DataLevel.DETAIL, valFieldName, dtRow["FVAL"].ToString(), dtRow["DESCR"].ToString(), T_CUSTOMER, false, true, true);
                    if (!fieldsChecked.Contains(valFieldName)) fieldsChecked.Add(valFieldName);
                    dt = ValidateField(vr);

                    if (vr.FieldStatus == ReturnStatus.VALID)
                    {
                        // Check the customer data returned for block and delete flags
                        // Company level                        
                        //(KNB1) SPERR   SPERB_B CHAR    1   0   Posting block for company code
                        //(KNB1) LOEVM   LOEVM_B CHAR    1   0   Deletion Flag for Master Record (Company Code Level)

                        dt.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "' AND FNAM = 'SPERR'";
                        if (dt.DefaultView[0]["SPERR"].ToString() == "X")
                        {
                            validationStatus = csE + csDETAIL + dtRow["XLROW"] + xlRow;
                            validationMessage = "Vendor " + vr.FieldValue + " is blocked for posting in company " + currBUKRS + ".";
                            ErrorMessages(xlRow, validationStatus, validationMessage, isHeader);
                        }

                        dt.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "' AND FNAM = 'LOEVM'";
                        if (dt.DefaultView[0]["LOEVM"].ToString() == "X")
                        {
                            validationStatus = csE + csDETAIL + dtRow["XLROW"] + xlRow;
                            validationMessage = "Vendor " + vr.FieldValue + " is flagged for deletion in company " + currBUKRS + ".";
                            ErrorMessages(xlRow, validationStatus, validationMessage, isHeader);
                        }

                        // Central level                        
                        //(KNA1) KNA1_SPERR   SPERB_B CHAR    1   0   Posting block for company code
                        //(KNA1) KNA1_LOEVM   LOEVM_B CHAR    1   0   Deletion Flag for Master Record (Company Code Level)

                        dt.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "' AND FNAM = 'KNA1-SPERR'";
                        if (dt.DefaultView[0]["KNA1-SPERR"].ToString() == "X")
                        {
                            validationStatus = csE + csDETAIL + dtRow["XLROW"] + xlRow;
                            validationMessage = "Vendor " + vr.FieldValue + " is blocked for posting in company.";
                            ErrorMessages(xlRow, validationStatus, validationMessage, isHeader);
                        }

                        dt.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "' AND FNAM = 'KNA1-LOEVM'";
                        if (dt.DefaultView[0]["KNA1-LOEVM"].ToString() == "X")
                        {
                            validationStatus = csE + csDETAIL + dtRow["XLROW"] + xlRow;
                            validationMessage = "Vendor " + vr.FieldValue + " is flagged for deletion centrally.";
                            ErrorMessages(xlRow, validationStatus, validationMessage, isHeader);
                        }
                    }
                }
            }
            catch
            {
            }
            finally
            {
                vr = null;
                dtRow = null;
                dtCoreData.DefaultView.RowFilter = string.Empty;
            }
            #endregion

            #region Validate all other fields for format and length only
            // FINAL VALIDATION RULES
            // Run through all of the fields not validated above (i.e not found in the fieldsChecked list

            // Change Log
            // Crew Reynolds    PRUN-1790   02/26/21    Modified to validate only those line level fields not found in the fieldsChecked list

            dtCoreData.DefaultView.RowFilter = "LEVEL = 'L' AND XLROW = '" + xlRow + "'";

            foreach (DataRow dtVal in dtCoreData.DefaultView.ToTable().Rows)
            {
                string fieldName = dtVal["FNAM"].ToString();
                string fieldValue = dtVal["FVAL"].ToString();
                string fieldDescr = dtVal["DESCR"].ToString();

                if (!fieldsChecked.Contains(fieldName))
                {
                    vr = new ValidationRequest(xlRow, DataLevel.DETAIL, fieldName, fieldValue, fieldDescr, T_NONE, false, false, true);
                    ValidateBasicChecks(vr);
                    vr = null;
                }
            }
            #endregion

            #region Cleanup objects
            // Cleanup
            fieldsChecked = null;
            vr = null;
            dtAccount = null;
            dtPostingKey = null;
            #endregion

            if (Options.OtherOption.ApplicationLog) LogManage.WriteAppLog("VD-" + csDETAIL + "-02: Validate line (" + xlRow.ToString() + ") complete");
        }
        #endregion

        #region VALIDATE FIELDS [Stage 1 done]
        private DataTable ValidateField(ValidationRequest vr)
        {
            // ValidateField() is used to check a user data field value for requirement, correct value, and proper format
            // It is called from all validation rules.

            // CHANGE LOG
            // Crew Reynolds    PRUN-1790               Created
            // Crew Reynolds    PRUN-1790   02/05/21    Modified to use DataRequest constructor
            // Crew Reynolds    PRUN-1790   02/26/21    Modified ValidateRequest (vr) to include bool vr.ReportErrors. Field is checked and cached but called creates the error message  

            DataTable dt = new DataTable();
            DataRequest dr;
            DataRow dtRow;

            DataView dbView = new DataView(dtCoreData); //Added By Crew On 08 Feb 2021 | Main dtCoreData copy in Temp Datatable -> Filter Condition Replace

            string validationStatus = string.Empty;
            string validationMessage = string.Empty;
            bool isHeaderLevel;

            if (vr.FieldLevel == DataLevel.HEADER)
                isHeaderLevel = true;
            else
                isHeaderLevel = false;

            try
            {
                // Does this FieldName exist in the user data?
                if (isHeaderLevel)
                {
                    // Header data
                    dbView.RowFilter = "LEVEL = 'H' AND XLROW = '" + vr.Row + "' AND FNAM = '" + vr.FieldName + "'";
                }
                else
                {
                    // Line data
                    dbView.RowFilter = "LEVEL = 'L' AND XLROW = '" + vr.Row + "' AND FNAM = '" + vr.FieldName + "'";
                }

                if (dbView.Count == 1)
                {
                    dtRow = dbView[0].Row;

                    // If it is required, check that first
                    // Crew Reynolds    PRUN-1790   03/03/21    Modifed code to properly handle the vr.ReportError parameter
                    if (vr.CheckRequired)
                    {
                        if (vr.FieldValue == "")
                        {
                            if (vr.ReportErrors)
                            {
                                validationStatus = csE + vr.StatusCode + "-" + dtRow["XLCOL"].ToString() + vr.Row.ToString();
                                validationMessage = vr.FieldDescription + " (" + vr.FieldName + ") is a required field.";
                                ErrorMessages(vr.Row, validationStatus, validationMessage, isHeaderLevel);
                            }
                            vr.FieldStatus = ReturnStatus.INVALID;
                            dr = null;
                            return null;
                        }
                        else
                        {
                            vr.FieldStatus = ReturnStatus.VALID;
                        }
                    }

                    // FieldValue is present. Validate it.
                    // Crew Reynolds    PRUN-1790   02/10/21    Modified to skip this logic if the extract key is T_NONE. Format of data will be checked instead.
                    if (vr.CheckValue && vr.FieldValue != "" && vr.ExtractKey != T_NONE)
                    {
                        // Set up a data request
                        //Crew Reynolds    PRUN-1790   02/10/21    Moved dr logic here from vr.CheckRequired logic above
                        dr = new DataRequest(vr.FieldName, vr.FieldValue, vr.ExtractKey);

                        switch (dr.ExtractKey)
                        {
                            case T_ACCOUNT: { dt = GetAccount(dr); break; }
                            case T_POSTINGKEY: { dt = GetPostingKey(dr); break; }
                            case T_COMPANYCODE: { dt = GetCompanyCode(dr); break; }
                            case T_COSTCNTR: { dt = GetCostCenter(dr); break; }
                            case T_COSTELEM: { dt = GetCostElement(dr); break; }
                            case T_CONTROLAREA: { dt = GetControllingArea(dr); break; }
                            case T_CUSTOMER: { dt = GetCustomer(dr); break; }
                            case T_VENDOR: { dt = GetVendor(dr); break; }
                            case T_CURRENCY: { dt = GetCurrency(dr); break; }
                            case T_DOCTYPE: { dt = GetDocType(dr); break; }
                            case T_PROFITCNTR: { dt = GetProfitCenter(dr); break; }
                            default:
                                {
                                    if (vr.ReportErrors)
                                    {
                                        validationStatus = csE + vr.StatusCode + "-" + dtRow["XLCOL"].ToString() + vr.Row.ToString();
                                        validationMessage = vr.StatusCode + "- Data extract (" + vr.ExtractKey + ")" + " is unknown.";
                                        ErrorMessages(vr.Row, validationStatus, validationMessage, isHeaderLevel);
                                        vr.FieldStatus = ReturnStatus.ERROR;
                                        return null;
                                    }
                                    break;
                                }
                        }

                        switch (dr.FieldStatus)
                        {
                            case ReturnStatus.VALID:
                                vr.FieldStatus = ReturnStatus.VALID; //Added By Crew On 10 Mar 2021
                                break;

                            default:
                                {
                                    if (vr.ReportErrors)
                                    {
                                        // Field data is invalid
                                        validationStatus = csE + vr.StatusCode + "-" + dtRow["XLCOL"].ToString() + vr.Row.ToString();
                                        validationMessage = vr.FieldDescription + " (" + vr.FieldName + ") value is invalid.";
                                        ErrorMessages(vr.Row, validationStatus, validationMessage, isHeaderLevel);
                                    }
                                    vr.FieldStatus = ReturnStatus.INVALID;
                                    break;
                                }
                        }
                    }
                    else
                    {
                        vr.FieldStatus = ReturnStatus.VALID;
                    }

                    //Crew Reynolds    PRUN-1790   02/10/21    Modified to check the value format when there is no data extract used
                    if (vr.ExtractKey == T_NONE)
                    {
                        // Since we didn't check the value, check the format
                        ValidateBasicChecks(vr);
                    }
                }
            }
            catch (Exception ex)
            {
                validationStatus = csE + vr.StatusCode + "-R" + vr.Row.ToString();
                validationMessage = "Data extract (" + vr.ExtractKey + ") error: " + ex.Message;
                ErrorMessages(vr.Row, validationStatus, validationMessage, isHeaderLevel);
                vr.FieldStatus = ReturnStatus.ERROR;
            }
            finally
            {
                dr = null;
                dbView.RowFilter = string.Empty;
            }

            return dt;
        }
        private void ValidateBasicChecks(ValidationRequest vr)
        {
            //Crew Reynolds    PRUN-1790   01/11/21    Modified method signature and code to use a ValidationRequest object
            //Crew Reynolds    PRUN-1790   02/09/21    No need to check the format of balnk fields

            DataRow dtRow;
            bool isHeaderRow;
            string fieldLevel = string.Empty;

            //Added By Crew On 08 Feb 2021
            string validationStatus = string.Empty;
            string validationMessage = string.Empty;
            string SAPMsgValidation = string.Empty;
            //End

            // No need to check the format of blank fields. Required field check is done in ValidateField(). 
            //Crew Reynolds    PRUN-1790   02/09/21    Added
            if (vr.FieldValue == "") return;

            // Find the map's metadata information for the FieldName
            if (vr.FieldLevel == DataLevel.HEADER)
            {
                fieldLevel = "H"; isHeaderRow = true;
            }
            else
            {
                fieldLevel = "L"; isHeaderRow = false;
            }

            DataView dbView = new DataView(dtCoreData); //Added By Crew On 08 Feb 2021 | Main dtCoreData copy in Temp Datatable -> Filter Condition Replace
            dbView.RowFilter = "LEVEL = '" + fieldLevel + "' AND XLROW = '" + vr.Row + "' AND FNAM = '" + vr.FieldName + "' AND FVAL = '" + vr.FieldValue + "'";

            if (dbView.Count == 1)
            {
                dtRow = dbView[0].Row;
            }
            else
            {
                vr.FieldStatus = ReturnStatus.NODATA;
                return;
            }

            if (dtRow["TYPE"] != DBNull.Value)
            {
                string strFieldType = dtRow["TYPE"].ToString();
                object XLValue = null;
                switch (strFieldType)
                {
                    case RFCDataType.DATE_TYPE:
                        try
                        {
                            DateTime DT = Convert.ToDateTime(vr.FieldValue);
                            vr.FieldStatus = ReturnStatus.VALID;
                        }
                        catch
                        {
                            string sysFormat = CultureInfo.CurrentCulture.DateTimeFormat.ShortDatePattern;
                            validationStatus = csE + vr.StatusCode + "-" + dtRow["XLCOL"].ToString() + vr.Row; // Changed By Crew on 08 Feb 2021 | Add csE Error Code
                            SAPMsgValidation = GetSAPMsgString("00", "065"); //Added By Crew On 08 Feb 2021
                            if (string.IsNullOrEmpty(SAPMsgValidation))
                                validationMessage = dtRow["DESCR"].ToString() + " doesn't appear to be a valid date";
                            else
                                validationMessage = CommonBase.ReplaceTextWithMsgVParam(SAPMsgValidation, sysFormat, "", "", "");
                            ErrorMessages(vr.Row, validationStatus, validationMessage, isHeaderRow);
                            vr.FieldStatus = ReturnStatus.INVALID;
                        }
                        break;
                    case RFCDataType.TIME_TYPE:
                        TimeSpan TS;
                        if (TimeSpan.TryParse(vr.FieldValue, out TS))
                        {
                            XLValue = TS;
                            vr.FieldStatus = ReturnStatus.VALID;
                        }
                        else
                        {
                            validationStatus = csE + vr.StatusCode + "-" + dtRow["XLCOL"].ToString() + vr.Row; // Changed By Crew on 08 Feb 2021 | Add csE Error Code
                            validationMessage = dtRow["DESCR"].ToString() + " doesn't appear to be a valid time";
                            ErrorMessages(vr.Row, validationStatus, validationMessage, isHeaderRow);
                            vr.FieldStatus = ReturnStatus.INVALID;
                        }
                        break;
                    case RFCDataType.BCD_TYPE:
                        decimal DecValue = 0;
                        if (decimal.TryParse(vr.FieldValue, NumberStyles.Number, FileSettings.USCultInfo.NumberFormat, out DecValue))
                        {
                            XLValue = DecValue;
                            vr.FieldStatus = ReturnStatus.VALID;
                        }
                        else
                        {
                            validationStatus = csE + vr.StatusCode + "-" + dtRow["XLCOL"].ToString() + vr.Row; // Changed By Crew on 08 Feb 2021 | Add csE Error Code
                            SAPMsgValidation = GetSAPMsgString("00", "088"); //Added By Crew On 08 Feb 2021
                            if (string.IsNullOrEmpty(SAPMsgValidation))
                                validationMessage = dtRow["DESCR"].ToString() + " doesn't appear to be a valid decimal";
                            else
                                validationMessage = SAPMsgValidation;
                            ErrorMessages(vr.Row, validationStatus, validationMessage, isHeaderRow);
                            vr.FieldStatus = ReturnStatus.INVALID;
                        }
                        break;
                    case RFCDataType.FLOAT_TYPE:
                        double DblValue = 0;
                        if (double.TryParse(vr.FieldValue, NumberStyles.Float, FileSettings.USCultInfo.NumberFormat, out DblValue))
                        {
                            XLValue = DblValue;
                            vr.FieldStatus = ReturnStatus.VALID;
                        }
                        else
                        {
                            validationStatus = csE + vr.StatusCode + "-" + dtRow["XLCOL"].ToString() + vr.Row; // Changed By Crew on 08 Feb 2021 | Add csE Error Code
                            SAPMsgValidation = GetSAPMsgString("00", "088");
                            if (string.IsNullOrEmpty(SAPMsgValidation))
                                validationMessage = dtRow["DESCR"].ToString() + " doesn't appear to be a valid float";
                            else
                                validationMessage = SAPMsgValidation;
                            ErrorMessages(vr.Row, validationStatus, validationMessage, isHeaderRow);
                            vr.FieldStatus = ReturnStatus.INVALID;
                        }
                        break;
                    case RFCDataType.NUM_TYPE:
                    case RFCDataType.INT_TYPE:
                    case RFCDataType.INT1_TYPE:
                    case RFCDataType.INT2_TYPE:
                        int intValue = 0;
                        if (int.TryParse(vr.FieldValue, NumberStyles.Integer, FileSettings.USCultInfo.NumberFormat, out intValue))
                        {
                            XLValue = intValue;
                            vr.FieldStatus = ReturnStatus.VALID;
                        }
                        else
                        {
                            validationStatus = csE + vr.StatusCode + "-" + dtRow["XLCOL"].ToString() + vr.Row;
                            validationMessage = dtRow["DESCR"].ToString() + " doesn't appear to be a valid integer";
                            ErrorMessages(vr.Row, validationStatus, validationMessage, isHeaderRow);
                            vr.FieldStatus = ReturnStatus.INVALID;
                        }
                        break;
                    default:
                        if (dtRow["LENGTH"] != null && dtRow["LENGTH"].ToString() != "" && dtRow["LENGTH"].ToString() != "0")
                        {
                            decimal FieldLength = Convert.ToDecimal(dtRow["LENGTH"], FileSettings.USCultInfo.NumberFormat);
                            if (FieldLength < vr.FieldValue.Length)
                            {
                                validationStatus = csE + vr.StatusCode + "-" + dtRow["XLCOL"].ToString() + vr.Row;
                                SAPMsgValidation = GetSAPMsgString("00", "348");
                                if (string.IsNullOrEmpty(SAPMsgValidation))
                                    validationMessage = "Value too long -Field " + vr.FieldName + " can only be " + FieldLength + " characters long";
                                else
                                    validationMessage = CommonBase.ReplaceTextWithMsgVParam(SAPMsgValidation, vr.FieldName, "", "", "");
                                ErrorMessages(vr.Row, validationStatus, validationMessage, isHeaderRow);
                                vr.FieldStatus = ReturnStatus.INVALID;
                            }
                            else
                            {
                                vr.FieldStatus = ReturnStatus.VALID;
                            }
                        }
                        break;
                }
            }

            return;
        }
        #endregion

        #region ADDITIONAL METHODS [done]
        public string ZeroPad(string value, int length)
        {
            //Crew Reynolds    PRUN-1790   02/16/21    Created
            string paddedValue = string.Empty;

            // General use method to pad 0's on the left of a string ONLY if all numeric
            if (value.All(Char.IsDigit))
            {
                paddedValue = value.PadLeft(length, '0');
            }
            else
            {
                paddedValue = value;
            }
            return paddedValue;
        }
        private void DataExtractException(Exception exception, string tableName)
        {
            if (exception.Message.IndexOf(DEDefaults.Err_TABLE_NOT_AVAILABLE) > -1)
                throw new Exception(string.Format(AppResManag.GetString(AppResID.RunTblReadGenerator003), tableName));
            else if (exception.Message.IndexOf(DEDefaults.Err_TABLE_WITHOUT_DATA) > -1)
                throw new Exception(string.Format(AppResManag.GetString(AppResID.RunTblReadGenerator004), tableName));
            else if (exception.Message.IndexOf(DEDefaults.Err_NOT_AUTHORIZED) > -1)
                throw new Exception(string.Format(AppResManag.GetString(AppResID.RunTblReadGenerator005), tableName));
            else if (exception.Message.IndexOf(DEDefaults.Err_CUSTOM_NOT_AUTHORIZED) > -1)
                throw new Exception(string.Format(AppResManag.GetString(AppResID.MsgDEGenerator003), tableName));
            else if (exception.Message.IndexOf(DEDefaults.Err_AUTH_OBJ_VALIDATION_FAIL) > -1)
                throw new Exception(string.Format(AppResManag.GetString(AppResID.MsgDEGenerator004), tableName));
            else if (exception.Message.IndexOf(DEDefaults.Err_SAPERATE_TABLES_BY_COMMA) > -1)
                throw new Exception(AppResManag.GetString(AppResID.MsgDEGenerator005));
            else if (exception.Message.IndexOf(DEDefaults.Err_JOIN_NOT_ALLOWED) > -1)
                throw new Exception(AppResManag.GetString(AppResID.MsgDEGenerator006));
            else if (exception.Message.IndexOf(DEDefaults.Err_DYNAMIC_STRUCTURE_NOT_CREATED) > -1)
                throw new Exception(AppResManag.GetString(AppResID.MsgDEGenerator007));
            else if (exception.Message.IndexOf(DEDefaults.Err_NOT_AUTHORIZED_FOR_JOIN) > -1)
                throw new Exception(string.Format(AppResManag.GetString(AppResID.MsgDEGenerator008), tableName));
            else if (exception.Message.IndexOf(DEDefaults.Err_CUSTOM_JOIN_TABLE_NOT_FOUND) > -1)
                throw new Exception(AppResManag.GetString(AppResID.MsgDEGenerator009));
            else
                throw new Exception(exception.Message);
        }

        // TODO Fix ErrorMessages to handle HEADER type

        private void ErrorMessages(int xlRow, string validationStatus, string validationMessage, bool IsHeader)
        {
            LineLevelDetail lineLevelDetail = new LineLevelDetail();
            lineLevelDetail.ValidationMessage = validationMessage;
            lineLevelDetail.ValidationStatus = validationStatus;
            lineLevelDetail.IsHeader = IsHeader;

            if (Messages != null && Messages.Count > 0)
            {
                LineLevelDetail GetValue = new LineLevelDetail();
                if (Messages.TryGetValue(xlRow, out GetValue))
                {
                    Messages[xlRow].ValidationStatus = GetValue.ValidationStatus + "\n" + validationStatus;
                    Messages[xlRow].ValidationMessage = GetValue.ValidationMessage + "\n" + validationMessage;
                }
                else
                    Messages.Add(xlRow, lineLevelDetail);
            }
            else
                Messages.Add(xlRow, lineLevelDetail);

            IsValidationFail = true;
        }

        #endregion

        #region SAP Message [done]
        private void DefaultMsgList()
        {
            try
            {
                if (!SAPCacheTables.ContainsKey(T_MSGDETAIL))
                {
                    string WhereCond = strMsgList();
                    MsgTextDetail = SAPDefaults.SAPReadTable(SAPConn, "T100", "SPRSL~ARBGB~MSGNR~TEXT", WhereCond, 0, 0, 0, true);

                    if (MsgTextDetail.Rows.Count > 0)
                        SAPCacheTables.Add(T_MSGDETAIL, MsgTextDetail);
                }
            }
            catch
            {
                //Not Handle
            }
        }

        private string strMsgList()
        {
            string msgList = string.Empty;
            msgList = "SPRSL = '" + SAPDefaults.GetInternalSAPLangCode(SAPConn.Language) + "' AND ((ARBGB = '00' AND MSGNR = '055') OR ~ "
                        + "(ARBGB = '00' AND MSGNR = '348') OR ~ "
                        + "(ARBGB = 'F5' AND MSGNR = '165') OR ~ "
                        + "(ARBGB = '00' AND MSGNR = '065') OR ~ "
                        + "(ARBGB = 'F5' AND MSGNR = '205') OR ~ "
                        + "(ARBGB = '00' AND MSGNR = '088') OR ~ "
                        + "(ARBGB = 'KI' AND MSGNR = '235') OR ~ "
                        + "(ARBGB = 'F5' AND MSGNR = '132') OR ~ "
                        + "(ARBGB = 'F5A' AND MSGNR = '100') OR ~ "
                        + "(ARBGB = '00' AND MSGNR = '088'))";

            return msgList;
        }

        private string GetSAPMsgString(string MsgID, string MsgNo)
        {
            string strMsg = string.Empty;
            try
            {
                if (SAPCacheTables.ContainsKey(T_MSGDETAIL))
                {
                    SAPCacheTables[T_MSGDETAIL].DefaultView.RowFilter = "ARBGB = '" + MsgID.Trim() + "' AND MSGNR = '" + MsgNo.Trim() + "'";
                    if (SAPCacheTables[T_MSGDETAIL].DefaultView.Count > 0)
                        strMsg = Convert.ToString(SAPCacheTables[T_MSGDETAIL].DefaultView[0]["TEXT"]);
                }

                if (strMsg == string.Empty)
                {
                    string WText = "SPRSL = '" + SAPDefaults.GetInternalSAPLangCode(SAPConn.Language) + "' AND ARBGB = '" + MsgID.Trim() + "' AND MSGNR = '" + MsgNo.Trim() + "'";
                    MsgTextDetail = SAPDefaults.SAPReadTable(SAPConn, "T100", "SPRSL~ARBGB~MSGNR~TEXT", WText, 0, 0, 0, true);

                    if (MsgTextDetail.Rows.Count > 0)
                    {
                        if (SAPCacheTables.ContainsKey(T_MSGDETAIL))
                            SAPCacheTables[T_MSGDETAIL].Merge(MsgTextDetail);

                        SAPCacheTables[T_MSGDETAIL].DefaultView.RowFilter = "ARBGB = '" + MsgID.Trim() + "' AND MSGNR = '" + MsgNo.Trim() + "'";
                        if (SAPCacheTables[T_MSGDETAIL].DefaultView.Count > 0)
                            strMsg = Convert.ToString(SAPCacheTables[T_MSGDETAIL].DefaultView[0]["TEXT"]);
                    }
                }

            }
            catch
            {
                strMsg = string.Empty;
            }
            finally
            {
                if (SAPCacheTables.ContainsKey(T_MSGDETAIL))
                    SAPCacheTables[T_MSGDETAIL].DefaultView.RowFilter = string.Empty;
            }
            return strMsg;
        }
        #endregion

        #region STAGE 1 DATA EXTRACTS AND CACHING [done]
        private DataTable GetCompanyCode(DataRequest dr)
        {
            // CHANGE LOG
            // Crew Reynolds    PRUN-1790   01/17/21    Data extract modified to check for value in cache first before running SAP call
            // Crew Reynolds    PRUN-1790	01/19/21    Modified to use new DataRequest class in method signature
            // Crew Reynolds    PRUN-1790	02/22/21    Modified to use dtExtract in order to be compliant with all of the other data extracts

            // TEST LOG 

            // DATATABLE OUTPUT
            // BUKRS,FSTVA,KTOPL,BUTXT,ORT01,LAND1,PERIV,XVALV,KALSM,XKALE

            DataTable dtExtract = new DataTable();
            string tableName = string.Empty;
            string whereCond = string.Empty;
            string fieldKey = dr.FieldValue;

            // Check for blank value
            if (dr.FieldValue == "")
            {
                dr.FieldStatus = ReturnStatus.INVALID;
                return null;
            }

            // Check the cache for the fieldValue first
            if (SAPCacheTables.ContainsKey(T_COMPANYCODE))
            {
                dtExtract = SAPCacheTables[T_COMPANYCODE];
                dtExtract.DefaultView.RowFilter = "BUKRS = '" + fieldKey + "'";

                if (dtExtract.DefaultView.Count > 0)
                {
                    //Changed By Crew On 10 Mar 2021 - If Data is Valid or Not check into Cache Table otherwise call to SAP
                    if (Convert.ToBoolean(dtExtract.DefaultView[0][F_VALIDDATA]))
                    {
                        // Found the field data in the cache
                        dr.FieldStatus = ReturnStatus.VALID;
                        return dtExtract;
                    }
                    else if (!Convert.ToBoolean(dtExtract.DefaultView[0][F_VALIDDATA]))
                    {
                        // Field data is not in cache
                        dr.FieldStatus = ReturnStatus.INVALID;
                        return null;
                    }
                }
            }

            // Not in cache. Pull data from SAP
            try
            {
                tableName = "T001";
                whereCond = "BUKRS = '" + dr.FieldValue + "'";
                dtExtract = SAPDefaults.SAPReadTable(SAPConn, tableName, "BUKRS~FSTVA~KTOPL~BUTXT~ORT01~LAND1~PERIV~XVALV", whereCond, 0, 0, 0, true);

                dtExtract.Columns.Add("KALSM", typeof(string));
                dtExtract.Columns.Add("XKALE", typeof(string));
                dtExtract.Columns.Add(F_VALIDDATA, typeof(bool));

                if (dtExtract.Rows.Count > 0)
                {
                    foreach (DataRow dbRow in dtExtract.Rows)
                    {
                        tableName = "T005";
                        whereCond = "LAND1 = '" + dbRow["LAND1"] + "'";
                        DataTable T005Data = SAPDefaults.SAPReadTable(SAPConn, tableName, "KALSM", whereCond, 0, 1, 0, true);
                        if (T005Data.Rows.Count > 0)
                            dbRow["KALSM"] = T005Data.Rows[0]["KALSM"];

                        tableName = "T009";
                        whereCond = "PERIV = '" + dbRow["PERIV"] + "'";
                        DataTable T009Data = SAPDefaults.SAPReadTable(SAPConn, tableName, "XKALE", whereCond, 0, 1, 0, true);
                        if (T009Data.Rows.Count > 0)
                            dbRow["XKALE"] = T009Data.Rows[0]["XKALE"];
                    }
                }
            }
            catch (Exception ex)
            {
                DataExtractException(ex, tableName);
                dr.FieldStatus = ReturnStatus.ERROR;
                return null;
            }

            //InValidData Add Into Main Table //Added By Crew On 10 Mar 2021
            if (dtExtract.Rows.Count > 0)
                dtExtract.Select().ToList<DataRow>().ForEach(r => r[F_VALIDDATA] = true);
            if (dtExtract.Rows.Count == 0)
            {
                DataRow dataRow = dtExtract.NewRow();
                dataRow["BUKRS"] = Convert.ToString(dr.FieldValue);
                dataRow[F_VALIDDATA] = false;
                dtExtract.Rows.Add(dataRow);
            }
            //End

            // Cache the all of the data that was pulled
            if (dtExtract.Rows.Count > 0)
            {
                // Good SAP extract - cache it
                if (SAPCacheTables.ContainsKey(T_COMPANYCODE))
                    SAPCacheTables[T_COMPANYCODE].Merge(dtExtract); // Cache already exixts. Append it.
                else
                    SAPCacheTables.Add(T_COMPANYCODE, dtExtract);   // Cache doesn't yet exist. Create it.
            }

            // Check to see if the field and value are in the cache
            if (SAPCacheTables.ContainsKey(T_COMPANYCODE))
            {
                dtExtract = SAPCacheTables[T_COMPANYCODE];
                dtExtract.DefaultView.RowFilter = "BUKRS = '" + fieldKey + "' AND " + F_VALIDDATA + " = 'True'"; //Changed By Crew On 10 Mar 2021

                if (dtExtract.DefaultView.Count > 0)
                {
                    // Found the field data in the cache
                    dr.FieldStatus = ReturnStatus.VALID;
                    return dtExtract;
                }
                else //Added By Crew On 10 Mar 2021
                {
                    // Field data is not in cache
                    dr.FieldStatus = ReturnStatus.INVALID;
                    return null;
                }
            }

            return dtExtract;
        }
        private DataTable GetCurrency(DataRequest dr)
        {
            // CHANGE LOG
            //Crew Reynolds    PRUN-1790    01/17/21    Data extract modified to check for value in cache first before running SAP call
            //Crew Reynolds    PRUN-1790	01/19/21    Modified to use new DataRequest class in method signature

            // DATATABLE OUTPUT
            // WAERS,CURRDEC

            // TEST LOG 
            //

            string tableName = string.Empty;
            string whereCond = string.Empty;
            DataTable dtExtract;
            string fieldKey = dr.FieldValue;

            // Check for blank value
            if (dr.FieldValue == "")
            {
                dr.FieldStatus = ReturnStatus.INVALID;
                return null;
            }

            // Check the cache for the fieldValue first
            if (SAPCacheTables.ContainsKey(T_CURRENCY))
            {
                dtExtract = SAPCacheTables[T_CURRENCY];
                dtExtract.DefaultView.RowFilter = dr.FieldName + " = '" + dr.FieldValue + "'";

                if (dtExtract.DefaultView.Count > 0)
                {
                    //Changed By Crew On 10 Mar 2021 - If Data is Valid or Not check into Cache Table otherwise call to SAP
                    if (Convert.ToBoolean(dtExtract.DefaultView[0][F_VALIDDATA]))
                    {
                        // Found the field data in the cache
                        dr.FieldStatus = ReturnStatus.VALID;
                        return dtExtract;
                    }
                    else if (!Convert.ToBoolean(dtExtract.DefaultView[0][F_VALIDDATA]))
                    {
                        // Field data is not in cache
                        dr.FieldStatus = ReturnStatus.INVALID;
                        return null;
                    }
                }
            }

            // Not in cache. Pull data from SAP
            try
            {
                tableName = "TCURC";
                string WhereCond = "WAERS = '" + fieldKey + "'";
                dtExtract = SAPDefaults.SAPReadTable(SAPConn, tableName, "WAERS", WhereCond, 0, 0, 0, true);
                dtExtract.Columns.Add(F_VALIDDATA, typeof(bool));
            }
            catch (Exception ex)
            {
                // Error during the SAPReadTable call
                dr.FieldStatus = ReturnStatus.ERROR;
                DataExtractException(ex, tableName);
                return null;
            }

            //InValidData Add Into Main Table //Added By Crew On 10 Mar 2021
            if (dtExtract.Rows.Count > 0)
                dtExtract.Select().ToList<DataRow>().ForEach(r => r[F_VALIDDATA] = true);
            if (dtExtract.Rows.Count == 0)
            {
                DataRow dataRow = dtExtract.NewRow();
                dataRow["WAERS"] = Convert.ToString(dr.FieldValue);
                dataRow[F_VALIDDATA] = false;
                dtExtract.Rows.Add(dataRow);
            }
            //End

            // Cache the all of the data that was pulled
            if (dtExtract.Rows.Count > 0)
            {
                // Good SAP extract - cache it
                if (SAPCacheTables.ContainsKey(T_CURRENCY))
                    SAPCacheTables[T_CURRENCY].Merge(dtExtract); // Cache already exixts. Append it.
                else
                    SAPCacheTables.Add(T_CURRENCY, dtExtract);   // Cache doesn't yet exist. Create it.
            }

            // Check to see if the field and value are in the cache
            if (SAPCacheTables.ContainsKey(T_CURRENCY))
            {
                dtExtract = SAPCacheTables[T_CURRENCY];
                dtExtract.DefaultView.RowFilter = dr.FieldName + "= '" + fieldKey + "' AND " + F_VALIDDATA + " = 'True'"; //Changed By Crew On 10 Mar 2021

                if (dtExtract.DefaultView.Count > 0)
                {
                    // Found the field data in the cache
                    dr.FieldStatus = ReturnStatus.VALID;
                    return dtExtract;
                }
                else //Added By Crew On 10 Mar 2021
                {
                    // Field data is not in cache
                    dr.FieldStatus = ReturnStatus.INVALID;
                    return null;
                }
            }

            return dtExtract;
        }
        private DataTable GetDocType(DataRequest dr)
        {
            // CHANGE LOG
            //Crew Reynolds    PRUN-1790    01/17/21    Data extract modified to check for value in cache first before running SAP call
            //Crew Reynolds    PRUN-1790	01/19/21    Modified to use new DataRequest class in method signature

            // TEST LOG 

            // DATATABLE OUTPUT
            // BLART(2/P)

            DataTable dtExtract = new DataTable();
            string tableName = string.Empty;
            string whereCond = string.Empty;
            string fieldKey = dr.FieldValue;

            // Check for blank value
            if (dr.FieldValue == "")
            {
                dr.FieldStatus = ReturnStatus.INVALID;
                return null;
            }

            // Check the cache for the fieldValue first
            if (SAPCacheTables.ContainsKey(T_DOCTYPE))
            {
                dtExtract = SAPCacheTables[T_DOCTYPE];
                dtExtract.DefaultView.RowFilter = dr.FieldName + " = '" + fieldKey + "'";

                if (dtExtract.DefaultView.Count > 0)
                {
                    //Changed By Crew On 10 Mar 2021 - If Data is Valid or Not check into Cache Table otherwise call to SAP
                    if (Convert.ToBoolean(dtExtract.DefaultView[0][F_VALIDDATA]))
                    {
                        // Found the field data in the cache
                        dr.FieldStatus = ReturnStatus.VALID;
                        return dtExtract;
                    }
                    else if (!Convert.ToBoolean(dtExtract.DefaultView[0][F_VALIDDATA]))
                    {
                        // Field data is not in cache
                        dr.FieldStatus = ReturnStatus.INVALID;
                        return null;
                    }
                }
            }

            // Not in cache. Pull data from SAP
            try
            {
                tableName = "T003";
                string WhereCond = "BLART = '" + dr.FieldValue + "'";
                dtExtract = SAPDefaults.SAPReadTable(SAPConn, tableName, "BLART", WhereCond, 0, 0, 0, true);
                dtExtract.Columns.Add(F_VALIDDATA, typeof(bool));
            }
            catch (Exception ex)
            {
                // Error during the SAPReadTable call
                dr.FieldStatus = ReturnStatus.ERROR;
                DataExtractException(ex, tableName);
                return null;
            }

            //InValidData Add Into Main Table //Added By Crew On 10 Mar 2021
            if (dtExtract.Rows.Count > 0)
                dtExtract.Select().ToList<DataRow>().ForEach(r => r[F_VALIDDATA] = true);
            if (dtExtract.Rows.Count == 0)
            {
                DataRow dataRow = dtExtract.NewRow();
                dataRow["BLART"] = Convert.ToString(dr.FieldValue);
                dataRow[F_VALIDDATA] = false;
                dtExtract.Rows.Add(dataRow);
            }
            //End

            // Cache the all of the data that was pulled
            if (dtExtract.Rows.Count > 0)
            {
                // Good SAP extract - cache it
                if (SAPCacheTables.ContainsKey(T_DOCTYPE))
                    SAPCacheTables[T_DOCTYPE].Merge(dtExtract); // Cache already exixts. Append it.
                else
                    SAPCacheTables.Add(T_DOCTYPE, dtExtract);   // Cache doesn't yet exist. Create it.
            }

            // Check to see if the field and value are in the cache
            if (SAPCacheTables.ContainsKey(T_DOCTYPE))
            {
                dtExtract = SAPCacheTables[T_DOCTYPE];
                dtExtract.DefaultView.RowFilter = dr.FieldName + "= '" + fieldKey + "' AND " + F_VALIDDATA + " = 'True'"; //Changed By Crew On 10 Mar 2021

                if (dtExtract.DefaultView.Count > 0)
                {
                    // Found the field data in the cache
                    dr.FieldStatus = ReturnStatus.VALID;
                    return dtExtract;
                }
                else //Added By Crew On 10 Mar 2021
                {
                    // Field data is not in cache
                    dr.FieldStatus = ReturnStatus.INVALID;
                    return null;
                }
            }

            return dtExtract;
        }
        private DataTable GetAccount(DataRequest dr)
        {
            // CHANGE LOG
            // Crew Reynolds    PRUN-1790   01/17/21    Data extract modified to check for value in cache first before running SAP call
            // Crew Reynolds    PRUN-1790	01/19/21    Modified to use new DataRequest class in method signature
            // Crew Reynolds    PRUN-1790	02/22/21    Modified to use dtExtract in order to be compliant with all of the other data extracts

            // DATATABLE OUTPUT - Note: pre-caching of all account numbers used on sheet
            //BUKRS,SAKNR,FSTAG,XLOEB,XSPEB,MWSKZ,XINTB,MITKZ,XMWNO,XSPEB
            DataTable dtSKA1Data = new DataTable();
            DataTable dtExtract = new DataTable();
            string strAccountNumber = string.Empty;
            string whereCond = string.Empty;
            string tableName = string.Empty;
            string fieldKey = ZeroPad(dr.FieldValue, 10);

            // Check for blank value
            if (dr.FieldValue == "")
            {
                dr.FieldStatus = ReturnStatus.INVALID;
                return null;
            }

            // Check the cache for the fieldValue first
            if (SAPCacheTables.ContainsKey(T_ACCOUNT) && !IsAccountBlockFound)
            {
                dtExtract = SAPCacheTables[T_ACCOUNT];
                dtExtract.DefaultView.RowFilter = "BUKRS = '" + currBUKRS + "' AND SAKNR = '" + fieldKey + "' AND " + F_VALIDDATA + " = 'True'";

                if (dtExtract.DefaultView.Count > 0)
                {
                    // Found the field data in the cache
                    dr.FieldStatus = ReturnStatus.VALID;
                    return dtExtract;
                }
                else //Added By Crew On 09 Mar 2021
                {
                    // Field data is not in cache
                    dr.FieldStatus = ReturnStatus.INVALID;
                    return null;
                }
            }

            //Added By Crew On 10 Mar 2021 Not in cache. Pull data from SAP
            if (IsAccountBlockFound)
            {
                try
                {
                    DataTable dataTable = AccountNumberFetch();
                    if (dataTable.Rows.Count > 0)
                        IsAccountBlockFound = false;
                }
                catch (Exception ex)
                {
                    dr.FieldStatus = ReturnStatus.ERROR;
                    return null;
                }
            }

            #region Old Code
            /*
            try
            {
                //Get distinct NEWKO (account number) values for Current line level block excel rows
                strAccountNumber = GetExcelLineLevelData("NEWKO", 10);

                tableName = "SKB1";
                whereCond = "BUKRS = '" + currBUKRS + "' AND SAKNR IN (" + strAccountNumber + ")";
                dtExtract = SAPDefaults.SAPReadTable(SAPConn, tableName, "BUKRS~SAKNR~FSTAG~XLOEB~XSPEB~MWSKZ~XINTB~MITKZ~XMWNO", whereCond, 0, 0, 0, true);
                
                if (dtExtract.Rows.Count > 0)
                {
                    tableName = "SKA1";
                    whereCond = "KTOPL = '" + currKTOPL + "' AND SAKNR IN (" + strAccountNumber + ")";
                    dtSKA1Data = SAPDefaults.SAPReadTable(SAPConn, tableName, "KTOPL~SAKNR~XLOEV~XBILK", whereCond, 0, 0, 0, true);

                    //Merge Child Table columns into Master Table columns
                    dtExtract.Columns.Add("KTOPL", typeof(string));
                    dtExtract.Columns.Add("XLOEV", typeof(string));
                    dtExtract.Columns.Add("XBILK", typeof(string));

                    // Run through all SKB1 rows and attach the SKA1 fields
                    foreach (DataRow drSKB1Data in dtExtract.Rows)
                    {
                        foreach (DataRow drSKA1Data in dtSKA1Data.Rows)
                        {
                            if ((string)drSKA1Data["KTOPL"] == currKTOPL && (string)drSKB1Data["SAKNR"] == (string)drSKA1Data["SAKNR"])  // Crew Reynolds    PRUN-1790   02/19/21    conversion to string not needed. Removed.
                            {
                                drSKB1Data["KTOPL"] = currKTOPL;
                                drSKB1Data["XLOEV"] = drSKA1Data["XLOEV"];
                                drSKB1Data["XBILK"] = drSKA1Data["XBILK"];
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // Error during the SAPReadTable call
                dtExtract = null;
                dtSKA1Data = null;
                dr.FieldStatus = ReturnStatus.ERROR;
                DataExtractException(ex, tableName);
            }

            // Cache the all of the data that was pulled
            if (dtExtract.Rows.Count > 0)
            {
                // Good SAP extract - cache it
                if (SAPCacheTables.ContainsKey(T_ACCOUNT))
                    SAPCacheTables[T_ACCOUNT].Merge(dtExtract); // Cache already exixts. Append it.
                else
                    SAPCacheTables.Add(T_ACCOUNT, dtExtract);   // Cache doesn't yet exist. Create it.

                // Crew Reynolds    PRUN-1790   03/03/21    For consideration: Add PrimaryKey and 
                //DataColumn[] columns = new DataColumn[2];
                //columns[0] = dtExtract.Columns["BUKRS"];
                //columns[1] = dtExtract.Columns["SAKNR"];
                //dtExtract.PrimaryKey = columns;


                //Using the RowFilter property, you can specify subsets of rows based on their column values. 
                //For details about valid expressions for the RowFilter property, see the reference information
                //for the Expression property of the DataColumn class. If you want to return the results of a
                //particular query on the data, as opposed to providing a dynamic view of a subset of the data,
                //to achieve the best performance, use the Find or FindRows methods of the DataView rather than
                //setting the RowFilter property.Setting the RowFilter property causes the index for the data to
                //be rebuilt, adding overhead to your application, and decreasing performance.The RowFilter property
                //is best used in a data-bound application where a bound control displays filtered results.The
                //Find and FindRows methods leverage the current index without requiring the index to be rebuilt.
                //
                //DataTable.Rows.Findreturns only a single row. Essentially, when you specify the primary key, a binary tree is created" - 
                //from here msdn.microsoft.com/en-us/library/dd364983.aspx
            }
            */
            #endregion

            // Check to see if the field and value are in the cache
            if (SAPCacheTables.ContainsKey(T_ACCOUNT) && !IsAccountBlockFound)
            {
                dtExtract = SAPCacheTables[T_ACCOUNT];
                dtExtract.DefaultView.RowFilter = "BUKRS = '" + currBUKRS + "' AND SAKNR = '" + fieldKey + "' AND " + F_VALIDDATA + " = 'True'"; //Changed By Crew On 10 Mar 2021

                if (dtExtract.DefaultView.Count > 0)
                {
                    // Found the field data in the cache
                    dr.FieldStatus = ReturnStatus.VALID;
                    return dtExtract;
                }
                else //Added By Crew On 09 Mar 2021
                {
                    // Field data is not in cache
                    dr.FieldStatus = ReturnStatus.INVALID;
                    dtExtract = null;
                }
            }
            return dtExtract;
        }
        private DataTable AccountNumberFetch()
        {
            string tableName = string.Empty;
            DataTable dt = new DataTable();
            DataTable OptTable = new DataTable();
            DataTable extractData = new DataTable();
            try
            {
                //Get Field Value from dtCoreData
                dtCoreData.DefaultView.RowFilter = "FNAM = 'NEWKO'";
                if (dtCoreData.DefaultView.Count > 0)
                {
                    dt = dtCoreData.DefaultView.ToTable(true, "FVAL");
                    string[] _SQLArrData = dt.AsEnumerable().Select(r => r.Field<string>("FVAL")).ToArray();
                    OptTable.Columns.Add(new DataColumn("NEWKO", Type.GetType("System.String")));

                    int TmpLastSQLArrPos = 0;
                    DataRow dbOptRow = null;
                    int SQLLen = 0;
                    int RowPos = 0;
                    int _LastSQLArrPos = 0;
                    DataTable dtExtract = new DataTable();
                    DataTable dtSKA1Data = new DataTable();
                    string strAccountNumber = string.Empty;
                    DataTable CacheTable = new DataTable();

                Next:
                    SQLLen = 0;
                    RowPos = 0;
                    dtExtract = new DataTable();
                    dtSKA1Data = new DataTable();
                    strAccountNumber = string.Empty;
                    CacheTable = new DataTable();

                    if (OptTable.Rows.Count > 0)
                        OptTable.Rows.Clear();

                    if (_LastSQLArrPos > 0)
                        TmpLastSQLArrPos = _LastSQLArrPos;

                    for (int k = TmpLastSQLArrPos; k < _SQLArrData.Length; k++)
                    {
                        dbOptRow = OptTable.NewRow();

                        if ((SQLLen + _SQLArrData[k].Length) > _ACSLimit)
                            break;

                        dbOptRow[0] = ZeroPad((string)_SQLArrData[k], 10);

                        if (RowPos == 0)
                            OptTable.Rows.Add(dbOptRow);
                        else
                            OptTable.Rows.InsertAt(dbOptRow, RowPos);

                        RowPos++;
                        SQLLen += dbOptRow[0].ToString().Length;
                        _LastSQLArrPos++;
                    }
                    OptTable.AcceptChanges();

                    if (SAPCacheTables.ContainsKey(T_ACCOUNT))
                        CacheTable = SAPCacheTables[T_ACCOUNT];

                    foreach (DataRow dr in OptTable.DefaultView.ToTable(true, "NEWKO").Rows)   // Pulls only distinct rows for NEWKO
                    {
                        string value = ZeroPad((string)dr["NEWKO"], 10);

                        //Remove Already Added Account Number 
                        if (SAPCacheTables.ContainsKey(T_ACCOUNT))
                        {
                            CacheTable.DefaultView.RowFilter = "BUKRS = '" + currBUKRS + "' AND SAKNR = '" + value + "'";
                            if (CacheTable.DefaultView.Count > 0)
                                continue;
                        }

                        if (strAccountNumber == string.Empty)
                            strAccountNumber = "'" + value + "'";
                        else
                            strAccountNumber += ",~" + "'" + value + "'";
                    }

                    if (strAccountNumber != string.Empty)
                    {
                        tableName = "SKB1";
                        string whereCond = "BUKRS = '" + currBUKRS + "' AND SAKNR IN (" + strAccountNumber + ")";
                        dtExtract = SAPDefaults.SAPReadTable(SAPConn, tableName, "BUKRS~SAKNR~FSTAG~XLOEB~XSPEB~MWSKZ~XINTB~MITKZ~XMWNO", whereCond, 0, 0, 0, true);

                        //Merge Child Table columns into Master Table columns
                        dtExtract.Columns.Add("KTOPL", typeof(string));
                        dtExtract.Columns.Add("XLOEV", typeof(string));
                        dtExtract.Columns.Add("XBILK", typeof(string));
                        dtExtract.Columns.Add(F_VALIDDATA, typeof(bool));

                        if (dtExtract.Rows.Count > 0)
                        {
                            tableName = "SKA1";
                            whereCond = "KTOPL = '" + currKTOPL + "' AND SAKNR IN (" + strAccountNumber + ")";
                            dtSKA1Data = SAPDefaults.SAPReadTable(SAPConn, tableName, "KTOPL~SAKNR~XLOEV~XBILK", whereCond, 0, 0, 0, true);

                            // Run through all SKB1 rows and attach the SKA1 fields
                            foreach (DataRow drSKB1Data in dtExtract.Rows)
                            {
                                foreach (DataRow drSKA1Data in dtSKA1Data.Rows)
                                {
                                    if ((string)drSKA1Data["KTOPL"] == currKTOPL && (string)drSKB1Data["SAKNR"] == (string)drSKA1Data["SAKNR"])  // Crew Reynolds    PRUN-1790   02/19/21    conversion to string not needed. Removed.
                                    {
                                        drSKB1Data["KTOPL"] = currKTOPL;
                                        drSKB1Data["XLOEV"] = drSKA1Data["XLOEV"];
                                        drSKB1Data["XBILK"] = drSKA1Data["XBILK"];
                                    }
                                }
                            }
                        }

                        //InValidData Add Into Main Table
                        if (dtExtract.Rows.Count > 0)
                            dtExtract.Select().ToList<DataRow>().ForEach(r => r[F_VALIDDATA] = true);
                        string[] array = strAccountNumber.Replace("'", "").Replace(",", "").Split('~').ToArray();
                        var CheckWithArrayList = array.Select(x => new
                        {
                            SAKNR = x,
                            ValidData = dtExtract.AsEnumerable().Any(y => y.Field<string>("SAKNR") == x)
                        });
                        foreach (var item in CheckWithArrayList)
                        {
                            if (!item.ValidData)
                            {
                                DataRow dataRow = dtExtract.NewRow();
                                dataRow["SAKNR"] = Convert.ToString(item.SAKNR);
                                dataRow[F_VALIDDATA] = Convert.ToBoolean(item.ValidData);
                                dataRow["BUKRS"] = currBUKRS;
                                dtExtract.Rows.Add(dataRow);
                            }
                        }
                        //End

                        // Cache the all of the data that was pulled
                        if (dtExtract.Rows.Count > 0)
                        {
                            // Good SAP extract - cache it
                            if (SAPCacheTables.ContainsKey(T_ACCOUNT))
                                SAPCacheTables[T_ACCOUNT].Merge(dtExtract); // Cache already exixts. Append it.
                            else
                                SAPCacheTables.Add(T_ACCOUNT, dtExtract);   // Cache doesn't yet exist. Create it.
                        }

                        if (_LastSQLArrPos != _SQLArrData.Length)
                            goto Next;
                    }
                }

                if (SAPCacheTables.ContainsKey(T_ACCOUNT))
                    return extractData = SAPCacheTables[T_ACCOUNT];
            }
            catch (Exception ex)
            {
                DataExtractException(ex, tableName);
                throw ex;
            }
            finally
            {
                dt = null;
                OptTable = null;
            }
            return extractData;
        }
        private DataTable GetPostingKey(DataRequest dr)
        {
            // CHANGE LOG
            // Crew Reynolds    PRUN-1790   01/17/21    Data extract modified to check for value in cache first before running SAP call
            // Crew Reynolds    PRUN-1790	01/19/21    Modified to use new DataRequest class in method signature
            // Crew Reynolds    PRUN-1790	01/2821     Hard-coded "BSCHL" due to vr.FieldName coming in as "NEWBS". Same field.

            // DATATABLE OUTPUT
            // BSCHL,SHKZG,KOART,FAUS1,FAUS2

            // TEST LOG 
            //

            DataTable dtExtract = new DataTable();
            string tableName = string.Empty;
            string whereCond = string.Empty;
            string fieldKey = dr.FieldValue;

            // Check for blank value
            if (dr.FieldValue == "")
            {
                dr.FieldStatus = ReturnStatus.INVALID;
                return null;
            }

            // Check the cache for the fieldValue first
            try
            {
                if (SAPCacheTables.ContainsKey(T_POSTINGKEY) && !IsPostKeyBlockFound)
                {
                    dtExtract = SAPCacheTables[T_POSTINGKEY];
                    dtExtract.DefaultView.RowFilter = "BSCHL = '" + fieldKey + "' AND " + F_VALIDDATA + " = 'True'";

                    if (dtExtract.DefaultView.Count > 0)
                    {
                        // Found the field data in the cache
                        dr.FieldStatus = ReturnStatus.VALID;
                        return dtExtract;
                    }
                    else //Added By Crew On 09 Mar 2021
                    {
                        // Field data is not in cache
                        dr.FieldStatus = ReturnStatus.INVALID;
                        return null;
                    }
                }
            }
            catch { };

            //Added By Crew On 10 Mar 2021 Not in cache. Pull data from SAP
            if (IsPostKeyBlockFound)
            {
                try
                {
                    DataTable dataTable = PostingKeyFetch();
                    if (dataTable.Rows.Count > 0)
                        IsPostKeyBlockFound = false;
                }
                catch (Exception ex)
                {
                    dr.FieldStatus = ReturnStatus.ERROR;
                    return null;
                }
            }

            #region Old Code
            // Not in cache. Pull data from SAP
            /*
            try
            {
                tableName = "TBSL";
                string WhereCond = "BSCHL = '" + fieldKey + "'";
                dtExtract = SAPDefaults.SAPReadTable(SAPConn, tableName, "BSCHL~SHKZG~KOART~FAUS1~FAUS2", WhereCond, 0, 0, 0, true);
            }
            catch (Exception ex)
            {
                // Error during the SAPReadTable call
                dr.FieldStatus = ReturnStatus.ERROR;
                DataExtractException(ex, tableName);
                dtExtract = null;
            }

            // Cache the all of the data that was pulled
            if (dtExtract.Rows.Count > 0)
            {
                // Good SAP extract - cache it
                if (SAPCacheTables.ContainsKey(T_POSTINGKEY))
                    SAPCacheTables[T_POSTINGKEY].Merge(dtExtract); // Cache already exixts. Append it.
                else
                    SAPCacheTables.Add(T_POSTINGKEY, dtExtract);   // Cache doesn't yet exist. Create it.
            }
            */
            #endregion

            // Check to see if the field and value are in the cache
            if (SAPCacheTables.ContainsKey(T_POSTINGKEY) && !IsPostKeyBlockFound)
            {
                dtExtract = SAPCacheTables[T_POSTINGKEY];
                dtExtract.DefaultView.RowFilter = "BSCHL = '" + fieldKey + "' AND " + F_VALIDDATA + " = 'True'";

                if (dtExtract.DefaultView.Count > 0)
                {
                    // Value found the field data in the cache
                    dr.FieldStatus = ReturnStatus.VALID;
                    return dtExtract;
                }
                else
                {
                    // Value NOT found the field data in the cache
                    dr.FieldStatus = ReturnStatus.INVALID;
                    return null;
                }
            }

            return dtExtract;
        }
        private DataTable PostingKeyFetch()
        {
            string tableName = string.Empty;
            DataTable dt = new DataTable();
            DataTable OptTable = new DataTable();
            DataTable extractData = new DataTable();

            try
            {
                //Get Field Value from dtCoreData
                dtCoreData.DefaultView.RowFilter = "FNAM = 'NEWBS'";
                if (dtCoreData.DefaultView.Count > 0)
                {
                    dt = dtCoreData.DefaultView.ToTable(true, "FVAL");
                    string[] _SQLArrData = dt.AsEnumerable().Select(r => r.Field<string>("FVAL")).ToArray();
                    OptTable.Columns.Add(new DataColumn("NEWBS", Type.GetType("System.String")));

                    int TmpLastSQLArrPos = 0;
                    DataRow dbOptRow = null;
                    int SQLLen = 0;
                    int RowPos = 0;
                    int _LastSQLArrPos = 0;
                    DataTable dtExtract = new DataTable();
                    string strPostingKey = string.Empty;
                    DataTable CacheTable = new DataTable();

                Next:
                    SQLLen = 0;
                    RowPos = 0;
                    dtExtract = new DataTable();
                    strPostingKey = string.Empty;
                    CacheTable = new DataTable();

                    if (OptTable.Rows.Count > 0)
                        OptTable.Rows.Clear();

                    if (_LastSQLArrPos > 0)
                        TmpLastSQLArrPos = _LastSQLArrPos;

                    for (int k = TmpLastSQLArrPos; k < _SQLArrData.Length; k++)
                    {
                        dbOptRow = OptTable.NewRow();

                        if ((SQLLen + _SQLArrData[k].Length) > _ACSLimit)
                            break;

                        dbOptRow[0] = _SQLArrData[k];

                        if (RowPos == 0)
                            OptTable.Rows.Add(dbOptRow);
                        else
                            OptTable.Rows.InsertAt(dbOptRow, RowPos);

                        RowPos++;
                        SQLLen += dbOptRow[0].ToString().Length;
                        _LastSQLArrPos++;
                    }
                    OptTable.AcceptChanges();

                    if (SAPCacheTables.ContainsKey(T_POSTINGKEY))
                        CacheTable = SAPCacheTables[T_POSTINGKEY];

                    foreach (DataRow dr in OptTable.DefaultView.ToTable(true, "NEWBS").Rows)   // Pulls only distinct rows for NEWBS
                    {
                        string value = (string)dr["NEWBS"];

                        //Remove Already Added Posting Key
                        if (SAPCacheTables.ContainsKey(T_POSTINGKEY))
                        {
                            CacheTable.DefaultView.RowFilter = "BSCHL = '" + value + "'";
                            if (CacheTable.DefaultView.Count > 0)
                                continue;
                        }

                        if (strPostingKey == string.Empty)
                            strPostingKey = "'" + value + "'";
                        else
                            strPostingKey += ",~" + "'" + value + "'";
                    }

                    if (strPostingKey != string.Empty)
                    {
                        tableName = "TBSL";
                        string WhereCond = "BSCHL IN (" + strPostingKey + ")";
                        dtExtract = SAPDefaults.SAPReadTable(SAPConn, tableName, "BSCHL~SHKZG~KOART~FAUS1~FAUS2", WhereCond, 0, 0, 0, true);

                        //Merge Child Table columns into Master Table columns
                        dtExtract.Columns.Add(F_VALIDDATA, typeof(bool));

                        //InValidData Add Into Main Table
                        if (dtExtract.Rows.Count > 0)
                            dtExtract.Select().ToList<DataRow>().ForEach(r => r[F_VALIDDATA] = true);
                        string[] array = strPostingKey.Replace("'", "").Replace(",", "").Split('~').ToArray();
                        var CheckWithArrayList = array.Select(x => new
                        {
                            BSCHL = x,
                            ValidData = dtExtract.AsEnumerable().Any(y => y.Field<string>("BSCHL") == x)
                        });
                        foreach (var item in CheckWithArrayList)
                        {
                            if (!item.ValidData)
                            {
                                DataRow dataRow = dtExtract.NewRow();
                                dataRow["BSCHL"] = Convert.ToString(item.BSCHL);
                                dataRow[F_VALIDDATA] = Convert.ToBoolean(item.ValidData);
                                dtExtract.Rows.Add(dataRow);
                            }
                        }
                        //End

                        // Cache the all of the data that was pulled
                        if (dtExtract.Rows.Count > 0)
                        {
                            // Good SAP extract - cache it
                            if (SAPCacheTables.ContainsKey(T_POSTINGKEY))
                                SAPCacheTables[T_POSTINGKEY].Merge(dtExtract); // Cache already exixts. Append it.
                            else
                                SAPCacheTables.Add(T_POSTINGKEY, dtExtract);   // Cache doesn't yet exist. Create it.
                        }

                        if (_LastSQLArrPos != _SQLArrData.Length)
                            goto Next;
                    }
                }

                if (SAPCacheTables.ContainsKey(T_POSTINGKEY))
                    return extractData = SAPCacheTables[T_POSTINGKEY];
            }
            catch (Exception ex)
            {
                DataExtractException(ex, tableName);
                throw ex;
            }
            finally
            {
                dt = null;
                OptTable = null;
            }
            return extractData;
        }
        private DataTable GetControllingArea(DataRequest dr)
        {
            // CHANGE LOG
            // Crew Reynolds    PRUN-1790    01/17/21    Data extract modified to check for value in cache first before running SAP call
            // Crew Reynolds    PRUN-1790    01/19/21    Modified to use new DataRequest class in method signature

            // DATATABLE OUTPUT - Note: pre-caching of all account numbers used on sheet

            DataTable dtExtract = new DataTable();
            string tableName = string.Empty;
            string strControllingAreas = string.Empty;
            string fieldKey = dr.FieldValue;

            // Check for blank value
            if (dr.FieldValue == "")
            {
                dr.FieldStatus = ReturnStatus.INVALID;
                return null;
            }

            // Check the cache for the fieldValue first
            if (SAPCacheTables.ContainsKey(T_CONTROLAREA) && !IsControllingAreaFound)
            {
                dtExtract = SAPCacheTables[T_CONTROLAREA];
                dtExtract.DefaultView.RowFilter = "KOKRS = '" + fieldKey + "' AND " + F_VALIDDATA + " = 'True'";

                if (dtExtract.DefaultView.Count > 0)
                {
                    // Found the field data in the cache
                    dr.FieldStatus = ReturnStatus.VALID;
                    return dtExtract;
                }
                else //Added By Crew On 09 Mar 2021
                {
                    // Field data is not in cache
                    dr.FieldStatus = ReturnStatus.INVALID;
                    return null;
                }
            }

            //Added By Crew On 10 Mar 2021 Not in cache. Pull data from SAP
            if (IsControllingAreaFound)
            {
                try
                {
                    DataTable dataTable = ControllingAreaFetch();
                    if (dataTable.Rows.Count > 0)
                        IsControllingAreaFound = false;
                }
                catch (Exception ex)
                {
                    dr.FieldStatus = ReturnStatus.ERROR;
                    return null;
                }
            }

            #region Old Code
            // Not in cache. Pull data from SAP
            /*
            try
            {
                //Get distinct controlling areas (KOKRS) values for current line level block excel rows
                strControllingAreas = GetExcelLineLevelData("KOKRS", 0);  // No padding for KOKRS

                tableName = "TKA01";
                string WhereCond = "KOKRS IN (" + strControllingAreas + ")";
                dtExtract = SAPDefaults.SAPReadTable(SAPConn, tableName, "KOKRS~KTOPL~KOKFI~ERKRS", WhereCond, 0, 0, 0, true);

                if (dtExtract.Rows.Count > 0)
                {
                    tableName = "TKA00";
                    WhereCond = "KOKRS IN (" + strControllingAreas + ")";
                    DataTable TKA00Data = SAPDefaults.SAPReadTable(SAPConn, tableName, "RKBUK~GJAHR~KOKRS", WhereCond, 0, 0, 0, true);

                    //Merge Child Table columns into Master Table columns
                    TKA00Data.Columns.Add("RKBUK", typeof(string));
                    TKA00Data.Columns.Add("GJAHR", typeof(string));

                    foreach (DataRow drCARow in dtExtract.Rows)
                    {
                        foreach (DataRow drTKA00Row in TKA00Data.Rows)
                        {
                            if (drCARow["KOKRS"] == drTKA00Row["KOKRS"])
                            {
                                drCARow["RKBUK"] = drTKA00Row["RKBUK"];
                                drCARow["GJAHR"] = drTKA00Row["GJAHR"];
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // Error during the SAPReadTable call
                dtExtract = null;
                dr.FieldStatus = ReturnStatus.ERROR;
                DataExtractException(ex, tableName);
            }

            // Cache the all of the data that was pulled
            if (dtExtract.Rows.Count > 0)
            {
                // Good SAP extract - cache it
                if (SAPCacheTables.ContainsKey(T_CONTROLAREA))
                    SAPCacheTables[T_CONTROLAREA].Merge(dtExtract); // Cache already exixts. Append it.
                else
                    SAPCacheTables.Add(T_CONTROLAREA, dtExtract);   // Cache doesn't yet exist. Create it.

            }
            */
            #endregion

            // Check to see if the field and value are in the cache
            if (SAPCacheTables.ContainsKey(T_CONTROLAREA) && !IsControllingAreaFound)
            {
                dtExtract = SAPCacheTables[T_CONTROLAREA];
                dtExtract.DefaultView.RowFilter = "KOKRS = '" + fieldKey + "' AND " + F_VALIDDATA + " = 'True'";

                if (dtExtract.DefaultView.Count > 0)
                {
                    // Found the field data in the cache
                    dr.FieldStatus = ReturnStatus.VALID;
                    return dtExtract;
                }
                else //Added By Crew On 09 Mar 2021
                {
                    // Field data is not in cache
                    dr.FieldStatus = ReturnStatus.INVALID;
                    return null;
                }
            }

            return dtExtract;
        }
        private DataTable ControllingAreaFetch()
        {
            string tableName = string.Empty;
            DataTable dt = new DataTable();
            DataTable OptTable = new DataTable();
            DataTable extractData = new DataTable();
            try
            {
                //Get Field Value from dtCoreData
                dtCoreData.DefaultView.RowFilter = "FNAM = 'KOKRS'";
                if (dtCoreData.DefaultView.Count > 0)
                {
                    dt = dtCoreData.DefaultView.ToTable(true, "FVAL");
                    string[] _SQLArrData = dt.AsEnumerable().Select(r => r.Field<string>("FVAL")).ToArray();
                    OptTable.Columns.Add(new DataColumn("KOKRS", Type.GetType("System.String")));

                    int TmpLastSQLArrPos = 0;
                    DataRow dbOptRow = null;
                    int SQLLen = 0;
                    int RowPos = 0;
                    int _LastSQLArrPos = 0;
                    DataTable dtExtract = new DataTable();
                    DataTable TKA00Data = new DataTable();
                    string strControllingAreas = string.Empty;
                    DataTable CacheTable = new DataTable();

                Next:
                    SQLLen = 0;
                    RowPos = 0;
                    dtExtract = new DataTable();
                    TKA00Data = new DataTable();
                    strControllingAreas = string.Empty;
                    CacheTable = new DataTable();

                    if (OptTable.Rows.Count > 0)
                        OptTable.Rows.Clear();

                    if (_LastSQLArrPos > 0)
                        TmpLastSQLArrPos = _LastSQLArrPos;

                    for (int k = TmpLastSQLArrPos; k < _SQLArrData.Length; k++)
                    {
                        dbOptRow = OptTable.NewRow();

                        if ((SQLLen + _SQLArrData[k].Length) > _ACSLimit)
                            break;

                        dbOptRow[0] = _SQLArrData[k];

                        if (RowPos == 0)
                            OptTable.Rows.Add(dbOptRow);
                        else
                            OptTable.Rows.InsertAt(dbOptRow, RowPos);

                        RowPos++;
                        SQLLen += dbOptRow[0].ToString().Length;
                        _LastSQLArrPos++;
                    }
                    OptTable.AcceptChanges();

                    if (SAPCacheTables.ContainsKey(T_CONTROLAREA))
                        CacheTable = SAPCacheTables[T_CONTROLAREA];

                    foreach (DataRow dr in OptTable.DefaultView.ToTable(true, "KOKRS").Rows)   // Pulls only distinct rows for KOKRS
                    {
                        string value = (string)dr["KOKRS"];

                        //Remove Already Added Controlling Area
                        if (SAPCacheTables.ContainsKey(T_CONTROLAREA))
                        {
                            CacheTable.DefaultView.RowFilter = "KOKRS = '" + value + "'";
                            if (CacheTable.DefaultView.Count > 0)
                                continue;
                        }

                        if (strControllingAreas == string.Empty)
                            strControllingAreas = "'" + value + "'";
                        else
                            strControllingAreas += ",~" + "'" + value + "'";
                    }

                    if (strControllingAreas != string.Empty)
                    {
                        tableName = "TKA01";
                        string WhereCond = "KOKRS IN (" + strControllingAreas + ")";
                        dtExtract = SAPDefaults.SAPReadTable(SAPConn, tableName, "KOKRS~KTOPL~KOKFI~ERKRS", WhereCond, 0, 0, 0, true);

                        //Merge Child Table columns into Master Table columns
                        dtExtract.Columns.Add("RKBUK", typeof(string));
                        dtExtract.Columns.Add("GJAHR", typeof(string));
                        dtExtract.Columns.Add(F_VALIDDATA, typeof(bool));

                        if (dtExtract.Rows.Count > 0)
                        {
                            tableName = "TKA00";
                            WhereCond = "KOKRS IN (" + strControllingAreas + ")";
                            TKA00Data = SAPDefaults.SAPReadTable(SAPConn, tableName, "RKBUK~GJAHR~KOKRS", WhereCond, 0, 0, 0, true);

                            // Run through all SKB1 rows and attach the SKA1 fields
                            foreach (DataRow drCARow in dtExtract.Rows)
                            {
                                foreach (DataRow drTKA00Row in TKA00Data.Rows)
                                {
                                    if (drCARow["KOKRS"] == drTKA00Row["KOKRS"])
                                    {
                                        drCARow["RKBUK"] = drTKA00Row["RKBUK"];
                                        drCARow["GJAHR"] = drTKA00Row["GJAHR"];
                                    }
                                }
                            }
                        }

                        //InValidData Add Into Main Table
                        if (dtExtract.Rows.Count > 0)
                            dtExtract.Select().ToList<DataRow>().ForEach(r => r[F_VALIDDATA] = true);
                        string[] array = strControllingAreas.Replace("'", "").Replace(",", "").Split('~').ToArray();
                        var CheckWithArrayList = array.Select(x => new
                        {
                            KOKRS = x,
                            ValidData = dtExtract.AsEnumerable().Any(y => y.Field<string>("KOKRS") == x)
                        });
                        foreach (var item in CheckWithArrayList)
                        {
                            if (!item.ValidData)
                            {
                                DataRow dataRow = dtExtract.NewRow();
                                dataRow["KOKRS"] = Convert.ToString(item.KOKRS);
                                dataRow[F_VALIDDATA] = Convert.ToBoolean(item.ValidData);
                                dtExtract.Rows.Add(dataRow);
                            }
                        }
                        //End

                        // Cache the all of the data that was pulled
                        if (dtExtract.Rows.Count > 0)
                        {
                            // Good SAP extract - cache it
                            if (SAPCacheTables.ContainsKey(T_CONTROLAREA))
                                SAPCacheTables[T_CONTROLAREA].Merge(dtExtract); // Cache already exixts. Append it.
                            else
                                SAPCacheTables.Add(T_CONTROLAREA, dtExtract);   // Cache doesn't yet exist. Create it.
                        }

                        if (_LastSQLArrPos != _SQLArrData.Length)
                            goto Next;
                    }
                }

                if (SAPCacheTables.ContainsKey(T_CONTROLAREA))
                    return extractData = SAPCacheTables[T_CONTROLAREA];
            }
            catch (Exception ex)
            {
                DataExtractException(ex, tableName);
                throw ex;
            }
            finally
            {
                dt = null;
                OptTable = null;
            }
            return extractData;
        }
        private DataTable GetCustomer(DataRequest dr)
        {
            // CHANGE LOG
            // Crew Reynolds    PRUN-1790   01/17/21    Data extract modified to check for value in cache first before running SAP call
            // Crew Reynolds    PRUN-1790	01/19/21    Modified to use new DataRequest class in method signature

            // DATATABLE OUTPUT - Note: pre-caching of all account numbers used on sheet/block

            DataTable dtExtract = new DataTable();
            DataTable dtKNA1Data = new DataTable();
            string tableName = string.Empty;
            string strCustomer = string.Empty;
            string fieldKey = ZeroPad(dr.FieldValue, 10);

            // Check for blank value
            if (dr.FieldValue == "")
            {
                dr.FieldStatus = ReturnStatus.INVALID;
                return null;
            }

            // Check the cache for the fieldValue first
            if (SAPCacheTables.ContainsKey(T_CUSTOMER) && !IsCustomerFound)
            {
                dtExtract = SAPCacheTables[T_CUSTOMER];
                dtExtract.DefaultView.RowFilter = "BUKRS = '" + currBUKRS + "' AND " + dr.FieldName + " = '" + fieldKey + "' AND " + F_VALIDDATA + " = 'True'";

                if (dtExtract.DefaultView.Count > 0)
                {
                    // Found the field data in the cache
                    dr.FieldStatus = ReturnStatus.VALID;
                    return dtExtract;
                }
                else //Added By Crew On 12 Mar 2021
                {
                    // Field data is not in cache
                    dr.FieldStatus = ReturnStatus.INVALID;
                    return null;
                }
            }

            //Added By Crew On 12 Mar 2021 Not in cache. Pull data from SAP
            if (IsCustomerFound)
            {
                try
                {
                    DataTable dataTable = CustomerFetch();
                    if (dataTable.Rows.Count > 0)
                        IsCustomerFound = false;
                }
                catch (Exception ex)
                {
                    dr.FieldStatus = ReturnStatus.ERROR;
                    return null;
                }
            }

            #region Old Code
            // Not in cache. Pull data from SAP
            /*
            try
            {
                //Get distinct values for Current line level block excel rows
                strCustomer = GetExcelLineLevelData("KUNNR", 10);
                tableName = "KNB1";

                string WhereCond = "KUNNR IN (" + strCustomer + ") AND BUKRS ='" + currBUKRS + "'";
                dtExtract = SAPDefaults.SAPReadTable(SAPConn, tableName, "BUKRS~KUNNR~AKONT~SPERR~LOEVM", WhereCond, 0, 0, 0, true);

                if (dtExtract.Rows.Count > 0)
                {
                    tableName = "KNA1";
                    WhereCond = "KUNNR IN (" + strCustomer + ")";

                    dtKNA1Data = SAPDefaults.SAPReadTable(SAPConn, tableName, "SPERR~LOEVM~LAND1~KUNNR", WhereCond, 0, 0, 0, true);

                    //Merge Child Table columns into Master Table columns
                    dtExtract.Columns.Add("KNA1_LAND1", typeof(string));
                    dtExtract.Columns.Add("KNA1_SPERR", typeof(string));
                    dtExtract.Columns.Add("KNA1_LOEVM", typeof(string));

                    foreach (DataRow drCARow in dtExtract.Rows)
                    {
                        foreach (DataRow drKNA1Row in dtKNA1Data.Rows)
                        {
                            if (Convert.ToString(drCARow["KUNNR"]) == Convert.ToString(drKNA1Row["KUNNR"]))
                            {
                                drCARow["KNA1_LAND1"] = drKNA1Row["LAND1"];
                                drCARow["KNA1_SPERR"] = drKNA1Row["SPERR"];
                                drCARow["KNA1_LOEVM"] = drKNA1Row["LOEVM"];
                                break;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // Error during the SAPReadTable call
                dtExtract = null;
                dr.FieldStatus = ReturnStatus.ERROR;
                DataExtractException(ex, tableName);
            }

            // Cache the all of the data that was pulled
            dtExtract = dtKNA1Data;
            if (dtExtract.Rows.Count > 0)
            {
                // Good SAP extract - cache it
                if (SAPCacheTables.ContainsKey(T_CUSTOMER))
                    SAPCacheTables[T_CUSTOMER].Merge(dtExtract); // Cache already exixts. Append it.
                else
                    SAPCacheTables.Add(T_CUSTOMER, dtExtract);   // Cache doesn't yet exist. Create it.
            }
            */
            #endregion

            // Check to see if the field and value are in the cache
            if (SAPCacheTables.ContainsKey(T_CUSTOMER) && !IsCustomerFound)
            {
                dtExtract = SAPCacheTables[T_CUSTOMER];
                dtExtract.DefaultView.RowFilter = "BUKRS = '" + currBUKRS + "' AND " + dr.FieldName + " = '" + fieldKey + "' AND " + F_VALIDDATA + " = 'True'";  // Crew Reynolds  PRUN-1790   02/19/21    Modified

                if (dtExtract.DefaultView.Count > 0)
                {
                    // Found the field data in the cache
                    dr.FieldStatus = ReturnStatus.VALID;
                    return dtExtract;
                }
                else //Added By Crew On 12 Mar 2021
                {
                    // Field data is not in cache
                    dr.FieldStatus = ReturnStatus.INVALID;
                    return null;
                }
            }

            return dtExtract;
        }
        private DataTable CustomerFetch()
        {
            string tableName = string.Empty;
            DataTable dt = new DataTable();
            DataTable OptTable = new DataTable();
            DataTable extractData = new DataTable();
            try
            {
                //Get Field Value from dtCoreData
                dtCoreData.DefaultView.RowFilter = "FNAM = 'KUNNR'";
                if (dtCoreData.DefaultView.Count > 0)
                {
                    dt = dtCoreData.DefaultView.ToTable(true, "FVAL");
                    string[] _SQLArrData = dt.AsEnumerable().Select(r => r.Field<string>("FVAL")).ToArray();
                    OptTable.Columns.Add(new DataColumn("KUNNR", Type.GetType("System.String")));

                    int TmpLastSQLArrPos = 0;
                    DataRow dbOptRow = null;
                    int SQLLen = 0;
                    int RowPos = 0;
                    int _LastSQLArrPos = 0;
                    DataTable dtExtract = new DataTable();
                    DataTable dtKNA1Data = new DataTable();
                    string strCustomer = string.Empty;
                    DataTable CacheTable = new DataTable();

                Next:
                    SQLLen = 0;
                    RowPos = 0;
                    dtExtract = new DataTable();
                    dtKNA1Data = new DataTable();
                    strCustomer = string.Empty;
                    CacheTable = new DataTable();

                    if (OptTable.Rows.Count > 0)
                        OptTable.Rows.Clear();

                    if (_LastSQLArrPos > 0)
                        TmpLastSQLArrPos = _LastSQLArrPos;

                    for (int k = TmpLastSQLArrPos; k < _SQLArrData.Length; k++)
                    {
                        dbOptRow = OptTable.NewRow();

                        if ((SQLLen + _SQLArrData[k].Length) > _ACSLimit)
                            break;

                        dbOptRow[0] = ZeroPad((string)_SQLArrData[k], 10);

                        if (RowPos == 0)
                            OptTable.Rows.Add(dbOptRow);
                        else
                            OptTable.Rows.InsertAt(dbOptRow, RowPos);

                        RowPos++;
                        SQLLen += dbOptRow[0].ToString().Length;
                        _LastSQLArrPos++;
                    }
                    OptTable.AcceptChanges();

                    if (SAPCacheTables.ContainsKey(T_CUSTOMER))
                        CacheTable = SAPCacheTables[T_CUSTOMER];

                    foreach (DataRow dr in OptTable.DefaultView.ToTable(true, "KUNNR").Rows)   // Pulls only distinct rows for KUNNR
                    {
                        string value = ZeroPad((string)dr["KUNNR"], 10);

                        //Remove Already Added Customer
                        if (SAPCacheTables.ContainsKey(T_CUSTOMER))
                        {
                            CacheTable.DefaultView.RowFilter = "BUKRS = '" + currBUKRS + "' AND KUNNR = '" + value + "'";
                            if (CacheTable.DefaultView.Count > 0)
                                continue;
                        }

                        if (strCustomer == string.Empty)
                            strCustomer = "'" + value + "'";
                        else
                            strCustomer += ",~" + "'" + value + "'";
                    }

                    if (strCustomer != string.Empty)
                    {
                        tableName = "KNB1";
                        string WhereCond = "KUNNR IN (" + strCustomer + ") AND BUKRS ='" + currBUKRS + "'";
                        dtExtract = SAPDefaults.SAPReadTable(SAPConn, tableName, "BUKRS~KUNNR~AKONT~SPERR~LOEVM", WhereCond, 0, 0, 0, true);

                        //Merge Child Table columns into Master Table columns
                        dtExtract.Columns.Add("KNA1_LAND1", typeof(string));
                        dtExtract.Columns.Add("KNA1_SPERR", typeof(string));
                        dtExtract.Columns.Add("KNA1_LOEVM", typeof(string));
                        dtExtract.Columns.Add(F_VALIDDATA, typeof(bool));

                        if (dtExtract.Rows.Count > 0)
                        {
                            tableName = "KNA1";
                            WhereCond = "KUNNR IN (" + strCustomer + ")";
                            dtKNA1Data = SAPDefaults.SAPReadTable(SAPConn, tableName, "SPERR~LOEVM~LAND1~KUNNR", WhereCond, 0, 0, 0, true);

                            // Run through all SKB1 rows and attach the SKA1 fields
                            foreach (DataRow drCARow in dtExtract.Rows)
                            {
                                foreach (DataRow drKNA1Row in dtKNA1Data.Rows)
                                {
                                    if (Convert.ToString(drCARow["KUNNR"]) == Convert.ToString(drKNA1Row["KUNNR"]))
                                    {
                                        drCARow["KNA1_LAND1"] = drKNA1Row["LAND1"];
                                        drCARow["KNA1_SPERR"] = drKNA1Row["SPERR"];
                                        drCARow["KNA1_LOEVM"] = drKNA1Row["LOEVM"];
                                        break;
                                    }
                                }
                            }
                        }

                        //InValidData Add Into Main Table
                        if (dtExtract.Rows.Count > 0)
                            dtExtract.Select().ToList<DataRow>().ForEach(r => r[F_VALIDDATA] = true);
                        string[] array = strCustomer.Replace("'", "").Replace(",", "").Split('~').ToArray();
                        var CheckWithArrayList = array.Select(x => new
                        {
                            KUNNR = x,
                            ValidData = dtExtract.AsEnumerable().Any(y => y.Field<string>("KUNNR") == x)
                        });
                        foreach (var item in CheckWithArrayList)
                        {
                            if (!item.ValidData)
                            {
                                DataRow dataRow = dtExtract.NewRow();
                                dataRow["KUNNR"] = Convert.ToString(item.KUNNR);
                                dataRow[F_VALIDDATA] = Convert.ToBoolean(item.ValidData);
                                dataRow["BUKRS"] = currBUKRS;
                                dtExtract.Rows.Add(dataRow);
                            }
                        }
                        //End

                        // Cache the all of the data that was pulled
                        if (dtExtract.Rows.Count > 0)
                        {
                            // Good SAP extract - cache it
                            if (SAPCacheTables.ContainsKey(T_CUSTOMER))
                                SAPCacheTables[T_CUSTOMER].Merge(dtExtract); // Cache already exixts. Append it.
                            else
                                SAPCacheTables.Add(T_CUSTOMER, dtExtract);   // Cache doesn't yet exist. Create it.
                        }

                        if (_LastSQLArrPos != _SQLArrData.Length)
                            goto Next;
                    }
                }

                if (SAPCacheTables.ContainsKey(T_CUSTOMER))
                    return extractData = SAPCacheTables[T_CUSTOMER];
            }
            catch (Exception ex)
            {
                DataExtractException(ex, tableName);
                throw ex;
            }
            finally
            {
                dt = null;
                OptTable = null;
            }
            return extractData;
        }
        private DataTable GetVendor(DataRequest dr)
        {
            // CHANGE LOG
            // Crew Reynolds    PRUN-1790    01/17/21    Data extract modified to check for value in cache first before running SAP call
            // Crew Reynolds    PRUN-1790    01/19/21    Modified to use new DataRequest class in method signature

            // DATATABLE OUTPUT - Note: pre-caching of all account numbers used on sheet

            DataTable dtExtract = new DataTable();
            DataTable dtLFA1Data = new DataTable();
            string tableName = string.Empty;
            string strVendor = string.Empty;
            string fieldKey = ZeroPad(dr.FieldValue, 10);

            // Check for blank value
            if (dr.FieldValue == "")
            {
                dr.FieldStatus = ReturnStatus.INVALID;
                return null;
            }

            // Check the cache for the fieldValue first
            if (SAPCacheTables.ContainsKey(T_VENDOR) && !IsVendorFound)
            {
                dtExtract = SAPCacheTables[T_VENDOR];
                dtExtract.DefaultView.RowFilter = "BUKRS = '" + currBUKRS + "' AND " + dr.FieldName + " = '" + fieldKey + "' AND " + F_VALIDDATA + " = 'True'";  //Crew Reynolds   PRUN-1790   01/16/21    Added BUKRS

                if (dtExtract.DefaultView.Count > 0)
                {
                    // Found the field data in the cache
                    dr.FieldStatus = ReturnStatus.VALID;
                    return dtExtract;
                }
                else //Added By Crew On 12 Mar 2021
                {
                    // Field data is not in cache
                    dr.FieldStatus = ReturnStatus.INVALID;
                    return null;
                }
            }

            //Added By Crew On 12 Mar 2021 Not in cache. Pull data from SAP
            if (IsVendorFound)
            {
                try
                {
                    DataTable dataTable = VendorFetch();
                    if (dataTable.Rows.Count > 0)
                        IsVendorFound = false;
                }
                catch (Exception ex)
                {
                    dr.FieldStatus = ReturnStatus.ERROR;
                    return null;
                }
            }

            #region Old Code
            /*
            // Not in cache. Pull data from SAP
            try
            {
                //Get distinct values for Current line level block excel rows
                strVendor = GetExcelLineLevelData("LIFNR", 10);

                tableName = "LFB1";
                string WhereCond = "LIFNR IN (" + strVendor + ") AND BUKRS = '" + currBUKRS + "'";
                dtExtract = SAPDefaults.SAPReadTable(SAPConn, tableName, "BUKRS~LIFNR~AKONT~SPERR~LOEVM", WhereCond, 0, 0, 0, true);

                if (dtExtract.Rows.Count > 0)
                {
                    tableName = "LFA1";
                    WhereCond = "LIFNR IN (" + strVendor + ")";
                    dtLFA1Data = SAPDefaults.SAPReadTable(SAPConn, tableName, "SPERR~LOEVM~LAND1~LIFNR", WhereCond, 0, 0, 0, true);

                    //Merge Child Table columns into Master Table columns
                    dtExtract.Columns.Add("LFA1_LAND1", typeof(string));
                    dtExtract.Columns.Add("LFA1_SPERR", typeof(string));
                    dtExtract.Columns.Add("LFA1_LOEVM", typeof(string));

                    foreach (DataRow drVenRow in dtExtract.Rows)
                    {
                        foreach (DataRow drLFA1Row in dtLFA1Data.Rows)
                        {
                            if (Convert.ToString(drVenRow["LIFNR"]) == Convert.ToString(drLFA1Row["LIFNR"]))
                            {
                                drVenRow["LFA1_LAND1"] = drLFA1Row["LAND1"];
                                drVenRow["LFA1_SPERR"] = drLFA1Row["SPERR"];
                                drVenRow["LFA1_LOEVM"] = drLFA1Row["LOEVM"];
                                break;  //Crew Reynolds    PRUN-1790   02/16/21    Added to break when match found
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // Error during the SAPReadTable call
                dtExtract = null;
                dr.FieldStatus = ReturnStatus.ERROR;
                DataExtractException(ex, tableName);
            }

            // Cache the all of the data that was pulled
            if (dtExtract.Rows.Count > 0)
            {
                // Good SAP extract - cache it
                if (SAPCacheTables.ContainsKey(T_VENDOR))
                    SAPCacheTables[T_VENDOR].Merge(dtExtract); // Cache already exixts. Append it.
                else
                    SAPCacheTables.Add(T_VENDOR, dtExtract);   // Cache doesn't yet exist. Create it.
            }
            */
            #endregion

            // Check to see if the field and value are in the cache
            if (SAPCacheTables.ContainsKey(T_VENDOR) && !IsVendorFound)
            {
                dtExtract = SAPCacheTables[T_VENDOR];
                dtExtract.DefaultView.RowFilter = "BUKRS = '" + currBUKRS + "' AND " + dr.FieldName + " = '" + fieldKey + "' AND " + F_VALIDDATA + " = 'True'";  //Crew Reynolds   PRUN-1790   02/16/21    Modified to include BUKRS

                if (dtExtract.DefaultView.Count > 0)
                {
                    // Found the field data in the cache
                    dr.FieldStatus = ReturnStatus.VALID;
                    return dtExtract;
                }
                else //Added By Crew On 12 Mar 2021
                {
                    // Field data is not in cache
                    dr.FieldStatus = ReturnStatus.INVALID;
                    return null;
                }
            }

            return dtExtract;
        }
        private DataTable VendorFetch()
        {
            string tableName = string.Empty;
            DataTable dt = new DataTable();
            DataTable OptTable = new DataTable();
            DataTable extractData = new DataTable();
            try
            {
                //Get Field Value from dtCoreData
                dtCoreData.DefaultView.RowFilter = "FNAM = 'LIFNR'";
                if (dtCoreData.DefaultView.Count > 0)
                {
                    dt = dtCoreData.DefaultView.ToTable(true, "FVAL");
                    string[] _SQLArrData = dt.AsEnumerable().Select(r => r.Field<string>("FVAL")).ToArray();
                    OptTable.Columns.Add(new DataColumn("LIFNR", Type.GetType("System.String")));

                    int TmpLastSQLArrPos = 0;
                    DataRow dbOptRow = null;
                    int SQLLen = 0;
                    int RowPos = 0;
                    int _LastSQLArrPos = 0;
                    DataTable dtExtract = new DataTable();
                    DataTable dtLFA1Data = new DataTable();
                    string strVendor = string.Empty;
                    DataTable CacheTable = new DataTable();

                Next:
                    SQLLen = 0;
                    RowPos = 0;
                    dtExtract = new DataTable();
                    dtLFA1Data = new DataTable();
                    strVendor = string.Empty;
                    CacheTable = new DataTable();

                    if (OptTable.Rows.Count > 0)
                        OptTable.Rows.Clear();

                    if (_LastSQLArrPos > 0)
                        TmpLastSQLArrPos = _LastSQLArrPos;

                    for (int k = TmpLastSQLArrPos; k < _SQLArrData.Length; k++)
                    {
                        dbOptRow = OptTable.NewRow();

                        if ((SQLLen + _SQLArrData[k].Length) > _ACSLimit)
                            break;

                        dbOptRow[0] = ZeroPad((string)_SQLArrData[k], 10);

                        if (RowPos == 0)
                            OptTable.Rows.Add(dbOptRow);
                        else
                            OptTable.Rows.InsertAt(dbOptRow, RowPos);

                        RowPos++;
                        SQLLen += dbOptRow[0].ToString().Length;
                        _LastSQLArrPos++;
                    }
                    OptTable.AcceptChanges();

                    if (SAPCacheTables.ContainsKey(T_VENDOR))
                        CacheTable = SAPCacheTables[T_VENDOR];

                    foreach (DataRow dr in OptTable.DefaultView.ToTable(true, "LIFNR").Rows)   // Pulls only distinct rows for LIFNR
                    {
                        string value = ZeroPad((string)dr["LIFNR"], 10);

                        //Remove Already Added Account Number 
                        if (SAPCacheTables.ContainsKey(T_VENDOR))
                        {
                            CacheTable.DefaultView.RowFilter = "BUKRS = '" + currBUKRS + "' AND LIFNR = '" + value + "'";
                            if (CacheTable.DefaultView.Count > 0)
                                continue;
                        }

                        if (strVendor == string.Empty)
                            strVendor = "'" + value + "'";
                        else
                            strVendor += ",~" + "'" + value + "'";
                    }

                    if (strVendor != string.Empty)
                    {
                        tableName = "LFB1";
                        string WhereCond = "LIFNR IN (" + strVendor + ") AND BUKRS = '" + currBUKRS + "'";
                        dtExtract = SAPDefaults.SAPReadTable(SAPConn, tableName, "BUKRS~LIFNR~AKONT~SPERR~LOEVM", WhereCond, 0, 0, 0, true);

                        //Merge Child Table columns into Master Table columns
                        dtExtract.Columns.Add("LFA1_LAND1", typeof(string));
                        dtExtract.Columns.Add("LFA1_SPERR", typeof(string));
                        dtExtract.Columns.Add("LFA1_LOEVM", typeof(string));
                        dtExtract.Columns.Add(F_VALIDDATA, typeof(bool));

                        if (dtExtract.Rows.Count > 0)
                        {
                            tableName = "LFA1";
                            WhereCond = "LIFNR IN (" + strVendor + ")";
                            dtLFA1Data = SAPDefaults.SAPReadTable(SAPConn, tableName, "SPERR~LOEVM~LAND1~LIFNR", WhereCond, 0, 0, 0, true);

                            // Run through all SKB1 rows and attach the SKA1 fields
                            foreach (DataRow drVenRow in dtExtract.Rows)
                            {
                                foreach (DataRow drLFA1Row in dtLFA1Data.Rows)
                                {
                                    if (Convert.ToString(drVenRow["LIFNR"]) == Convert.ToString(drLFA1Row["LIFNR"]))
                                    {
                                        drVenRow["LFA1_LAND1"] = drLFA1Row["LAND1"];
                                        drVenRow["LFA1_SPERR"] = drLFA1Row["SPERR"];
                                        drVenRow["LFA1_LOEVM"] = drLFA1Row["LOEVM"];
                                        break;  //Crew Reynolds    PRUN-1790   02/16/21    Added to break when match found
                                    }
                                }
                            }
                        }

                        //InValidData Add Into Main Table
                        if (dtExtract.Rows.Count > 0)
                            dtExtract.Select().ToList<DataRow>().ForEach(r => r[F_VALIDDATA] = true);
                        string[] array = strVendor.Replace("'", "").Replace(",", "").Split('~').ToArray();
                        var CheckWithArrayList = array.Select(x => new
                        {
                            LIFNR = x,
                            ValidData = dtExtract.AsEnumerable().Any(y => y.Field<string>("LIFNR") == x)
                        });
                        foreach (var item in CheckWithArrayList)
                        {
                            if (!item.ValidData)
                            {
                                DataRow dataRow = dtExtract.NewRow();
                                dataRow["LIFNR"] = Convert.ToString(item.LIFNR);
                                dataRow[F_VALIDDATA] = Convert.ToBoolean(item.ValidData);
                                dataRow["BUKRS"] = currBUKRS;
                                dtExtract.Rows.Add(dataRow);
                            }
                        }
                        //End

                        // Cache the all of the data that was pulled
                        if (dtExtract.Rows.Count > 0)
                        {
                            // Good SAP extract - cache it
                            if (SAPCacheTables.ContainsKey(T_VENDOR))
                                SAPCacheTables[T_VENDOR].Merge(dtExtract); // Cache already exixts. Append it.
                            else
                                SAPCacheTables.Add(T_VENDOR, dtExtract);   // Cache doesn't yet exist. Create it.
                        }

                        if (_LastSQLArrPos != _SQLArrData.Length)
                            goto Next;
                    }
                }

                if (SAPCacheTables.ContainsKey(T_VENDOR))
                    return extractData = SAPCacheTables[T_VENDOR];
            }
            catch (Exception ex)
            {
                DataExtractException(ex, tableName);
                throw ex;
            }
            finally
            {
                dt = null;
                OptTable = null;
            }
            return extractData;
        }
        private DataTable GetProfitCenter(DataRequest dr)
        {
            string tableName = string.Empty;
            string strProfitCenters = string.Empty;
            DataTable dtExtract = new DataTable();
            string fieldKey = ZeroPad(dr.FieldValue, 10);

            // Check for blank value
            if (dr.FieldValue == "")
            {
                dr.FieldStatus = ReturnStatus.INVALID;
                return null;
            }

            // Check the cache for the fieldValue first
            if (SAPCacheTables.ContainsKey(T_PROFITCNTR) && !IsProfitCenterFound)
            {
                dtExtract = SAPCacheTables[T_PROFITCNTR];
                dtExtract.DefaultView.RowFilter = "BUKRS = '" + currBUKRS + "' AND PRCTR = '" + fieldKey + "' AND " + F_VALIDDATA + " = 'True'";

                if (dtExtract.DefaultView.Count > 0)
                {
                    // Found the field data in the cache
                    dr.FieldStatus = ReturnStatus.VALID;
                    return dtExtract;
                }
                else
                {
                    // Field data is not in cache
                    dr.FieldStatus = ReturnStatus.INVALID;
                    dtExtract = null;
                }
            }

            //Added By Crew On 12 Mar 2021 Not in cache. Pull data from SAP
            if (IsProfitCenterFound)
            {
                try
                {
                    DataTable dataTable = ProfitCenterFetch();
                    if (dataTable.Rows.Count > 0)
                        IsProfitCenterFound = false;
                }
                catch (Exception ex)
                {
                    dr.FieldStatus = ReturnStatus.ERROR;
                    return null;
                }
            }

            #region Old Code
            /*
            try
            {
                //Get distinct values for Current line level block excel rows
                strProfitCenters = GetExcelLineLevelData("PRCTR", 10);   // Crew Reynolds    PRUN-1790   02/19/21    Modified call to include value pad length

                tableName = "CEPC_BUKRS";
                string WhereCond = "BUKRS = '" + currBUKRS + "' AND PRCTR IN (" + strProfitCenters + ")";
                dtExtract = SAPDefaults.SAPReadTable(SAPConn, tableName, "PRCTR~BUKRS~KOKRS", WhereCond, 0, 0, 0, true);

                if (dtExtract.Rows.Count > 0)
                {
                    dtExtract.Columns.Add("LOCK_IND", typeof(string));

                    foreach (DataRow dbRow in dtExtract.Rows)
                    {
                        tableName = "CEPC";
                        WhereCond = "PRCTR = '" + dbRow["PRCTR"] + "' AND KOKRS = '" + dbRow["KOKRS"] + "' AND LOCK_IND = 'X' AND DATAB <= '" + DateTime.Now + "' AND DATBI >= '" + DateTime.Now + "'";
                        DataTable CEPCData = SAPDefaults.SAPReadTable(SAPConn, tableName, "LOCK_IND", WhereCond, 0, 1, 0, true);
                        if (CEPCData.Rows.Count > 0) dbRow["LOCK_IND"] = CEPCData.Rows[0]["LOCK_IND"];
                    }
                }
            }
            catch (Exception ex)
            {
                // Error during the SAPReadTable call
                dtExtract = null;
                dr.FieldStatus = ReturnStatus.ERROR;
                DataExtractException(ex, tableName);
            }

            // Cache the all of the data that was pulled
            if (dtExtract.Rows.Count > 0)
            {
                // Good SAP extract - cache it
                if (SAPCacheTables.ContainsKey(T_PROFITCNTR))
                    SAPCacheTables[T_PROFITCNTR].Merge(dtExtract); // Cache already exixts. Append it.
                else
                    SAPCacheTables.Add(T_PROFITCNTR, dtExtract);   // Cache doesn't yet exist. Create it.
            }
            */
            #endregion

            // Check to see if the field and value are in the cache
            if (SAPCacheTables.ContainsKey(T_PROFITCNTR) && !IsProfitCenterFound)
            {
                dtExtract = SAPCacheTables[T_PROFITCNTR];
                dtExtract.DefaultView.RowFilter = "BUKRS = '" + currBUKRS + "' AND PRCTR = '" + fieldKey + "' AND " + F_VALIDDATA + " = 'True'";

                if (dtExtract.DefaultView.Count > 0)
                {
                    // Found the field data in the cache
                    dr.FieldStatus = ReturnStatus.VALID;
                    return dtExtract;
                }
                else
                {
                    // Field data is not in cache
                    dr.FieldStatus = ReturnStatus.INVALID;
                    dtExtract = null;
                }
            }

            return dtExtract;
        }
        private DataTable ProfitCenterFetch()
        {
            string tableName = string.Empty;
            DataTable dt = new DataTable();
            DataTable OptTable = new DataTable();
            DataTable extractData = new DataTable();
            string ToDayDate = DateTime.Now.ToString("YYYYMMDD");
            try
            {
                //Get Field Value from dtCoreData
                dtCoreData.DefaultView.RowFilter = "FNAM = 'PRCTR'";
                if (dtCoreData.DefaultView.Count > 0)
                {
                    dt = dtCoreData.DefaultView.ToTable(true, "FVAL");
                    string[] _SQLArrData = dt.AsEnumerable().Select(r => r.Field<string>("FVAL")).ToArray();
                    OptTable.Columns.Add(new DataColumn("PRCTR", Type.GetType("System.String")));

                    int TmpLastSQLArrPos = 0;
                    DataRow dbOptRow = null;
                    int SQLLen = 0;
                    int RowPos = 0;
                    int _LastSQLArrPos = 0;
                    DataTable dtExtract = new DataTable();
                    DataTable dtLFA1Data = new DataTable();
                    string strProfitCenters = string.Empty;
                    DataTable CacheTable = new DataTable();

                Next:
                    SQLLen = 0;
                    RowPos = 0;
                    dtExtract = new DataTable();
                    dtLFA1Data = new DataTable();
                    strProfitCenters = string.Empty;
                    CacheTable = new DataTable();

                    if (OptTable.Rows.Count > 0)
                        OptTable.Rows.Clear();

                    if (_LastSQLArrPos > 0)
                        TmpLastSQLArrPos = _LastSQLArrPos;

                    for (int k = TmpLastSQLArrPos; k < _SQLArrData.Length; k++)
                    {
                        dbOptRow = OptTable.NewRow();

                        if ((SQLLen + _SQLArrData[k].Length) > _ACSLimit)
                            break;

                        dbOptRow[0] = ZeroPad((string)_SQLArrData[k], 10);

                        if (RowPos == 0)
                            OptTable.Rows.Add(dbOptRow);
                        else
                            OptTable.Rows.InsertAt(dbOptRow, RowPos);

                        RowPos++;
                        SQLLen += dbOptRow[0].ToString().Length;
                        _LastSQLArrPos++;
                    }
                    OptTable.AcceptChanges();

                    if (SAPCacheTables.ContainsKey(T_PROFITCNTR))
                        CacheTable = SAPCacheTables[T_PROFITCNTR];

                    foreach (DataRow dr in OptTable.DefaultView.ToTable(true, "PRCTR").Rows)   // Pulls only distinct rows for PRCTR
                    {
                        string value = ZeroPad((string)dr["PRCTR"], 10);

                        //Remove Already Added Profit Center
                        if (SAPCacheTables.ContainsKey(T_PROFITCNTR))
                        {
                            CacheTable.DefaultView.RowFilter = "BUKRS = '" + currBUKRS + "' AND PRCTR = '" + value + "'";
                            if (CacheTable.DefaultView.Count > 0)
                                continue;
                        }

                        if (strProfitCenters == string.Empty)
                            strProfitCenters = "'" + value + "'";
                        else
                            strProfitCenters += ",~" + "'" + value + "'";
                    }

                    if (strProfitCenters != string.Empty)
                    {
                        tableName = "CEPC_BUKRS";
                        string WhereCond = "BUKRS = '" + currBUKRS + "' AND PRCTR IN (" + strProfitCenters + ")";
                        dtExtract = SAPDefaults.SAPReadTable(SAPConn, tableName, "PRCTR~BUKRS~KOKRS", WhereCond, 0, 0, 0, true);

                        //Merge Child Table columns into Master Table columns
                        dtExtract.Columns.Add("LOCK_IND", typeof(string));
                        dtExtract.Columns.Add(F_VALIDDATA, typeof(bool));

                        if (dtExtract.Rows.Count > 0)
                        {
                            foreach (DataRow dbRow in dtExtract.Rows)
                            {
                                tableName = "CEPC";
                                WhereCond = "PRCTR = '" + dbRow["PRCTR"] + "' AND KOKRS = '" + dbRow["KOKRS"] + "' AND LOCK_IND = 'X' AND DATAB <= '" + ToDayDate + "' AND DATBI >= '" + ToDayDate + "'";
                                DataTable CEPCData = SAPDefaults.SAPReadTable(SAPConn, tableName, "LOCK_IND", WhereCond, 0, 1, 0, true);
                                if (CEPCData.Rows.Count > 0) dbRow["LOCK_IND"] = CEPCData.Rows[0]["LOCK_IND"];
                            }
                        }

                        //InValidData Add Into Main Table
                        if (dtExtract.Rows.Count > 0)
                            dtExtract.Select().ToList<DataRow>().ForEach(r => r[F_VALIDDATA] = true);
                        string[] array = strProfitCenters.Replace("'", "").Replace(",", "").Split('~').ToArray();
                        var CheckWithArrayList = array.Select(x => new
                        {
                            PRCTR = x,
                            ValidData = dtExtract.AsEnumerable().Any(y => y.Field<string>("PRCTR") == x)
                        });
                        foreach (var item in CheckWithArrayList)
                        {
                            if (!item.ValidData)
                            {
                                DataRow dataRow = dtExtract.NewRow();
                                dataRow["PRCTR"] = Convert.ToString(item.PRCTR);
                                dataRow[F_VALIDDATA] = Convert.ToBoolean(item.ValidData);
                                dataRow["BUKRS"] = currBUKRS;
                                dtExtract.Rows.Add(dataRow);
                            }
                        }
                        //End

                        // Cache the all of the data that was pulled
                        if (dtExtract.Rows.Count > 0)
                        {
                            // Good SAP extract - cache it
                            if (SAPCacheTables.ContainsKey(T_PROFITCNTR))
                                SAPCacheTables[T_PROFITCNTR].Merge(dtExtract); // Cache already exixts. Append it.
                            else
                                SAPCacheTables.Add(T_PROFITCNTR, dtExtract);   // Cache doesn't yet exist. Create it.
                        }

                        if (_LastSQLArrPos != _SQLArrData.Length)
                            goto Next;
                    }
                }

                if (SAPCacheTables.ContainsKey(T_PROFITCNTR))
                    return extractData = SAPCacheTables[T_PROFITCNTR];
            }
            catch (Exception ex)
            {
                DataExtractException(ex, tableName);
                throw ex;
            }
            finally
            {
                dt = null;
                OptTable = null;
            }
            return extractData;
        }
        private DataTable GetCostElement(DataRequest dr)
        {
            // TODO Does this query need KOKRS key? ABAP doesn't use it

            DataTable CostElementData = new DataTable();
            DataTable dtExtract = new DataTable();
            string tableName = string.Empty;
            string strCostElement = string.Empty;
            string WhereCond = string.Empty;
            string fieldKey = ZeroPad(dr.FieldValue, 10);

            // Check for blank value
            if (dr.FieldValue == "")
            {
                dr.FieldStatus = ReturnStatus.INVALID;
                return null;
            }

            // Check the cache for the fieldValue first
            if (SAPCacheTables.ContainsKey(T_COSTELEM) && !IsCostElementFound)
            {
                dtExtract = SAPCacheTables[T_COSTELEM];
                dtExtract.DefaultView.RowFilter = "KTOPL = '" + currKTOPL + "' AND KSTAR = '" + fieldKey + "' AND " + F_VALIDDATA + " = 'True'";

                if (dtExtract.DefaultView.Count > 0)
                {
                    // Found the field data in the cache
                    dr.FieldStatus = ReturnStatus.VALID;
                    return dtExtract;
                }
                else
                {
                    // Field data is not in cache
                    dr.FieldStatus = ReturnStatus.INVALID;
                    return null;
                }
            }

            //Added By Crew On 12 Mar 2021 Not in cache. Pull data from SAP
            if (IsCostElementFound)
            {
                try
                {
                    DataTable dataTable = CostElementFetch();
                    if (dataTable.Rows.Count > 0)
                        IsCostElementFound = false;
                }
                catch (Exception ex)
                {
                    dr.FieldStatus = ReturnStatus.ERROR;
                    return null;
                }
            }

            #region Old Code
            /*
            try
            {
                //Get distinct values for Current line level block excel rows
                strCostElement = GetExcelLineLevelData("KSTAR", 10);

                tableName = "CSKA";
                WhereCond = "KTOPL = '" + currKTOPL + "' AND KSTAR IN (" + strCostElement + ")";
                CostElementData = SAPDefaults.SAPReadTable(SAPConn, tableName, "KTOPL~KSTAR", WhereCond, 0, 0, 0, true);
            }
            catch (Exception ex)
            {
                // Error during the SAPReadTable call
                dtExtract = null;
                dr.FieldStatus = ReturnStatus.ERROR;
                DataExtractException(ex, tableName);
            }

            // Cache the all of the data that was pulled
            dtExtract = CostElementData;
            if (dtExtract.Rows.Count > 0)
            {
                // Good SAP extract - cache it
                if (SAPCacheTables.ContainsKey(T_COSTELEM))
                    SAPCacheTables[T_COSTELEM].Merge(dtExtract); // Cache already exixts. Append it.
                else
                    SAPCacheTables.Add(T_COSTELEM, dtExtract);   // Cache doesn't yet exist. Create it.
            }
            */
            #endregion

            // Check to see if the field and value are in the caches
            if (SAPCacheTables.ContainsKey(T_COSTELEM) && !IsCostElementFound)
            {
                dtExtract = SAPCacheTables[T_COSTELEM];
                dtExtract.DefaultView.RowFilter = "KTOPL = '" + currKTOPL + "' AND KSTAR = '" + fieldKey + "' AND " + F_VALIDDATA + " = 'True'";

                if (dtExtract.DefaultView.Count > 0)
                {
                    // Found the field data in the cache
                    dr.FieldStatus = ReturnStatus.VALID;
                    return dtExtract;
                }
                else
                {
                    // Field data is not in cache
                    dr.FieldStatus = ReturnStatus.INVALID;
                    return null;
                }
            }

            return dtExtract;
        }
        private DataTable CostElementFetch()
        {
            string tableName = string.Empty;
            DataTable dt = new DataTable();
            DataTable OptTable = new DataTable();
            DataTable extractData = new DataTable();
            try
            {
                //Get Field Value from dtCoreData
                dtCoreData.DefaultView.RowFilter = "FNAM = 'KSTAR'";
                if (dtCoreData.DefaultView.Count > 0)
                {
                    dt = dtCoreData.DefaultView.ToTable(true, "FVAL");
                    string[] _SQLArrData = dt.AsEnumerable().Select(r => r.Field<string>("FVAL")).ToArray();
                    OptTable.Columns.Add(new DataColumn("KSTAR", Type.GetType("System.String")));

                    int TmpLastSQLArrPos = 0;
                    DataRow dbOptRow = null;
                    int SQLLen = 0;
                    int RowPos = 0;
                    int _LastSQLArrPos = 0;
                    DataTable dtExtract = new DataTable();
                    string strCostElement = string.Empty;
                    DataTable CacheTable = new DataTable();

                Next:
                    SQLLen = 0;
                    RowPos = 0;
                    dtExtract = new DataTable();
                    strCostElement = string.Empty;
                    CacheTable = new DataTable();

                    if (OptTable.Rows.Count > 0)
                        OptTable.Rows.Clear();

                    if (_LastSQLArrPos > 0)
                        TmpLastSQLArrPos = _LastSQLArrPos;

                    for (int k = TmpLastSQLArrPos; k < _SQLArrData.Length; k++)
                    {
                        dbOptRow = OptTable.NewRow();

                        if ((SQLLen + _SQLArrData[k].Length) > _ACSLimit)
                            break;

                        dbOptRow[0] = ZeroPad((string)_SQLArrData[k], 10);

                        if (RowPos == 0)
                            OptTable.Rows.Add(dbOptRow);
                        else
                            OptTable.Rows.InsertAt(dbOptRow, RowPos);

                        RowPos++;
                        SQLLen += dbOptRow[0].ToString().Length;
                        _LastSQLArrPos++;
                    }
                    OptTable.AcceptChanges();

                    if (SAPCacheTables.ContainsKey(T_COSTELEM))
                        CacheTable = SAPCacheTables[T_COSTELEM];

                    foreach (DataRow dr in OptTable.DefaultView.ToTable(true, "KSTAR").Rows)   // Pulls only distinct rows for KSTAR
                    {
                        string value = ZeroPad((string)dr["KSTAR"], 10);

                        //Remove Already Added Cost Element
                        if (SAPCacheTables.ContainsKey(T_COSTELEM))
                        {
                            CacheTable.DefaultView.RowFilter = "KTOPL = '" + currKTOPL + "' AND KSTAR = '" + value + "'";
                            if (CacheTable.DefaultView.Count > 0)
                                continue;
                        }

                        if (strCostElement == string.Empty)
                            strCostElement = "'" + value + "'";
                        else
                            strCostElement += ",~" + "'" + value + "'";
                    }

                    if (strCostElement != string.Empty)
                    {
                        tableName = "CSKA";
                        string WhereCond = "KTOPL = '" + currKTOPL + "' AND KSTAR IN (" + strCostElement + ")";
                        dtExtract = SAPDefaults.SAPReadTable(SAPConn, tableName, "KTOPL~KSTAR", WhereCond, 0, 0, 0, true);

                        //Merge Child Table columns into Master Table columns
                        dtExtract.Columns.Add(F_VALIDDATA, typeof(bool));

                        //InValidData Add Into Main Table
                        if (dtExtract.Rows.Count > 0)
                            dtExtract.Select().ToList<DataRow>().ForEach(r => r[F_VALIDDATA] = true);
                        string[] array = strCostElement.Replace("'", "").Replace(",", "").Split('~').ToArray();
                        var CheckWithArrayList = array.Select(x => new
                        {
                            KSTAR = x,
                            ValidData = dtExtract.AsEnumerable().Any(y => y.Field<string>("KSTAR") == x)
                        });
                        foreach (var item in CheckWithArrayList)
                        {
                            if (!item.ValidData)
                            {
                                DataRow dataRow = dtExtract.NewRow();
                                dataRow["KSTAR"] = Convert.ToString(item.KSTAR);
                                dataRow[F_VALIDDATA] = Convert.ToBoolean(item.ValidData);
                                dataRow["KTOPL"] = currKTOPL;
                                dtExtract.Rows.Add(dataRow);
                            }
                        }
                        //End

                        // Cache the all of the data that was pulled
                        if (dtExtract.Rows.Count > 0)
                        {
                            // Good SAP extract - cache it
                            if (SAPCacheTables.ContainsKey(T_COSTELEM))
                                SAPCacheTables[T_COSTELEM].Merge(dtExtract); // Cache already exixts. Append it.
                            else
                                SAPCacheTables.Add(T_COSTELEM, dtExtract);   // Cache doesn't yet exist. Create it.
                        }

                        if (_LastSQLArrPos != _SQLArrData.Length)
                            goto Next;
                    }
                }

                if (SAPCacheTables.ContainsKey(T_COSTELEM))
                    return extractData = SAPCacheTables[T_COSTELEM];
            }
            catch (Exception ex)
            {
                DataExtractException(ex, tableName);
                throw ex;
            }
            finally
            {
                dt = null;
                OptTable = null;
            }
            return extractData;
        }
        private DataTable GetCostCenter(DataRequest dr)
        {
            // OUTPUT 
            // KOKRS = Controlling Area
            // KOSTL = Cost Center
            // BUKRS = Company Code
            // DATAB = Valid From
            // DATBI = Valid To
            // KTEXT = Valid Description
            // BKZKP = Locked, Actual Primary Costs
            // BKZER = Locked, Actual Revenue   

            string tableName = string.Empty;
            string fieldKey = ZeroPad(dr.FieldValue, 10);
            DataTable dtExtract = new DataTable();

            // Check for blank value
            if (dr.FieldValue == "")
            {
                dr.FieldStatus = ReturnStatus.INVALID;
                return null;
            }

            // Check the cache for the fieldValue first
            if (SAPCacheTables.ContainsKey(T_COSTCNTR) && !IsCostCenterFound)
            {
                dtExtract = SAPCacheTables[T_COSTCNTR];
                dtExtract.DefaultView.RowFilter = "BUKRS = '" + currBUKRS + "' AND KOSTL = '" + fieldKey + "' AND " + F_VALIDDATA + " = 'True'";

                if (dtExtract.DefaultView.Count > 0)
                {
                    // Found the field data in the cache
                    dr.FieldStatus = ReturnStatus.VALID;
                    return dtExtract;
                }
                else
                {
                    // Field data is not in cache
                    dr.FieldStatus = ReturnStatus.INVALID;
                    return null;
                }
            }

            //Added By Crew On 12 Mar 2021 Not in cache. Pull data from SAP
            if (IsCostCenterFound)
            {
                try
                {
                    DataTable dataTable = CostCenterFetch();
                    if (dataTable.Rows.Count > 0)
                        IsCostCenterFound = false;
                }
                catch (Exception ex)
                {
                    dr.FieldStatus = ReturnStatus.ERROR;
                    return null;
                }
            }

            #region Old Code
            /*
            try
            {
                if (dr.FieldValue != string.Empty)
                {
                    tableName = "CSKS";
                    //Crew Reynolds PRUN-1790   02/16/21    Cannot pull ALL cost centers from sheet as the Company Code (BUKRS) may change from line to line.

                    //Crew Reynolds PRUN-1790  02/17/21    Added BUKRS in the key per ABAP select. Added columns BKZKP and BKZER
                    string WhereCond = "BUKRS = '" + currBUKRS + "' AND KOSTL = '" + fieldKey + "'";

                    //Crew Reynolds PRUN-1790 02/17/21 Replaced CostCenterData with std dtExtract
                    dtExtract = SAPDefaults.SAPReadTable(SAPConn, tableName, "KOKRS~KOSTL~BUKRS~DATAB~DATBI~BKZKP~BKZER", WhereCond, 0, 0, 0, true);
                }
            }
            catch (Exception ex)
            {
                // Error during the SAPReadTable call
                dtExtract = null;
                dr.FieldStatus = ReturnStatus.ERROR;
                DataExtractException(ex, tableName);
            }

            // Cache the all of the data that was pulled
            if (dtExtract.Rows.Count > 0)
            {
                // Good SAP extract - cache it
                if (SAPCacheTables.ContainsKey(T_COSTCNTR))
                    SAPCacheTables[T_COSTCNTR].Merge(dtExtract); // Cache already exixts. Append it.
                else
                    SAPCacheTables.Add(T_COSTCNTR, dtExtract);   // Cache doesn't yet exist. Create it.
            }
            */
            #endregion

            // Check to see if the field and value are in the cache
            if (SAPCacheTables.ContainsKey(T_COSTCNTR) && !IsCostCenterFound)
            {
                dtExtract = SAPCacheTables[T_COSTCNTR];
                dtExtract.DefaultView.RowFilter = "BUKRS = '" + currBUKRS + "' AND KOSTL = '" + fieldKey + "' AND " + F_VALIDDATA + " = 'True'";

                if (dtExtract.DefaultView.Count > 0)
                {
                    // Found the field data in the cache
                    dr.FieldStatus = ReturnStatus.VALID;
                    return dtExtract;
                }
                else
                {
                    // Field data is not in cache
                    dr.FieldStatus = ReturnStatus.INVALID;
                    return null;
                }
            }

            return dtExtract;
        }
        private DataTable CostCenterFetch()
        {
            string tableName = string.Empty;
            DataTable dt = new DataTable();
            DataTable OptTable = new DataTable();
            DataTable extractData = new DataTable();
            try
            {
                //Get Field Value from dtCoreData
                dtCoreData.DefaultView.RowFilter = "FNAM = 'KOSTL'";
                if (dtCoreData.DefaultView.Count > 0)
                {
                    dt = dtCoreData.DefaultView.ToTable(true, "FVAL");
                    string[] _SQLArrData = dt.AsEnumerable().Select(r => r.Field<string>("FVAL")).ToArray();
                    OptTable.Columns.Add(new DataColumn("KOSTL", Type.GetType("System.String")));

                    int TmpLastSQLArrPos = 0;
                    DataRow dbOptRow = null;
                    int SQLLen = 0;
                    int RowPos = 0;
                    int _LastSQLArrPos = 0;
                    DataTable dtExtract = new DataTable();
                    string strCostCenter = string.Empty;
                    DataTable CacheTable = new DataTable();

                Next:
                    SQLLen = 0;
                    RowPos = 0;
                    dtExtract = new DataTable();
                    strCostCenter = string.Empty;
                    CacheTable = new DataTable();

                    if (OptTable.Rows.Count > 0)
                        OptTable.Rows.Clear();

                    if (_LastSQLArrPos > 0)
                        TmpLastSQLArrPos = _LastSQLArrPos;

                    for (int k = TmpLastSQLArrPos; k < _SQLArrData.Length; k++)
                    {
                        dbOptRow = OptTable.NewRow();

                        if ((SQLLen + _SQLArrData[k].Length) > _ACSLimit)
                            break;

                        dbOptRow[0] = ZeroPad((string)_SQLArrData[k], 10);

                        if (RowPos == 0)
                            OptTable.Rows.Add(dbOptRow);
                        else
                            OptTable.Rows.InsertAt(dbOptRow, RowPos);

                        RowPos++;
                        SQLLen += dbOptRow[0].ToString().Length;
                        _LastSQLArrPos++;
                    }
                    OptTable.AcceptChanges();

                    if (SAPCacheTables.ContainsKey(T_COSTCNTR))
                        CacheTable = SAPCacheTables[T_COSTCNTR];

                    foreach (DataRow dr in OptTable.DefaultView.ToTable(true, "KOSTL").Rows)   // Pulls only distinct rows for KOSTL
                    {
                        string value = ZeroPad((string)dr["KOSTL"], 10);

                        //Remove Already Added Account Number 
                        if (SAPCacheTables.ContainsKey(T_COSTCNTR))
                        {
                            CacheTable.DefaultView.RowFilter = "BUKRS = '" + currBUKRS + "' AND KOSTL = '" + value + "'";
                            if (CacheTable.DefaultView.Count > 0)
                                continue;
                        }

                        if (strCostCenter == string.Empty)
                            strCostCenter = "'" + value + "'";
                        else
                            strCostCenter += ",~" + "'" + value + "'";
                    }

                    if (strCostCenter != string.Empty)
                    {
                        tableName = "CSKS";
                        //Crew Reynolds PRUN-1790   02/16/21    Cannot pull ALL cost centers from sheet as the Company Code (BUKRS) may change from line to line.

                        //Crew Reynolds PRUN-1790  02/17/21    Added BUKRS in the key per ABAP select. Added columns BKZKP and BKZER
                        string WhereCond = "BUKRS = '" + currBUKRS + "' AND KOSTL IN (" + strCostCenter + ")";

                        //Crew Reynolds PRUN-1790 02/17/21 Replaced CostCenterData with std dtExtract
                        dtExtract = SAPDefaults.SAPReadTable(SAPConn, tableName, "KOKRS~KOSTL~BUKRS~DATAB~DATBI~BKZKP~BKZER", WhereCond, 0, 0, 0, true);

                        //Merge Child Table columns into Master Table columns
                        dtExtract.Columns.Add(F_VALIDDATA, typeof(bool));

                        //InValidData Add Into Main Table
                        if (dtExtract.Rows.Count > 0)
                            dtExtract.Select().ToList<DataRow>().ForEach(r => r[F_VALIDDATA] = true);
                        string[] array = strCostCenter.Replace("'", "").Replace(",", "").Split('~').ToArray();
                        var CheckWithArrayList = array.Select(x => new
                        {
                            KOSTL = x,
                            ValidData = dtExtract.AsEnumerable().Any(y => y.Field<string>("KOSTL") == x)
                        });
                        foreach (var item in CheckWithArrayList)
                        {
                            if (!item.ValidData)
                            {
                                DataRow dataRow = dtExtract.NewRow();
                                dataRow["KOSTL"] = Convert.ToString(item.KOSTL);
                                dataRow[F_VALIDDATA] = Convert.ToBoolean(item.ValidData);
                                dataRow["BUKRS"] = currBUKRS;
                                dtExtract.Rows.Add(dataRow);
                            }
                        }
                        //End

                        // Cache the all of the data that was pulled
                        if (dtExtract.Rows.Count > 0)
                        {
                            // Good SAP extract - cache it
                            if (SAPCacheTables.ContainsKey(T_COSTCNTR))
                                SAPCacheTables[T_COSTCNTR].Merge(dtExtract); // Cache already exixts. Append it.
                            else
                                SAPCacheTables.Add(T_COSTCNTR, dtExtract);   // Cache doesn't yet exist. Create it.
                        }

                        if (_LastSQLArrPos != _SQLArrData.Length)
                            goto Next;
                    }
                }

                if (SAPCacheTables.ContainsKey(T_COSTCNTR))
                    return extractData = SAPCacheTables[T_COSTCNTR];
            }
            catch (Exception ex)
            {
                DataExtractException(ex, tableName);
                throw ex;
            }
            finally
            {
                dt = null;
                OptTable = null;
            }
            return extractData;
        }
        private string GetExcelLineLevelData(string fieldName, int padLength)
        {
            //Crew Reynolds PRUN-1790   02/19/21    Rewrite to use dtCoreData
            string valueList = string.Empty;

            //Get Field Value from dtCoreData
            dtCoreData.DefaultView.RowFilter = "FNAM = '" + fieldName + "'";

            if (dtCoreData.DefaultView.Count > 0)
            {
                foreach (DataRow dr in dtCoreData.DefaultView.ToTable(true, "FVAL").Rows)   // Pulls only distinct rows for FVAL
                {
                    string value = string.Empty;

                    if (padLength > 0)
                        value = ZeroPad((string)dr["FVAL"], padLength);
                    else
                        value = (string)dr["FVAL"];

                    if (valueList == string.Empty)
                        valueList = "'" + value + "'";
                    else
                        valueList += ",~" + "'" + value + "'";
                }
            }

            return valueList;
        }
        #endregion

        // Crew Reynolds 02/28/21
        //
        // TODO Each of the Stage 2 data extracts must consistently follow the stage 1 data extract code pattern
        //
        // The code must be be consistent for each data extract between Stage 1 and Stage 2.
        // This will keep things clean AND give us the opportunity to refactor out the duplicated cache code (Check, Create) into resusable methods.
        //
        // Psuedocode of code pattern
        // Note the addition of string fieldName. This is used as follows:
        // - string fieldKey = vr.FieldName if the key field does not need to be zero padded.
        // - string fieldKey = ZeroPad(dr.FieldValue, 10) when accessing a key field that must be padded.
        // - Always use fieldKey wherever the field date being extracted has normally been hard-coded (usually several times)
        //
        // - Check for blank value - INVALID - return
        // - Check the cache for the fieldValue first
        // - Found the field data in the cache - VALID - return
        // - Not in cache. Pull data from SAP
        // - Cache the all of the data that was pulled ( Normally just one row but some extracts pull all possbiel values: See Get Account()
        // - Check to see if the field and value are in the cache. i.e Pull the single value requested into dtExtract
        // -- In cache = VALID. Not in cahche = INVALID
        //
        // NOTE: private string GetExcelLineLevelData(string fieldName) signature is now GetExcelLineLevelData(string fieldName, int padLength)


        #region DATA EXTRACT TESTS [Extra: As needed for testing and optimization]
        private void DataExtractTests()
        {
            #region TEST COMPANY CODE DATA EXTRACT
            DataTable dt;
            DataRequest dr;
            string elapsedTime = string.Empty;
            TimeSpan ts = new TimeSpan();

            // INIT
            Stopwatch stopWatch = new Stopwatch();

            // TEST COMPANY CODE: BUKRS
            var testValues = new List<string>() { "1000", "1000", "2000", "2000", "3000", "3000", "9999" };
            Debug.WriteLine("---");
            foreach (string value in testValues)
            {
                dr = new DataRequest("BUKRS", value, T_COMPANYCODE);
                stopWatch.Start();
                dt = GetCompanyCode(dr);
                stopWatch.Stop();
                ts = stopWatch.Elapsed;
                elapsedTime = (Double)(ts.Milliseconds / 10) + "ms";
                Debug.WriteLine("COMPANY CODE: FieldName=" + dr.FieldName + " FieldValue=" + dr.FieldValue + " ExtractKey=" + dr.ExtractKey + " Result: " + dr.FieldStatus + " Time: " + elapsedTime + " DataTable is " + dt.Rows.Count.ToString() + "x" + dt.Columns.Count.ToString());
            }

            #endregion
            return;
        }
        #endregion

    }
}