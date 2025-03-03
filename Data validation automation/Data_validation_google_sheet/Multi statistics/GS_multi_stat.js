//Adding the script to the menu bar.
function onOpen() {
    var ui = SpreadsheetApp.getUi();
    ui.createMenu('Data validation')
      .addItem('Update validation', 'updateValidation')
      .addItem('Run new validation', 'runNewValidation')
      .addToUi();
  }
  
  //Adding menu items do not accept parameters, hence interim functions are required
  function updateValidation() {
    callCloudRunService(0);
  }
  
  function runNewValidation() {
    callCloudRunService(1);
  }
  
  
  // Function to create a temporary (1hour) identity token to access the backend function.
  // NOTE: the user must be added to data-validation@dj-ds-marketdata-nonprod.iam.gserviceaccount.com as principal 
  // with the Service Account Token Creator and Service Account User roles!!!
  // link: https://console.cloud.google.com/iam-admin/serviceaccounts/details/109206829698862128173/permissions?inv=1&invt=Abm6gA&project=dj-ds-marketdata-nonprod
  
  function getToken()
  {
    var options = {
    'method': 'post',
    'headers': {
      "Authorization": "Bearer " + ScriptApp.getOAuthToken()
    },
    'contentType': 'application/json',
    'payload': JSON.stringify({
      "includeEmail": true,
      "audience": "https://us-central1-dj-ds-marketdata-nonprod.cloudfunctions.net/data_validation"  // Replace with your actual Cloud Function URL or service URL
    })
  };
  
  // Call Google IAM API to generate an ID token for the service account
  var response = UrlFetchApp.fetch("https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/data-validation@dj-ds-marketdata-nonprod.iam.gserviceaccount.com:generateIdToken", options);
  
  // Log the generated ID token
  //Logger.log(JSON.parse(response.getContentText()).token);
  Logger.log("Token generated successfully");
  writeLog("Token generated successfully");
  
  const token = JSON.parse(response.getContentText()).token;
  return token;
  }
  
  // Function to write logs to the "logs" sheet
  function writeLog(message) {
    const sheetName = "logs"; // Name of the logs sheet
    const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    let logSheet = spreadsheet.getSheetByName(sheetName);
    
    // Check if the "logs" sheet exists, if not, create it
    if (!logSheet) {
      logSheet = spreadsheet.insertSheet(sheetName);
      logSheet.appendRow(["Timestamp", "Log Message"]); // Header row
    }
    
    // Append the log entry with a timestamp and message
    logSheet.appendRow([new Date(), message]);
  }
  
  // Function to send key parameters to the backend and retrive the respective report from Screener.
  // If a more complex analysis is required, the backend script decides based on the JIRA ticket ID. If a match is found,
  // it returns the results of the enhanced analysis.
  // If no specific JIRA ticket is submitted, or the one submitted is not linked to an advanced usecase, the respective 
  // Screener report is returned without further manipulation.
  
  function callCloudRunService(NewReport) {
  
    CLOUD_RUN_URL="https://us-central1-dj-ds-marketdata-nonprod.cloudfunctions.net/data_validation";
    // Use the OpenID token inside App Scripts
  
    let token = getToken();
    
    //Get input variables
    var config_sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Config");
    var jira_ID = config_sheet.getRange("B1").getValue();
    var screenID = config_sheet.getRange("B2").getValue();
    var env = config_sheet.getRange("B3").getValue();
  
  
    // Define the data payload with the required parameters for POST, mandatory values.
    const payload = {
        "ScreenNames": screenID,
        "JIRA_ticket": jira_ID,
        "Environment": env
    };
  
    //Passing data_point to be validated to the API, if provided
    var data_point = config_sheet.getRange("B20").getValue();
    // Check if data_point has a value
    if (data_point && data_point !== "") {
      // Trim leading and trailing whitespace from the entire string
      data_point = data_point.trim();
  
      // Check if the input contains a separator
      if (data_point.includes(",")) {
        // Split the input into an array, trim whitespace for each item, and remove any empty items
        var data_list = data_point.split(",").map(function(item) {
          return item.trim();
        });
  
        // Validate that none of the items are empty after trimming
        if (data_list.some(function(item) { return item === ""; })) {
          throw new Error("The input contains invalid entries. Ensure all items are properly formatted.");
        }
  
        // Add the array to the payload
        payload["Data_point"] = data_list;
      } else if (data_point.includes(" ")) {
        // If another separator (like space) is detected, throw an error
        throw new Error("Invalid separator detected. Please use a comma (,) as the separator.");
      } else {
        // Single word case, add it as a single-item array
        payload["Data_point"] = [data_point];
      }
    }
  
    // Define optional parameters
    var param_headers = config_sheet.getRange("A7:A16").getValues().flat(); // Column A: Headers
    var param_values = config_sheet.getRange("B7:B16").getValues().flat();  // Column B: Values
  
    param_headers.forEach((key, index) => {
      var value = param_values[index];
      if (key && value) { // Ensure both key and value are non-empty
        payload[key] = value; // Add non-empty headers and values to the payload
      }
    });
  
    const options = {
      'method' : 'post',
      'contentType': 'application/json',
      'headers': {
          'Authorization': 'Bearer ' + token,
          'Content-Type': 'application/json'
      },
        'payload': JSON.stringify(payload),  // Sending the parameters as a JSON payload
        'muteHttpExceptions': true    
    };
  
    // call the remote function and return JSON response
    try {
          // Call the Cloud Run function
          const response = UrlFetchApp.fetch(CLOUD_RUN_URL + '/data_validation', options);
          const responseBody = response.getContentText();
  
          // Parse the entire response
          const jsonResponse = JSON.parse(responseBody);
  
          // Check if "core_data" and "contextual_data" keys exist
          if (!jsonResponse.core_data || jsonResponse.contextual_data=== undefined) {
              Logger.log(jsonResponse);
              writeLog(jsonResponse);
              Logger.log("Invalid response structure. Ensure 'core_data' and 'contextual_data' are returned.");
              writeLog("Invalid response structure. Ensure 'core_data' and 'contextual_data' are returned.");
              return;
          }
  
          // Check if "core_data" has the split structure
          if (!jsonResponse.core_data.columns || !jsonResponse.core_data.data) {
              Logger.log("Invalid 'core_data' structure. Ensure 'split' format is returned.");
              writeLog("Invalid 'core_data' structure. Ensure 'split' format is returned.");
              return;
          }
  
          // Extract columns and data
          const columns = jsonResponse.core_data.columns;
          const data = jsonResponse.core_data.data;
  
          // Check if the "Data" sheet exists and clear old data
          var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Screener data");
          if (!sheet) {
              Logger.log("Error: 'Screener data' sheet not found.");
              writeLog("Error: 'Screener data' sheet not found.");
              return;
          }
          //Only clear the whole sheet if it's a new report
          if (NewReport == 1) {
              sheet.clearContents();
              sheet.clearFormats();
          }
  
          // Write the headers
          const headerRange = sheet.getRange(1, 1, 1, columns.length);
          headerRange.setValues([columns]);
          headerRange.setFontColor("white") // Set text color to white
                    .setBackground("darkblue") // Set background color to dark blue
                    .setHorizontalAlignment("center") // Center align text
                    .setVerticalAlignment("middle") // Center align text vertically
                    .setFontWeight("bold"); // Make text bold
  
          // Write the data rows
          const dataRange = sheet.getRange(2, 1, data.length, columns.length);
          dataRange.setValues(data);
  
          // Apply formatting to data rows
          dataRange.setBorder(true, true, true, true, true, true) // Add borders to all cells
                  .setWrap(true) // Enable text wrapping
                  .setHorizontalAlignment("left") // Align text to left for better readability
                  .setHorizontalAlignment("left") // Align text horizontally to the left
                  .setVerticalAlignment("middle"); // Align text vertically to the middle
  
          // Auto-fit columns to match content
          sheet.autoResizeColumns(1, columns.length);
  
          // Extract "contextual_data"
          const contextualData = jsonResponse.contextual_data;
  
          // Ensure "contextual_data" exists and is not null or undefined
          if (contextualData) {
              // Write "contextual_data" to the "Overall statistics" sheet at position A5
              const sheetName = "Overall statistics";
              const stat_sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(sheetName);
  
              if (stat_sheet) {
                  // Clear previous data in the range starting at A5
                  if (NewReport == 1) {
                      stat_sheet.getRange("A5:Z").clearContent(); // Clears a wide range
                  } else {
                      stat_sheet.getRange("A5:B").clearContent(); // Clears a smaller range
                  }
  
                  // Convert contextual data to a 2D array
                  // If contextual_data is an array of objects, map each row to its values
                  let contextualDataArray;
                  if (Array.isArray(contextualData)) {
                      contextualDataArray = contextualData.map(Object.values);
                  } else {
                      contextualDataArray = Object.entries(contextualData);
                  }
  
                  // Determine the number of rows and columns dynamically
                  const numRows = contextualDataArray.length;
                  const numCols = contextualDataArray[0]?.length || 0;
  
                  if (numRows > 0 && numCols > 0) {
                      // Write contextual data to the sheet starting at A5
                      stat_sheet.getRange(5, 1, numRows, numCols).setValues(contextualDataArray);
                      Logger.log("Contextual data written to 'Overall statistics' sheet starting at A5.");
                  } else {
                      Logger.log("No valid data to write to the sheet.");
                  }
              } else {
                  Logger.log(`Sheet '${sheetName}' does not exist.`);
                  writeLog(`Sheet '${sheetName}' not found.`);
              }
          } else {
              Logger.log("contextual_data is null or undefined.");
              writeLog("contextual_data is null or undefined."); // Log or handle this case as needed
          }
  
          
          Logger.log("Data successfully written to the sheet!");
          writeLog("Data successfully written to the sheet!");
  
      } catch (error) {
          Logger.log("Error calling Cloud Run: " + error);
          writeLog("Error calling Cloud Run: " + error);
  
      }
  
  }