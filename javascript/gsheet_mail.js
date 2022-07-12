// execute a BQ query
function queryBigQuery(query_string) {
  var project_id = 'segment-data' ; 
  var request = {
    query: query_string, 
    useLegacySql: false
  };
  var query_results = BigQuery.Jobs.query(request, project_id);
  var job_id = query_results.jobReference.jobId;
  
  // Check on status of the Query Job.
  var sleep_time_ms = 500;
  while (!query_results.jobComplete) {
    Utilities.sleep(sleep_time_ms);
    sleep_time_ms *= 2;
    query_results = BigQuery.Jobs.getQueryResults(project_id, job_id);
  }
  
  // Get all the rows of results.
  var rows = query_results.rows;
  while (query_results.pageToken) {
    query_results = BigQuery.Jobs.getQueryResults(project_id, job_id, {
      pageToken: query_results.pageToken
    });
    rows = rows.concat(query_results.rows);
  }
  
  if (rows) {
    // Append the results.
    var data = new Array(rows.length);
    for (var i = 0; i < rows.length; i++) {
      var cols = rows[i].f;
      data[i] = new Array(cols.length);
      for (var j = 0; j < cols.length; j++) {
        data[i][j] = cols[j].v;
      }
    }
    return data ; 
  } else {
    return null ;
  }  
}



function main() {
  var spreadsheet = SpreadsheetApp.openById('INSERT_GSHEET_ID') ; 
  var sheet = spreadsheet.getSheetByName('SHEET_NAME') ;
  var query_string = '\
  WITH tour AS (\
  SELECT\
    id,\
    name,\
    tour_group_id\
  FROM\
    `segment-data.prod_replica_bigquery._tour`\
  ),\
  tour_group AS (\
  SELECT\
    id,\
    name,\
    city\
  FROM\
    `segment-data.prod_replica_bigquery._tour_group`\
  ),\
  available_tours AS (\
  SELECT\
    DISTINCT(available_tours.tour_id) as tour_id\
  FROM\
    (\
      SELECT\
        distinct(tour_id) as tour_id\
      FROM\
        `segment-data.inventory._inventory_slot`\
      WHERE\
        EXTRACT(DATE FROM inventory_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) and\
        closed = false and\
        price_profile_id is not null\
    ) available_tours\
    LEFT JOIN\
      `segment-data.inventory.calipso_tour` on available_tours.tour_id = `segment-data.inventory.calipso_tour`.id\
  WHERE\
    `segment-data.inventory.calipso_tour`.status = "ACTIVE"\
  ),\
  vendor_tour AS (\
  SELECT\
    vendor_tour.tour_id,\
    vendor_tour.inventory_automated,\
    vendor_tour.fulfillment_type,\
    vendor_detail.name AS vendor_name\
  FROM\
    (\
      SELECT\
        DISTINCT(tour_id) AS tour_id,\
	    FIRST_VALUE(vendor_detail_id) OVER(PARTITION BY tour_id ORDER BY priority ASC) as vendor_detail_id,\
        CASE WHEN (FIRST_VALUE(is_inventory_automated) OVER(PARTITION BY tour_id ORDER BY priority ASC)) = false THEN "No" ELSE "Yes" END as inventory_automated,\
        FIRST_VALUE(fulfillment_type) OVER(PARTITION BY tour_id ORDER BY priority ASC) as fulfillment_type\
      FROM\
        `segment-data.prod_replica_bigquery._vendor_tour`\
    ) vendor_tour\
  LEFT JOIN\
    `segment-data.prod_replica_bigquery._vendor_detail` AS vendor_detail\
  ON vendor_tour.vendor_detail_id = vendor_detail.id\
  ),\
  tour_details AS (\
  SELECT\
    available_tours.tour_id,\
    tour_group.id AS tour_group_id,\
    tour.name AS tour_name,\
    tour_group.name AS product_name,\
    vendor_tour.inventory_automated,\
    vendor_tour.fulfillment_type,\
    vendor_tour.vendor_name,\
    tour_group.city\
  FROM\
    available_tours\
  LEFT JOIN\
    tour ON available_tours.tour_id = tour.id\
  LEFT JOIN\
    tour_group ON tour.tour_group_id = tour_group.id\
  LEFT JOIN\
    vendor_tour ON tour.id = vendor_tour.tour_id\
  ORDER BY 1\
  )\
  SELECT\
    *\
  FROM\
    tour_details'
 
 
  var query_result = queryBigQuery(query_string) ;
  
  read_slots()
  
  if (query_result.length > 0) {
    // clear existing content
    sheet.getRange(2, 1, sheet.getLastRow(), sheet.getLastColumn()).clearContent() ; 
    
    // paste new values
    sheet.getRange(2, 1, query_result.length, query_result[0].length).setValues(query_result) ;
    
    
  num_slots()
  slots_previous()
  difference()
  buckets()
  Utilities.sleep(22000)
  query_function()
  Utilities.sleep(30000)
  email()
  }
}


// Makes an API call to fetch available slots for tour_ids
function num_slots() {
  var spreadsheet = SpreadsheetApp.openById('INSERT_GSHEETID').getSheetByName('TourId_Details') ;
  var data = spreadsheet.getDataRange().getDisplayValues() ;
  
  for (i in data) {
    var tour_id = data[i][0] ;
    var inventory_slot = data[i][9] ;
    if (tour_id != 'tour_id' && inventory_slot == '') {
      var url = "API_URL" + tour_id ; //build the url for inventory API call. 
      
      try {
        var response = UrlFetchApp.fetch(url).getContentText() ; //make the API call 
      } catch(e) {
        var row_num = parseInt(i) + 1;
        spreadsheet.getRange(row_num, 9).setValue('returned_error') ;
        continue 
      }
      response = JSON.parse(response) ; //parse the JSON response
    
      var num_inventory_slots = response.total //total number of inventory slots 
      var row_num = parseInt(i) + 1;
      spreadsheet.getRange(row_num, 9).setValue(num_inventory_slots) ;
    }
  }
}


// Pastes previous slot values before they get refreshed
function read_slots() {
  var sheetFrom = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("TourId_Details");
  var sheetTo = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Slot Log");

  // Copy from 1st row, 1st column, all rows for one column 
  var tour_ids = sheetFrom.getRange(1, 1, sheetFrom.getLastRow(), 1).getValues();
  var slots = sheetFrom.getRange(1, 9, sheetFrom.getLastRow(), 1).getValues();

  sheetTo.getRange(1, 1, sheetTo.getLastRow(), sheetTo.getLastColumn()).clearContent() ; 

  //Paste to another sheet from first cell onwards
  sheetTo.getRange(1,1,tour_ids.length,1).setValues(tour_ids);
  sheetTo.getRange(1,2,slots.length,1).setValues(slots);
}

function slots_previous(){
  // create an array the same size as the number of rows.
  var spreadsheet = SpreadsheetApp.openById('INSERT_GSHEET_ID').getSheetByName('TourId_Details') ;
  var num = spreadsheet.getLastRow() - 1;
  var data = new Array(num);
  // populate the array with the formulas.
  for (var i=0; i < num; i++)
  {
    // note that as usual, each element of the array must itself be an array 
    // that has as many elements as columns. (1, in this case.)
    data[i] = ["=vlookup(A" + (i+2).toString() + ",'Slot Log'!A:B,2,0)"];
  }
  // set the column values.
  spreadsheet.getRange(2,10,spreadsheet.getLastRow() - 1,1).setFormulas(data);
}  


function difference(){
  // create an array the same size as the number of rows.
  var spreadsheet = SpreadsheetApp.openById('INSERT_GSHEET_ID').getSheetByName('TourId_Details') ;
  var num = spreadsheet.getLastRow() - 1;
  var data = new Array(num);
  // populate the array with the formulas.
  for (var i=0; i < num; i++)
  {
    // note that as usual, each element of the array must itself be an array 
    // that has as many elements as columns. (1, in this case.)
    data[i] = ["=I" + (i+2).toString() + "-J" + (i+2).toString()];
  }
  // set the column values.
  spreadsheet.getRange(2,11,spreadsheet.getLastRow() - 1,1).setFormulas(data);
}  


function buckets(){
  // create an array the same size as the number of rows.
  var spreadsheet = SpreadsheetApp.openById('INSERT_GSHEET_ID').getSheetByName('TourId_Details') ;
  var num = spreadsheet.getLastRow() - 1;
  var data = new Array(num);
  // populate the array with the formulas.
  for (var i=0; i < num; i++)
  {
    // note that as usual, each element of the array must itself be an array 
    // that has as many elements as columns. (1, in this case.)
    data[i] = ["=IF(AND(I" + (i+2).toString() + ">=0,I" + (i+2).toString() + "<=10,K" + (i+2).toString() + "<-6),\"Yes\",IF(AND(I" + (i+2).toString() + ">=11,I" + (i+2).toString() + "<=50,K" + (i+2).toString() + "<-8),\"Yes\",IF(AND(I" + (i+2).toString() + ">=51,I" + (i+2).toString() + "<=150,K" + (i+2).toString() + "<-10),\"Yes\",IF(AND(I" + (i+2).toString() + ">=151,I" + (i+2).toString() + "<=400,K" + (i+2).toString() + "<-15),\"Yes\",IF(AND(I" + (i+2).toString() + ">=401,I" + (i+2).toString() + "<=1000,K" + (i+2).toString() + "<-20),\"Yes\",IF(AND(I" + (i+2).toString() + ">=1001,K" + (i+2).toString() + "<-40),\"Yes\",\"No\"))))))"];
    
    
  }
  // set the column values.
  spreadsheet.getRange(2,12,spreadsheet.getLastRow() - 1,1).setFormulas(data);
}


function query_function(){
  // create an array the same size as the number of rows.
  var spreadsheet = SpreadsheetApp.openById('INSERT_GSHEET_ID').getSheetByName('Mail') ;
  var cell1 = spreadsheet.getRange("A2");
  var cell2 = spreadsheet.getRange("A17");
  cell1.setFormula("=QUERY('TourId_Details'!A:K,\"SELECT A,B,C,D,E,F,G,H,I,J,K  WHERE K>1 ORDER BY K DESC LIMIT 12\")");
  cell2.setFormula("=QUERY('TourId_Details'!A:L,\"SELECT A,B,C,D,E,F,G,H,I,J,K WHERE L='Yes' and K is not null ORDER BY K ASC\")");
}  



function email() {
    var ss_data = getData();
    var emails = 'EMAIL_LIST_COMMA_SEPARATED';
    var cc = 'EMAIL_LIST_COMMA_SEPARATED';
    var nowDate = new Date(); 
    var date = nowDate.getFullYear()+'/'+(nowDate.getMonth()+1)+'/'+nowDate.getDate();
    var data = ss_data[0];
    var background = ss_data[1];
    var fontColor = ss_data[2];
    var fontStyles = ss_data[3];
    var fontWeight = ss_data[4];
    var fontSize = ss_data[5];
    var html = "<br>Find below the list of tours with the highest change in the number of inventory slots compared from previous run. Get this data for all the tours"  +  "<table border='2'>";
    for (var i = 0; i < data.length; i++) {
        html += "<tr>"
        for (var j = 0; j < data[i].length; j++) {
            html += "<td style='height:20px;background:" + background[i][j] + ";color:" + fontColor[i][j] + ";font-style:" + fontStyles[i][j] + ";font-weight:" + fontWeight[i][j] + ";font-size:" + (fontSize[i][j] + 2) + "px;'>" + data[i][j] + "</td>";
        }
        html += "</tr>";
    }
    html + "</table>"
    MailApp.sendEmail({
        to: emails ,
        cc: cc,
        subject: 'Inventory Slots Update - ' + date,
        htmlBody: html
    })
}


function getData(){
  var ss = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Mail").getDataRange();
  var background = ss.getBackgrounds();
  var val = ss.getDisplayValues();
  var fontColor = ss.getFontColors();
  var fontStyles = ss.getFontStyles();
  var fontWeight = ss.getFontWeights();
  var fontSize = ss.getFontSizes();
  return [val,background,fontColor,fontStyles,fontWeight,fontSize];
}
