/**
*
* Change in Campaign Spending Alert
*
* This script uses the current hour to calculate how much has been spent on
* individual campaigns on the day of running. The average spend up to the
* current hour in a specified numbers of days previously is averaged. If the
* spend today is higher by a specified percentage threshold an alert email
* is sent.
*
* There is a 20 minute delay between events occurring and the data being
* available in AdWords. This script should be scheduled to run after 20
* past the hour.
*
* Version: 1.0
* Google AdWords Script maintained on brainlabsdigital.com
*
**/

function main() {
  //////////////////////////////////////////////////////////////////////////////
  // Options 

  var campaignNameDoesNotContain = [];
  // Use this if you want to exclude some campaigns.
  // For example ["Display"] would ignore any campaigns with 'Display' in the name,
  // while ["Display","Shopping"] would ignore any campaigns with 'Display' or
  // 'Shopping' in the name.
  // Leave as [] to not exclude any campaigns.

  var campaignNameContains = [];
  // Use this if you only want to look at some campaigns.
  // For example ["Brand"] would only look at campaigns with 'Brand' in the name,
  // while ["Brand","Generic"] would only look at campaigns with 'Brand' or 'Generic'
  // in the name.
  // Leave as [] to include all campaigns.

  var webhook_url = ["INSERT_SLACK_WEB_HOOK_URL"];

  //////////////////////////////////////////////////////////////////////////////
  // Thresholds

  var percentageDifferenceSpend = getThreshold().percentageThreshold;
  
  // The positive or negative percentage change in spend must be greater than
  // this number for an alert to be sent. eg 10 means greater than a positive or negative
  // 10% change. The variable must be positive.

  var minimumDifferenceFromAvgSpend = getThreshold().absoluteThreshold;
  
  
  var slackReference = getThreshold().slackReference;
  
  // This value sets a minimum value that the average historic spend should be
  // for it to be compared to today's spend. This number must be greater than 0.

  var earliestHour = 0;
  // Restricts the script to run only after a certain hour of the day so that a
  // significant amount of data can be gathered. This number should be 0 - 23.

  //////////////////////////////////////////////////////////////////////////////
  // Advanced settings

  var timePeriod = 27;
  // The default time period averages the previous 7 days of spending. This number
  // must be greater than 0.

  //////////////////////////////////////////////////////////////////////////////  
  // The actual code starts here

  // Validate input
  var validated = validateInput(
    webhook_url,
    percentageDifferenceSpend,
    minimumDifferenceFromAvgSpend,
    earliestHour,
    timePeriod
  );

  if (validated !== true) {
    throw validated;
  }

  // Create date strings for AWQL query and data comparison
  var dates = makeDates(timePeriod);
  
  // Check if it's too early to run the script or not
  if (dates.currentHour < earliestHour) {
    Logger.log('Too early for code, need coffee.');
    return;
  }

  // Get the IDs of the campaigns to look at
  var ignorePausedCampaigns = true;
  var activeCampaignIds = getCampaignIds(campaignNameDoesNotContain, campaignNameContains, ignorePausedCampaigns);
 
  // Construct the AWQL query using the campaign IDs and dates
  var query = constructQuery(activeCampaignIds, dates);
  
  var queryReport = AdWordsApp.report(query);
  
  // Calculate sum of spend today and historically by campaign ID
  var costs = calculateCostByCampaign(queryReport, dates);
  
  // Generate a dictionary of overspending campaigns
  var overSpendingCampaigns = checkPercentageChange(costs, minimumDifferenceFromAvgSpend, timePeriod, percentageDifferenceSpend);

  // Do nothing if there are no overspending campaigns
  if (Object.keys(overSpendingCampaigns).length === 0) {
    Logger.log('No overspending campaigns.');
    return;
  }
  
  Logger.log('Overspending campaigns: ' + JSON.stringify(overSpendingCampaigns));
  
  // Notify contacts if there are overspending campaigns
  notifyContact(webhook_url, overSpendingCampaigns, percentageDifferenceSpend, dates.currentHour,slackReference);
}


function validateInput(webhook_url,
  percentageDifferenceSpend,
  minimumDifferenceFromAvgSpend,
  earliestHour,
  timePeriod
) {

  if (webhook_url.length === 0) {
    return 'Please provide at least one email address to notify.';
  }

  if (percentageDifferenceSpend <= 0) {
    return 'Please provide a positive percentage difference spend.';
  }

  if (minimumDifferenceFromAvgSpend <= 0) {
    return 'Please provide a positive minimum threshold.';
  }

  if (earliestHour > 23 | earliestHour < 0) {
    return 'Please provide an earliest hour between 0 and 23 inclusive.'
  }

  if (timePeriod < 1) {
    return 'Please provide a time period of at least one day.'
  }

  return true;
}


function getThreshold() {
  var spreadsheet = SpreadsheetApp.openById('INSERT_GSHEET_ID');
  var sheet = spreadsheet.getSheetByName('Thresholds');
  var range = sheet.getRange(2,1,sheet.getLastRow(),sheet.getLastColumn()).getDisplayValues();
  var accountName = AdWordsApp.currentAccount().getName();
  
  for(var i in range){
    if(range[i][0] == accountName){
      var percentagethreshold = range[i][1];
      var absolutethreshold = range[i][2];
      var slackReference = range[i][3];
    }
  }
  return {
    'percentageThreshold': percentagethreshold,
    'absoluteThreshold': absolutethreshold,
    'slackReference': slackReference
  };
}



function notifyContact(webhook_url, overSpendingCampaigns, threshold, hour,slackReference) {
  var accountName = AdWordsApp.currentAccount().getName();
  var subject = accountName + ' Campaigns exceeded threshold.';
  var body = 'The following campaigns have exceeded the ' + threshold + '% spend threshold:\n\n';
  
  var campaignIds = Object.keys(overSpendingCampaigns);
  
  for (var i = 0; i < campaignIds.length; i++) {
    var campaignId = campaignIds[i];
    var campaign = overSpendingCampaigns[campaignId];
    var campaignName = campaign.campaignName;
    var percentageChange = campaign.percentageChange.toFixed(2);
    var absoluteChange = campaign.absoluteChange.toFixed(2);
    var spendToday = campaign.today.toFixed(2);
    var budget = campaign.Budget;

    body += (i+1) + '. \nName: ' + campaignName + '\n' +
      'Change(%): ' + percentageChange + '\n' +
      'Change(abs): ' + absoluteChange + '\n' +
      'Hour: ' + hour + '\n' +
      'Daily Budget ($): ' + budget + '\n' +
      'Spend ($): ' + spendToday + '\n\n';
  }
  var payload = {
        "username": subject,
        "icon_emoji": ":warning:",
        "link_names": 1,
        "text":"@" + slackReference + "\n" + body
      };
      var url = webhook_url;
      var options = {
        'method': 'post',
        'payload': JSON.stringify(payload)
      };
      var response = UrlFetchApp.fetch(url,options);
      Logger.log(response)
}



function checkPercentageChange(costs, spendThreshold, timePeriod, percentageThreshold) {
  var campaignIds = Object.keys(costs);

  return campaignIds.reduce(function(overspendingCampaigns, campaignId){
    var campaign = costs[campaignId];
    var averageSpend = campaign.sumTimePeriod/4;
    var spendToday = campaign.today;

    if((spendToday - averageSpend) < spendThreshold){
      return overspendingCampaigns;
    }

    var percentageChange = ((spendToday - averageSpend) / averageSpend) * 100;
    var absoluteChange = spendToday - averageSpend;
    
    if (Math.abs(percentageChange) > percentageThreshold) {
      campaign['percentageChange'] = percentageChange;
      campaign['absoluteChange'] = absoluteChange;
      overspendingCampaigns[campaignId] = campaign;
    }

    return overspendingCampaigns;
  }, {}); 
}



function makeDates(timePeriod) {
  var millisPerDay = 1000 * 60 * 60 * 24;
  var timeZone = AdWordsApp.currentAccount().getTimeZone();

  var now = new Date();
  var dayOfWeek = new Date().getDay();
  var dateInPast = new Date(now - ((timePeriod + 1) * millisPerDay));

  var todayHyphenated = Utilities.formatDate(now, timeZone, 'yyyy-MM-dd');
  var todayFormatted =  todayHyphenated.replace(/-/g, '');
  var currentHour = Number(Utilities.formatDate(now, timeZone, 'H')) - 1;
  var dayToday = isNaN(dayOfWeek) ? null : ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'][dayOfWeek];

  var dateInPastFormatted = Utilities.formatDate(dateInPast, timeZone, 'yyyyMMdd');

  return {
    'todayHyphenated': todayHyphenated,
    'todayFormatted': todayFormatted,
    'dateInPastFormatted': dateInPastFormatted,
    'currentHour': currentHour,
    'dayToday' : dayToday,
  };

}



function constructQuery(activeCampaignIds, dates) {
  var currentHour = dates.currentHour;
  var todayFormatted = dates.todayFormatted;
  var dateInPastFormatted = dates.dateInPastFormatted;

  var query = 
    'SELECT CampaignName, CampaignId, Amount, Cost, DayOfWeek, Date ' +
    'FROM CAMPAIGN_PERFORMANCE_REPORT ' + 
    'WHERE CampaignId IN [' + activeCampaignIds.join(',') + '] ' +
    'AND CampaignStatus = ENABLED ' +
    'AND HourOfDay = ' + currentHour + ' ' +  
    'DURING ' + dateInPastFormatted + ',' + todayFormatted;

  return query;
}


function calculateCostByCampaign(report, dates) {
  var reportRows = report.rows();
  var costs = {};
  var dayToday = dates.dayToday;
  
  while(reportRows.hasNext()) {
    var row = reportRows.next();
    var cost = parseFloat(row.Cost);
    var DayOfWeek = row.DayOfWeek;
    var campaignId = row.CampaignId;
    var amount = row.Amount;
    
    if (costs[campaignId] === undefined) {
        costs[campaignId] = {
          'today': 0,
          'Budget': 0,
          'sumTimePeriod': 0,
          'campaignName': row.CampaignName,
        }
    }
     
    
    if (row.Date === dates.todayHyphenated) {
      costs[campaignId].today += cost;
      costs[campaignId].Budget += amount;
    } else if (row.Date !== dates.todayHyphenated && DayOfWeek === dayToday) {
      costs[campaignId].sumTimePeriod += cost;
    }
  }
  return costs;
}



function getCampaignIds(campaignNameDoesNotContain, campaignNameContains, ignorePausedCampaigns) {
  var whereStatement = "WHERE ";
  var whereStatementsArray = [];
  var campaignIds = [];

  if (ignorePausedCampaigns) {
    whereStatement += "CampaignStatus = ENABLED ";
  } else {
    whereStatement += "CampaignStatus IN ['ENABLED','PAUSED'] ";
  }

  for (var i=0; i<campaignNameDoesNotContain.length; i++) {
    whereStatement += "AND CampaignName DOES_NOT_CONTAIN_IGNORE_CASE '" + campaignNameDoesNotContain[i].replace(/"/g,'\\\"') + "' ";
  }

  if (campaignNameContains.length == 0) {
    whereStatementsArray = [whereStatement];
  } else {
    for (var i=0; i<campaignNameContains.length; i++) {
      whereStatementsArray.push(whereStatement + 'AND CampaignName CONTAINS_IGNORE_CASE "' + campaignNameContains[i].replace(/"/g,'\\\"') + '" ');
    }
  }

  for (var i=0; i<whereStatementsArray.length; i++) {
    var campaignReport = AdWordsApp.report(
      "SELECT CampaignId " +
      "FROM   CAMPAIGN_PERFORMANCE_REPORT " +
      whereStatementsArray[i] +
      "DURING LAST_30_DAYS");

    var rows = campaignReport.rows();
    while (rows.hasNext()) {
      var row = rows.next();
      campaignIds.push(row['CampaignId']);
    }
  }

  if (campaignIds.length == 0) {
    throw("No campaigns found with the given settings.");
  }
  return campaignIds;
}
