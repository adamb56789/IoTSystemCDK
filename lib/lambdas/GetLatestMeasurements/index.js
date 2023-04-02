import { DynamoDB, SSM, IotData } from "aws-sdk";
import { createHash } from 'crypto';
import { readFileSync } from 'fs';

const docClient = new DynamoDB.DocumentClient();
const iotdata = new IotData({ endpoint: process.env.IOT_ENDPOINT });
const CORRECT_PASSWORD_HASH = process.env.PASSWORD_HASH;
const LOCATION_TABLE_NAME = process.env.TABLE_NAME;

/**
 * Derive the location of a device from the DynamoDB table
 */
async function getDeviceLocation(device_id) {
  const ddb_params = {
    TableName: LOCATION_TABLE_NAME,
    Key: { device_id: device_id }
  };

  const awsRequest = await docClient.get(ddb_params);
  const result = await awsRequest.promise();
  return result;
}

/**
 * Derive the location of a device from the DynamoDB table
 * 
 * @param list of device IDs
 */
function getLatestMessages(devices) {
  var requests = [];
  for (let device of devices) {
    var params = {
      topic: device
    };

    requests.push(iotdata.getRetainedMessage(params, function (err, data) {
      if (err) console.log(err, err.stack);
    }).promise());
  }

  return Promise.all(requests);
}

/**
 * Estimates the absolute humidity in g/m3 using a formula I found on the internet
 * 
 * @param temperature in C
 * @param humidity - relative humidity as a percentage
 */
function calculateAbsoluteHumidity(temperature, humidity) {
  var absoluteHumidity = (6.112 * Math.exp((17.67 * temperature) / (temperature + 243.5)) * humidity * 2.1674) / (273.15 + temperature);
  return Math.round((absoluteHumidity + Number.EPSILON) * 100) / 100;
}

/**
 * Formats the required data into a format for display
 * 
 * @param {message} an object containing an IoT message of a measurement
 */
async function transformData(message) {
  var output = {};
  output.lastModifiedTime = new Date(message.lastModifiedTime).toLocaleString("en-GB", { timeZone: "Europe/London" });

  // Numbers to strings with fixed decimal places
  // 18 becomes 18.0 for consistent format
  output.temperature = message.temperature.toFixed(1);
  output.humidity = message.humidity.toFixed(1);
  output.absoluteHumidity = calculateAbsoluteHumidity(message.temperature, message.humidity).toFixed(1);

  var item = (await getDeviceLocation(message.topic)).Item;
  // If the device has no recorded location default to its ID
  if (item) {
    output.location = item.location;
  } else {
    output.location = message.topic;
  }
  return output;
}

export async function handler(event, context) {
  // If is a lambda url extract the url params
  if (event.hasOwnProperty('queryStringParameters')) {
    event = event.queryStringParameters;
    event.devices = event.devices.split(',');
  }

  if (!(event.hasOwnProperty('password') && event.hasOwnProperty('devices'))) {
    throw ("Incorrect parameters");
  }

  const hashed_password = createHash('sha256').update(event.password).digest('hex');
  if (hashed_password != CORRECT_PASSWORD_HASH) {
    throw ("Incorrect password");
  }

  // Can't remember why I added this check. Leaving it in because it potentially stops abuse
  if (event.devices.length > 10) {
    throw ("Too many devices");
  }

  console.log('Received event: ', JSON.stringify(event.devices));

  var messages = await getLatestMessages(event.devices);

  var output_data = [];
  for (let message of messages) {
    Object.assign(message, JSON.parse(message.payload.toString()));
    output_data.push(await transformData(message));
  }

  // Replace MEASUREMENT_DATA from the html file with the actual data so it is loaded into the table automatically
  var content = MEASUREMENTS_OUTPUT_PAGE.replace('MEASUREMENT_DATA', JSON.stringify(output_data));

  console.log("Success: " + JSON.stringify(output_data))
  // A return with these parameters makes the browser treat it as an html page
  return {
    "statusCode": 200,
    "body": content,
    "headers": {
      'Content-Type': 'text/html;charset=utf-8',
    }
  };
}

const MEASUREMENTS_OUTPUT_PAGE = `
<!DOCTYPE html>
<html>
<head>
  <title>Home Measurements</title>
  <link rel="icon" href="data:,">
  <style>
    html, body, form, fieldset, table, tr, td, img {
      font: 100%/150% calibri,helvetica,sans-serif;
    }
    table {
      border:1px solid #b3adad;
      border-collapse:collapse;
      padding:5px;
    }
    table th {
      border:1px solid #b3adad;
      padding:5px;
      background: #f0f0f0;
      color: #313030;
    }
    table td {
      border:1px solid #b3adad;
      text-align:center;
      padding:5px;
      background: #ffffff;
      color: #313030;
    }
  </style>
</head>

<body>


<table render-area="template_1">
    <thead>
        <tr>
            <th class="">Location</th>
            <th class="text-center">Temperature °C</th>
            <th class="text-center">Humidity %</th>
            <th class="text-center">Humidity g/m<sup>3</sup></th>
            <th class="text-center">Time (GMT)</th>
        </tr>
    </thead>
    <tbody render-action="loop">
        <tr>
            <td>{!location!}</td>
            <td>{!temperature!}</td>
            <td>{!humidity!}</td>
            <td>{!absoluteHumidity!}</td>
            <td>{!lastModifiedTime!}</td>
        </tr>
    </tbody>
</table>

<script>
function loopDataTemplateRender(data, renderTemplate) {

    var template = document.querySelector('[render-area="' + renderTemplate + '"]');
    var loopElm = template.querySelector('[render-action="loop"]');
    _loopArea = loopElm;
    if((template || null != template) && loopElm){
      if(Object.keys(data).length){
        var nodataElm = loopElm.querySelector('[render-data="nodata"]');
        if(nodataElm){
          nodataElm.style.display = 'none';
        }
        var keyRpStr;
        var WordRegEx;
        var loopAreaHtml = loopElm.innerHTML;
        loopElm.innerHTML = 'Loading...';
        var replaceRow = '';
        var loopStr = '';
        data.forEach(function(ditem) {
            replaceRow = loopAreaHtml;
            Object.keys(ditem).forEach(function(dataKey) {
                keyRpStr = '{!' + dataKey + '!}';
                WordRegEx = new RegExp(keyRpStr,'g');
                replaceRow = replaceRow.replace(WordRegEx, ditem[dataKey]);
                return false;
            })
            loopStr += replaceRow;
        });
        loopElm.innerHTML = loopStr;
      }else{
        var dataElm = loopElm.querySelector('[render-data="data"]');
        if(dataElm){
          dataElm.style.display = 'none';
        }else{
          loopElm.childNodes[0].style.display = 'none';
        }
      }
    }
}

var data = MEASUREMENT_DATA;
loopDataTemplateRender(data, 'template_1');

</script>

</body>
</html>
`