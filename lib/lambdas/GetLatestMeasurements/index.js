import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, GetCommand, QueryCommand } from "@aws-sdk/lib-dynamodb";
import { createHash } from 'crypto';

const CORRECT_PASSWORD_HASH = process.env.PASSWORD_HASH;
const LOCATION_TABLE_NAME = process.env.LOCATION_TABLE_NAME;
const MEASUREMENTS_TABLE_NAME = process.env.MEASUREMENTS_TABLE_NAME;

const docClient = DynamoDBDocumentClient.from(new DynamoDBClient());

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

  const promises = [];
  for (const device of event.devices) {
    promises.push(getLatestData(device));
  }
  const output_data = await Promise.all(promises);

  // Create html page by inserting data into javascript
  var html = getOutputPage(JSON.stringify(output_data))

  console.log("Success: " + JSON.stringify(output_data))
  // A return with these parameters makes the browser treat it as an html page
  return {
    "statusCode": 200,
    "body": html,
    "headers": {
      'Content-Type': 'text/html;charset=utf-8',
    }
  };
}

async function getLatestData(device) {
  const deviceLocationPromise = getDeviceLocation(device);
  const params = {
    TableName: MEASUREMENTS_TABLE_NAME,
    KeyConditionExpression: "device_id = :device_id",
    ExpressionAttributeValues: {
      ":device_id": device,
    },
    ScanIndexForward: false,
    Limit: 1
  };

  console.log("Making latest data request:", params);
  const result = await docClient.send(new QueryCommand(params));
  console.log("Latest data result:", result);

  const item = result.Items[0];
  const temperature = item.temperature;
  const humidity = item.humidity;
  return {
    lastModifiedTime: new Date(item.time).toLocaleString("en-GB", { timeZone: "Europe/London" }),
    temperature: temperature.toFixed(1),
    humidity: humidity.toFixed(1),
    absoluteHumidity: calculateAbsoluteHumidity(temperature, humidity).toFixed(1),
    location: await deviceLocationPromise
  };
}

/**
 * Derive the location of a device from the DynamoDB table
 */
async function getDeviceLocation(device_id) {
  const params = {
    TableName: LOCATION_TABLE_NAME,
    Key: { device_id: device_id }
  };

  console.log("Making location request:", params)
  const result = await docClient.send(new GetCommand(params));
  console.log("Location result:", result);

  const item = result.Item;
  return item?.location ?? device_id; // If the device has no recorded location default to its ID
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

function getOutputPage(measurement_data) {
  return `
<!DOCTYPE html>
<html>
<head>
  <title>Home Measurements</title>
  <!-- icon data to avoid browser making another request -->
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

var data = ${measurement_data};
loopDataTemplateRender(data, 'template_1');

</script>

</body>
</html>
`
}