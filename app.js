/**
 * 
 * Copyright 2020 IBM
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */
// https://www.npmjs.com/package/dotenv
require('dotenv').config();
// https://www.npmjs.com/package/ibm-cos-sdk
const { S3 } = require('ibm-cos-sdk');
// https://www.npmjs.com/package/request
const request = require('request-promise').defaults({ forever: true });
// https://nodejs.org/api/zlib.html
const { unzip } = require('zlib');
// https://nodejs.org/api/util.html
const util = require('util');

async function retryUtil(retriable, triesLeft) {
    console.log('DEBUG: Retry util. Tries left: ' + triesLeft);
    try {
        return await retriable();
    } catch (e) {
        console.log("DEBUG: Caught: ");
        console.log(e);
        newTries = triesLeft - 1;
        if (newTries <= 0) {
            console.log("DEBUG: No tries left. Giving up.");
            throw (e);
        } else {
            return await retryUtil(retriable, newTries);
        }
    }
}

async function sendLogDNA(json, data) {
    console.log('DEBUG: sendLogDNA');
    return await request({
        method: 'POST',
        url: `https://logs.eu-de.logging.cloud.ibm.com/logs/ingest?hostname=${data.host}`,
        body: json,
        auth: {
            user: data.ingestionKey
        },
        headers: { 'Content-Type': 'application/json' },
        json: true,
        timeout: 18000,
        agent: false,
        pool: { maxSockets: 200 }
    });
}

async function download(data) {
    var cos = new S3({
        endpoint: data.endpoint,
        apiKeyId: data.apiKeyId,
        ibmAuthEndpoint: data.ibmAuthEndpoint,
        serviceInstanceId: data.serviceInstanceId
    })

    console.log(`DEBUG: log file = ${data.key}`)
    return await cos.getObject({ Bucket: data.bucketReceiver, Key: data.key }).promise();
}

async function downloadAndSend(data) {
    try {
        const o = await retryUtil(async () => {
            return await download(data);
        }, 3);
        const buffer = Buffer.from(o.Body)
        console.log(`DEBUG: Buffer length = ${buffer.length}`)
        const unzipPromise = util.promisify(unzip)
        const newBuffer = await unzipPromise(buffer)
        const fullDataArray = newBuffer.toString().split(/\r?\n/)

        fullDataArray.pop()
        var i, fj = { lines: [] }
        for (i = 0; i < fullDataArray.length; i++) {
            var json = JSON.parse(fullDataArray[i])
            fj.lines.push({
                timestamp: new Date().getTime(),
                line: '[AUTOMATIC] LOG FROM IBM CLOUD INTERNET SERVICE',
                app: 'cis-cos',
                level: 'INFO',
                meta: {
                    customfield: json
                }
            })
        }
        if (fj.lines.length > 0) {
            return await retryUtil(async () => {
                return await sendLogDNA(fj, data);
            }, 5);
        } else {
            return { "message": "No logs for period." };
        }
    } catch (e) {
        console.error(e);
        process.exit(1);
    }
}

async function main(data) {
    console.time("LogDNA-COS..");
    data.apiKeyId = process.env.__OW_IAM_NAMESPACE_API_KEY;
    data.ibmAuthEndpoint = process.env.__OW_IAM_API_URL;
    const response = await downloadAndSend(data);

    console.log(`DEBUG: downloadAndSend = ${JSON.stringify(response)}`);
    console.timeEnd("LogDNA-COS");
}

exports.main = main;

