require("dotenv").config();
const protobuf = require('protobufjs');
const path = require("path");
const fs = require('fs')

const videoIntelligence = require('@google-cloud/video-intelligence').v1;
const {Storage} = require('@google-cloud/storage');
const serviceAccount = require('./config/service_account');
const { writeFile } = require("fs/promises");
const storage = new Storage({
    credentials: serviceAccount
})
const bucketName = 'rtme-videos';
const inputFileName = 'den_og.mp4';
const folderName = 'coordinates'


let jobData = [];
let waitingJobData = [];
let inProgressJobData = [];

const client = new videoIntelligence.VideoIntelligenceServiceClient({
    credentials: serviceAccount
});

async function downloadFile(bucketName, fileName, destFileName) {
    const options = {
      destination: destFileName,
    };

    // Downloads the file
    await storage.bucket(bucketName).file(fileName).download(options)
}
  


async function detectFacesInVideo(bucketName, fileName) {
    try {
        const time = new Date().getTime()
        // This will be refered in local
        const localFile = `${fileName}-${time}.json`

        // This refers to the Cloud storage
        const destinationFileName = `gs://${bucketName}/${localFile}`

        // This refers to the local storage
        const localFileDestination = `${folderName}/${localFile}`
        const inputFileUri = `gs://${bucketName}/${fileName}`
        const request = {
          inputUri: inputFileUri,
          features: ['FACE_DETECTION'],
            outputUri: destinationFileName,
            videoContext: {
                faceDetectionConfig: {
                    includeBoundingBoxes: true
                }
            }
        };
      
        // Perform the face detection request
        const [operation] = await client.annotateVideo(request);
        console.log('operation name => ', operation?.name)
        if (operation?.name) jobData.push({
            id: operation?.name,
            destinationFileName,
            inputFileName: `${fileName}-${time}.json`,
            inputFileUri,
            localFileDestination,
            localFile
        })
    } catch (e) {
        console.error(e)
    }
}

// Check if new job ID came
setInterval(() => {
    if (jobData.length > 0) {
        const tempJobIds =[]
        for (let i = 0; i < jobData.length; i++) {
            tempJobIds.push(jobData[i])
        }
        if (tempJobIds.length) {
            console.log('New Jobs detected, moved to check status')
            waitingJobData.push(...tempJobIds)
            jobData = jobData.filter((job) => !tempJobIds.some((el) => el.id === job.id))
        }
    }
}, 3000)

// Check if any jobs in waiting
setInterval(async () => {
    try {
        if (waitingJobData.length) {
            promises = waitingJobData.map((el) => checkStatus(el))
            const res = await Promise.all(promises)
            const completedWaitJobs = []
            for (let i = 0; i < res.length; i++) {
                if (res[i]) {
                    completedWaitJobs.push(waitingJobData[i])
                }
            }
            if (completedWaitJobs.length) {
                inProgressJobData.push(...completedWaitJobs)
                waitingJobData = waitingJobData?.filter((job) => !completedWaitJobs.some((el) => el.id === job.id))
                console.log('Jobs completed, moved to process it')
            } else {
                // console.log('No jobs to check status')
            }
        }
    } catch (e) {
        console.log('error => ', e)
    }
}, 10000)


// check if any jobs ready
setInterval(async () => {
    try {
        if (inProgressJobData.length) {
            console.log('Jobs detected ready to be processed')
            const promises = inProgressJobData.filter((el) => Boolean(el)).map((jobId) => fetchAnalysisResults(jobId))
            await Promise.all(promises)
        }
    } catch (e) {
        console.log('error => ', e)
    }
}, 10000)


async function checkStatus(jobId) {
    return new Promise(async (resolve, reject) => {
        try {
            const [operation] = await client.operationsClient.getOperation({name: jobId.id})
            if (operation?.done === true) {
                // push this to the in-progress arr
                // inProgressJobData.push(jobId)
                // waitingJobData = waitingJobData.filter((el) => el.id !== jobId.id)
                return resolve(true)
            }
            return resolve(false)
        } catch (e) {
            console.log('error here -- ', e)
            reject(e)
        }
    })
}

async function fetchAnalysisResults(jobData) {
    return new Promise(async (resolve, reject) => {
        try {
            inProgressJobData = inProgressJobData.filter((job) => jobData.id !== job.id)
            await downloadFile(bucketName, jobData.localFile, jobData.localFileDestination)
            const data = convertDS(`./${folderName}/${jobData.localFile}`)
            await fileWrite(Buffer.from(JSON.stringify(data)), `./timestamps/${jobData.localFile}`)
            resolve()
        } catch (e) {
            console.log('Error while fetching the results');
            // Put the job back into in-progress array
            inProgressJobData.push(jobData)
            reject(e)
        }
    })
}

// checkStatus('projects/845183034104/locations/us-east1/operations/11186938882026978987')

// 'gs://rtme-videos/den_part.mp4'
// detectFacesInVideo(bucketName, inputFileName)
// jobData.push({
//     id: 'projects/845183034104/locations/us-west1/operations/7378368288491322872',
//     destinationFileName: 'den_part.mp4-1729166628396.json',
//     inputFileName: 'den_part.mp4-1729166628396.json',
//     inputFileUri: 'gs://rtme-videos/den_part.mp4',
//     localFileDestination: 'coordinates/den_part.mp4-1729166628396.json'
//   })
// downloadFile('rtme-videos', '123_next.json', 'coordinates/123_next.json')
// fetchAnalysisResults('projects/845183034104/locations/us-east1/operations/11186938882026978987')


function convertDS(filePath) {
    try {
        const allTimeRes = {}
        const rawData = require(filePath)
        if (!rawData || !rawData?.annotation_results || !rawData.annotation_results.length > 0)
            throw 'Error in raw data'

        const allFaceAttr = rawData.annotation_results[0]?.face_detection_annotations ?? []
        for (let i = 0; i < allFaceAttr.length; i++) {
            const tracks = allFaceAttr[i]?.tracks ?? []
            if (tracks.length > 0) {
                const allTimeStamps = tracks[0]?.timestamped_objects ?? null;
                if (allTimeStamps) {
                    for (let j = 0; j < allTimeStamps.length; j++) {
                        const { normalized_bounding_box, time_offset } = allTimeStamps[j]
                        const time = time_offset.seconds * 1000;
                        if (allTimeRes[time]) {
                            (allTimeRes[time]).push(normalized_bounding_box)
                        } else {
                            allTimeRes[time] = [normalized_bounding_box]
                        }
                    }
                }
            }
        }
        return allTimeRes
    } catch (e) {
        console.log(e)
        console.log('Error while converting DS')
    }
}

function fileWrite(data, localFileName) {
    return new Promise((resolve, reject) => {
        const writer = fs.createWriteStream(localFileName)
        try {
            writer.write(data, (err) => {
                if (err) {
                    return reject(err)
                }
                return resolve()
            })
        } catch (e) {
            reject(e)
        } finally {
            if (writer) writer.close()
        }
    })
}


// async function temp() {
//     const data = convertDS('./coordinates/den_og.mp4-1729235548610.json');
//     console.log('Transformed...')
//     await fileWrite(Buffer.from(JSON.stringify(data)), './timestamps/den_og.mp4-1729235548610.json')
//     console.log('done..')
// }

// temp()

detectFacesInVideo(bucketName, inputFileName)
