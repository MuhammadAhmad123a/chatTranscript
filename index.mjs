import { MongoClient } from "mongodb";
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import Redis from "ioredis";
import log4js from "log4js";

//Redis methods
const REDIS_HOST = "redis-15596.c270.us-east-1-3.ec2.cloud.redislabs.com";
const REDIS_PORT = "15596";
const REDIS_PASW = "yQG3PbrwvFk0VCHX4RHCpGXbuMS8J71n";
const cacheClient = new Redis({
  port: REDIS_PORT,
  host: REDIS_HOST,
  connectTimeout: 10000,
  username: "default",
  password: REDIS_PASW,
});

async function setDataInCache(key, value) {
  return await cacheClient.lpush(key, value);
}
async function getDataFromCache(key) {
  let result = await cacheClient.lrange(key, 0, -1);
  return result.reverse();
}
async function deleteDataFromCache(key) {
  return await cacheClient.del(key);
}

// Configure the logger
log4js.configure({
  appenders: { console: { type: "console" } },
  categories: { default: { appenders: ["console"], level: "trace" } },
});
// Get the logger instance
 const logger = log4js.getLogger();
// const dbUrl = process.env.URL;
// const dbOptions = {
//   useNewUrlParser: true,
//   useUnifiedTopology: true,
//   // maxIdleTimeMS: 60000,
//   // connectTimeoutMS: 150000,
//   // socketTimeoutMS: 90000,
// };


const s3Client = new S3Client();

const dbUrl = process.env.URL;
const dbOptions = {
  // maxIdleTimeMS: 60000,
  // connectTimeoutMS: 150000,
  // socketTimeoutMS: 90000,
};

let cachedDb;
let client;

//---DATABASE CONNECTION---//
async function connectToDatabase() {
  if (cachedDb) {
    //console.log("Existing cached connection found!");
    logger.info(`Existing cached connection found! ${cachedDb}`);
    return cachedDb;
  }
  //console.log("Aquiring new DB connection....");
  logger.info(`Aquiring new DB connection.... `);
  try {
    client = await MongoClient.connect(dbUrl, dbOptions);

    // Specify which database we want to use
    const db = await client.db("AGENT-DESKTOP-BACKEND");

    cachedDb = db;
    return db;
  } catch (error) {
    //console.log("ERROR aquiring DB Connection!");
    logger.error("ERROR aquiring DB Connection!", error);
    //console.log(error);
    throw error;
  }
}

// async function connectToDatabase(collection) {
//   try {
//     const client = new MongoClient(dbUrl, dbOptions);
//     await client.connect();
//     // logger.info("Connected to MongoDB!");
//     return client.db("AGENT-DESKTOP-BACKEND").collection(collection);
//   } catch (err) {
//     // logger.error("Error connecting to MongoDB: ", err);
//     throw err;
//   }
// }

function convertToS3Url(input) {
  const firstIndex = input.indexOf("/");

  const bucket = input.substring(0, firstIndex);
  //console.log("Bucket:", bucket);
  logger.info(`Bucket: ${bucket}`);

  const path = input.substring(firstIndex + 1);
  //console.log("Path:", path);
  logger.info(`Path: ${path}`);

  const s3Url = `https://${bucket}.s3.${process.env.REGION}.amazonaws.com/${path}`;
  return s3Url;
}

async function getTranscript(transcriptLocation) {
  try {
    const firstIndex = transcriptLocation.indexOf("/");

    const bucket = transcriptLocation.substring(0, firstIndex);
    //console.log("Bucket:", bucket);
    logger.info(`Bucket: ${bucket}`);

    const file = transcriptLocation.substring(firstIndex + 1);
    //console.log("File:", file);
    logger.info(`File: ${file}`);
    const input = {
      Bucket: bucket,
      Key: file,
    };

    const command = new GetObjectCommand(input);
    const response = await s3Client.send(command);
    //console.log("Response ===>", response);
    logger.info(`Response ===> ${response}`);

    let body = await response.Body.transformToString();
    body = JSON.parse(body);

    const transcript = body.Transcript;
    //console.log("Transcript ===>", JSON.stringify(transcript));
    logger.info(`Transcript ===> ${JSON.stringify(transcript)}`);

    return transcript;
  } catch (error) {
    //logger.error("Error fetching customer data: ", error);
    throw error;
  }
}

async function getConversation(contactId) {
  try {
    let responseObj;
    const db = await connectToDatabase();
    const conversation = db.collection("conversations");
    //console.log("Going to get Conversation with contact ID:", contactId);
    logger.info(`Going to get Conversation with contact ID: ${contactId}`);
    const conversationObject = await conversation.findOne({
      contactId: contactId,
    });
    return conversationObject;
  } catch (error) {
    //console.log(error);
    logger.error("Error fetching customer data: ", error);
    throw error;
  }
}

async function saveTranscriptToDb(
  transcript,
  conversation,
  contactId,
  attachmentLocation,
  agent
) {
  try {
    //console.log(conversation);
    logger.info(`${conversation}`);
    const customerId = conversation.customer._id;
    const db = await connectToDatabase();
    const activities = db.collection("customeractivities");
    let customerActivities;

    //console.log("Going to save transcript in DB");
    logger.info(`Going to save transcript in DB`);

    const messages = transcript.filter((att) => {
      return (
        (att.Type === "MESSAGE" || att.Type === "ATTACHMENT") &&
        (att.Content || att.Attachments)
      );
    });

    if (!messages || messages.length === 0) {
      return;
    }

    for (let i = 0; i < messages.length; i++) {
      let attachment;
      if (
        messages[i].ParticipantRole === "CUSTOMER" &&
        messages[i].Type === "MESSAGE"
      ) {
        customerActivities = {
          customerId: customerId,
          contactId: contactId,
          timeStamp: messages[i].AbsoluteTime,
          type: "Message",
          data: {
            message: { text: messages[i].Content, messageType: "plain" },
            user: messages[i].DisplayName,
            customerId: customerId,
            type: "customerMessage",
            date: messages[i].AbsoluteTime,
          },
        };
      } else if (
        messages[i].ParticipantRole === "AGENT" &&
        messages[i].Type === "MESSAGE"
      ) {
        customerActivities = {
          customerId: customerId,
          contactId: contactId,
          timeStamp: messages[i].AbsoluteTime,
          type: "Message",
          data: {
            message: { text: messages[i].Content, messageType: "plain" },
            user: agent,
            customerId: customerId,
            type: "agentMessage",
            date: messages[i].AbsoluteTime,
          },
        };
      } else if (
        messages[i].ParticipantRole === "CUSTOMER" &&
        messages[i].Type === "ATTACHMENT"
      ) {
        const attachmentId = messages[i].Attachments[0].AttachmentId;
        const customerAttachment = attachmentLocation.filter((att) => {
          return att.Value.includes(attachmentId);
        });

        let transcriptLocation = customerAttachment[0].Value;
        const url = convertToS3Url(transcriptLocation);
        //console.log(url);
        //console.log("Attachment ::", messages[i]);
        logger.info(`S3 bucket url: ${url}`);
        logger.info(`Attachment :: ${messages[i]}`);
        attachment = {
          fileUrl: url,
          messageType: messages[i].Attachments[0].ContentType,
          fileName: messages[i].Attachments[0].AttachmentName,
          //fileSize: res.size
        };
        //console.log("Customer Attachment", attachment);
        logger.info(`Customer Attachment: ${attachment}`);
        customerActivities = {
          customerId: customerId,
          contactId: contactId,
          timeStamp: messages[i].AbsoluteTime,
          type: "Attachment",
          data: {
            message: attachment,
            user: messages[i].DisplayName,
            customerId: customerId,
            type: "customerMessage",
            date: messages[i].AbsoluteTime,
          },
        };
      } else if (
        messages[i].ParticipantRole === "AGENT" &&
        messages[i].Type === "ATTACHMENT"
      ) {
        const attachmentId = messages[i].Attachments[0].AttachmentId;
        const agentAttachment = attachmentLocation.filter((att) => {
          return att.Value.includes(attachmentId);
        });
        let transcriptLocation = agentAttachment[0].Value;
        const url = convertToS3Url(transcriptLocation);
        // console.log(url);
        // console.log("Attachment ::", messages[i]);
        logger.info(`S3 bucket url: ${url}`);
        logger.info(`Attachment :: ${messages[i]}`);

        attachment = {
          fileUrl: url,
          messageType: messages[i].Attachments[0].ContentType,
          fileName: messages[i].Attachments[0].AttachmentName,
          //fileSize: res.size
        };
        //console.log("Agent Attachment", attachment);
        logger.info(`Agent Attachment: ${attachment}`);
        customerActivities = {
          customerId: customerId,
          contactId: contactId,
          timeStamp: messages[i].AbsoluteTime,
          type: "Attachment",
          data: {
            message: attachment,
            user: agent,
            customerId: customerId,
            type: "agentMessage",
            date: messages[i].AbsoluteTime,
          },
        };
      }
      await activities.insertOne(customerActivities);
    }
    return "OK";
  } catch (error) {
    logger.error("Error fetching customer data: ", error);
    throw error;
  }
}

async function saveVoiceDetilstoDB(data, conversation) {
  try {
    
    const customerId = conversation.customer._id;
    const conversationId = conversation._id.toString();
    const startTime = data.Agent.ConnectedToAgentTimestamp;
    const endTime = data.Agent.AfterContactWorkStartTimestamp;
    const agentName = data.Agent.Username;
    const agentArn = data.Agent.ARN.split("/")[3];
    const direction = data.InitiationMethod;
    const queueName = data.Queue.Name;
    const queueArn = data.Queue.ARN.split("/")[3];
    const recordingLink = data.Recording.Location;
    const disconnect = data.DisconnectReason;
    const from = data.CustomerEndpoint.Address;
    
    logger.info(`Save voice detail to DB function`);

    let obj = {
      callType:
        data.InitiationMethod.charAt(0).toUpperCase() +
        data.InitiationMethod.slice(1).toLowerCase(), //contact.contactData.connections[1].type,
      messageType: data.Channel.toLowerCase(),
      callDuration: data.Agent.AgentInteractionDuration,
      agentName: data.Agent.Username,
      queueName: data.Queue.Name ? data.Queue.Name : "N/A",
    };
    logger.info(`Object: ${obj}`);

    let customerActivities = {
      customerId: customerId,
      conversationId: conversationId,
      type: "Voice",
      timeStamp: startTime,
      data: {
        message: obj,
        user: "subhan",
        customerName: "",
        type: "customerMessage",
        date: startTime,
        disconnect: disconnect,
        from: from,
      },
    };
    logger.info(`Customer activity: ${customerActivities}`);

    // let customerActivities = {
    //   customerId: customerId,
    //   conversationId: conversationId,
    //   type: "Voice",
    //   timeStamp: startTime,
    //   data: {
    //     direction: direction,
    //     timeStamps: {
    //       startTime: startTime,
    //       endTime: endTime,
    //     },
    //     agentDetails: {
    //       agentName: agentName,
    //       agentArn: agentArn,
    //     },
    //     queueDetails: {
    //       queueName: queueName,
    //       queueArn: queueArn,
    //     },
    //     recordingLink: recordingLink,
    //   },
    // };

    const db = await connectToDatabase();
    const activities = db.collection("customeractivities");
    const response = await activities.insertOne(customerActivities);
    logger.info(`Customer activity stored in database: ${response}`);
    return response;
  } catch (error) {
    logger.error("Error while storing customer activity in database: ", error);
    throw error;
  }
}
//Create ivr activity
async function saveIVRActivityToMongo(data) {
  try {
    const contactId = data.ContactId;
    const InitiationTimestamp = data.InitiationTimestamp;
    const DisconnectTimestamp = data.DisconnectTimestamp;
    const queue = data.Queue;
    logger.info(`Save IVR activity to Mongo function.`);
    // console.log('Contact id: ', contactId);
    // console.log('Initial Timestamp: ', InitiationTimestamp);
    // console.log('Disconnect Timestamp: ', DisconnectTimestamp);
    // console.log('Queue: ', queue);
    logger.info(`Contact id: ${contactId}`);
    logger.info(`Initial Timestamp: ${InitiationTimestamp}`);
    logger.info(`Disconnect Timestamp: ${DisconnectTimestamp}`);
    logger.info(`Queue: ${queue}`);
    //Get redis data
    const redisData = await getDataFromCache(contactId);
    //console.log("Data in redis is: ", redisData);
    logger.info(`Data in redis is: ${redisData}`);
    const conversation = await getConversation(contactId);

    //Create Activity for IVR
    const ivrData = {
      Contact_Id: contactId,
      Initial_Timestamp: InitiationTimestamp,
      Disconnect_Timestamp: DisconnectTimestamp,
      Queue: queue,
      IVR_Tree: redisData
    }
    //console.log("Data for ivr activity is: ", ivrData);
    // Convert each string in IVR_Tree to an array of objects
    let ivrTreeArray = ivrData.IVR_Tree.map(jsonString => {
      // Parse the JSON string
      let parsedArray = JSON.parse(jsonString);

      // Convert null values to an empty object
      return parsedArray.map(item => item === null ? {} : item);
    });

    // Flatten the array of arrays to a single array
    let flattenedArray = [].concat(...ivrTreeArray);

    // Remove null objects and objects with no Module_Result
    let filteredArray = flattenedArray.filter(item => Object.keys(item).length > 0 && item.Module_Result !== undefined);

    // Merge filtered array back into the activity object at IVR_Tree
    ivrData.IVR_Tree = filteredArray;


    // Console log the updated activity object
    //console.log("Activity: ", ivrData);
    logger.info(`Activity: ${ivrData}`);

    const db = await connectToDatabase();
    const activities = db.collection("customeractivities");
    const response = await activities.insertOne(ivrData);
    if (response) {
      // console.log("IVR activity created successfully!");
      // console.log(response);
      logger.info(`IVR activity created successfully! ${response}`);
      //Remove data from redis cache after creating the ivr activity
      const deleteRedisData = await deleteDataFromCache(contactId);
      if (deleteRedisData) {
        //console.log("Data from redis is deleted successfully after creating activity!");
        logger.info(`Data from redis is deleted successfully after creating activity!`);
      }
    }

    return response
  } catch (error) {
    //console.log("IVR activity not created: ", error);
    logger.error("IVR activity not created: ", error);
    throw error;
  }
}
const sendNotFoundResponse = (obj) => {
  const response = {
    statusCode: 404,
    headers: {
      "Access-Control-Allow-Headers": "Content-Type",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
    },
    body: JSON.stringify(obj),
  };

  return response;
};

process.on("beforeExit", () => {
  if (client) {
    client.close();
  }
});

export const handler = async (event) => {
  try {
    const record = event.Records[0].kinesis;  //get kinesis record
    let responseObj;

    var payload = Buffer.from(record.data, "base64").toString("utf-8");
    let data = JSON.parse(payload);
    //console.log(data);
    logger.info(`Stream data after parsing: ${data}`);

    const routingProfile = data.Agent ? data.Agent.RoutingProfile.Name : "";
    //console.log("RoutingProfile", routingProfile);
    logger.info(`Routing profile: ${routingProfile}`);
    if (
      data.Channel === "CHAT" &&
      routingProfile === "Agent Desk 3.0 Routing Profile"
    ) {
      const transcriptLocation = data.Recording.Location;
      const contactId = data.ContactId;
      const attachmentLocation = data.References;
      const agent = data.Agent.Username;

      const transcript = await getTranscript(transcriptLocation);

      const conversation = await getConversation(contactId);

      if (!conversation) {
        logger.info(`Conversation not found against contact id: ${contactId}`);
        let response = `No conversation exists with contactId: ${contactId}`;
        responseObj = sendNotFoundResponse(response);
        return responseObj;
      }

      const activities = await saveTranscriptToDb(
        transcript,
        conversation,
        contactId,
        attachmentLocation,
        agent
      );

      return activities;
    } else if (
      data.Channel === "VOICE" &&
      routingProfile === "Agent Desk 3.0 Routing Profile"
    ) {
      const contactId = data.ContactId;
      //parameters of ivr activity
      //const activities = await saveTranscriptToDb
      const ivrActivity = await saveIVRActivityToMongo(data);

      if (!conversation) {
        logger.info(`Conversation not found against contact id: ${contactId}`);
        let response = `No conversation exists with contactId ${contactId}`;
        responseObj = sendNotFoundResponse(response);
        return responseObj;
      }
      const activities = await saveVoiceDetilstoDB(data, conversation);

      return activities;
    } else if (data.Channel === "VOICE") {
      //parameters of ivr activity
      //const activities = await saveTranscriptToDb
      const ivrActivity = await saveIVRActivityToMongo(data);
    }
    else {
      return {
        statusCode: 404,
      };
    }
  } catch (error) {
    logger.error("Error processing request: ", error);
    //console.log(error);
    throw error;
  }
};
