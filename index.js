const { GlueClient, StartJobRunCommand } = require("@aws-sdk/client-glue");

const GLUE_JOB_NAME = process.env.GLUE_JOB_NAME;
const RAW_BUCKET = process.env.RAW_BUCKET || "fiap-fase2-tech-challenge";
const RAW_PREFIX = process.env.RAW_PREFIX || "raw/";
const REGION = process.env.AWS_REGION;

const glue = new GlueClient({ region: REGION });

exports.handler = async (event) => {
  console.log("EVENT:", JSON.stringify(event));

  if (!GLUE_JOB_NAME) {
    console.error("Missing env var GLUE_JOB_NAME");
    return { statusCode: 500, body: "Missing GLUE_JOB_NAME" };
  }

  let shouldStart = false;

  for (const record of (event.Records || [])) {
    const bucket = record?.s3?.bucket?.name;
    const key = decodeURIComponent((record?.s3?.object?.key || "").replace(/\+/g, " "));

    console.log("S3 record:", { bucket, key });

    if (bucket !== RAW_BUCKET) continue;
    if (key.startsWith(RAW_PREFIX) && key.endsWith(".parquet")) {
      shouldStart = true;
    }
  }

  if (!shouldStart) {
    console.log("No matching parquet in raw/. Ignoring.");
    return { statusCode: 200, body: "Ignored" };
  }

  const cmd = new StartJobRunCommand({
    JobName: GLUE_JOB_NAME,
    Arguments: {
      "--RAW_S3_PATH": `s3://${RAW_BUCKET}/raw/`,
      "--REFINED_S3_PATH": `s3://${RAW_BUCKET}/refined/`,
    },
  });

  const resp = await glue.send(cmd);
  console.log("Started Glue JobRunId:", resp.JobRunId);

  return { statusCode: 200, body: `Started ${resp.JobRunId}` };
};