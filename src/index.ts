import { Worker } from "worker_threads";
import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse";
const csvWriter = require("csv-writer");

function runServer() {
  const csvFilePath = path.resolve(
    __dirname,
    "../coordinates_for_node_test.csv"
  );
  const headers = ["row_id", "vehicle_id", "latitude", "longitude"];
  const fileContent = fs.readFileSync(csvFilePath, { encoding: "utf-8" });

  parse(
    fileContent,
    {
      delimiter: ",",
      columns: headers,
    },
    async (error, result: any[]) => {
      if (error) {
        console.error(error);
      } else {
        await parseFileDate(result);
      }
    }
  );
}

async function parseFileDate(data) {
  const groupByVehicle_id = groupCoordinatesByVehicle(data);

  const workers = [];

  // delete the header row
  delete groupByVehicle_id["vehicle_id"];

  Object.keys(groupByVehicle_id).forEach((key, i) => {
    workers.push(createWorker(groupByVehicle_id[key]));
  });

  const convertedCoordinates = [].concat.apply([], await Promise.all(workers));

  writeResultsToCSV(convertedCoordinates);
}

function groupCoordinatesByVehicle(data) {
  return data.reduce((group, coordinate) => {
    const { vehicle_id } = coordinate;
    group[vehicle_id] = group[vehicle_id] ?? [];
    group[vehicle_id].push(coordinate);
    return group;
  }, {});
}

function createWorker(arr) {
  return new Promise((resolve, reject) => {
    const worker = new Worker("./dist/worker.js");

    worker.postMessage(arr);
    worker.on("message", resolve);
    worker.on("error", reject);
    worker.on("exit", (code) => {
      if (code !== 0)
        reject(
          new Error(`Stopped the Worker Thread with the exit code: ${code}`)
        );
    });
  });
}

function writeResultsToCSV(rows) {
  const writer = csvWriter.createObjectCsvWriter({
    path: path.resolve(__dirname, "../result.csv"),
    header: [
      { id: "row_id", title: "row_id" },
      { id: "vehicle_id", title: "vehicle_id" },
      { id: "latitude", title: "latitude" },
      { id: "longitude", title: "longitude" },
      { id: "worker_id", title: "worker_id" },
      { id: "distance_from_prev_point", title: "distance_from_prev_point" },
    ],
  });

  writer.writeRecords(rows).then(() => {
    console.log("Done!");
    process.exit();
  });
}


try {
  runServer();
} catch (error) {
  console.log(error);
  process.exit();
}

process.on('unhandledRejection',(reason: any, promise: any) => {
  console.error(reason, promise);
  process.exit(1);
})