import { workerData, parentPort, threadId } from "worker_threads";

parentPort.on("message", (msg) => {
  console.log(`running on thread ${threadId} on vehicle_id ${msg[0].vehicle_id}` );
  parentPort.postMessage(convertData(msg, threadId));
});

function convertData(coordinates: Array<any>, workerId) {
  let prevLat = null;
  let prevLong = null;
  const converted = [];
  for (let coor of coordinates) {
    converted.push(
      !prevLat
        ? { ...coor, worker_id: workerId, distance_from_prev_point: null }
        : {
            ...coor,
            worker_id: workerId,
            distance_from_prev_point: distance(
              prevLat,
              prevLong,
              coor.latitude,
              coor.longitude
            ),
          }
    );

    prevLat = coor.latitude;
    prevLong = coor.longitude;
  }

  return converted;
}

function distance(lat1, lon1, lat2, lon2) {
  const p = 0.017453292519943295; // Math.PI / 180
  const c = Math.cos;
  const a =
    0.5 -
    c((lat2 - lat1) * p) / 2 +
    (c(lat1 * p) * c(lat2 * p) * (1 - c((lon2 - lon1) * p))) / 2;

  return 12742 * Math.asin(Math.sqrt(a)) * 1000;
}
