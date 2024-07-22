const fs = require("fs");
const csv = require("csv-parser");
const sortBy = require("lodash/sortBy");

function readCSV(pathname = "") {
  return new Promise(resolve => {
    const transactions = [];

    fs.createReadStream(pathname)
      .pipe(csv())
      .on("data", data => transactions.push(data))
      .on("end", () => {
        resolve(transactions);
      });
  });
}

/**
 * 
 * @returns {Object} 
 * 
 * parsedTransaction
 * 
 * {
 *  "2021-08-01": {
 *   "agent_id_1": {
 *      first_transaction_finished_at: "2021-08-01 08:00:00",
        second_transaction_start_at: "2021-08-01 08:10:00",
        duration_in_seconds: 600
        selected_service: "service1"
 *    }
 * }
 * 
 */
function getFirstCallAfterCallWorkByDateByAgent(transactions) {
  const res = {};

  sortBy(transactions, ["agent_joined_at"]).forEach(transaction => {
    const date = transaction["agent_joined_at"].split(" ")[0];
    const agent_id = transaction["agent_id"];
    const selected_service = transaction["selected_service"];
    const finished_at = transaction["finished_at"];
    const started_at = transaction["agent_joined_at"];
    const final_status = transaction["final_status"];

    if (!res[date]) {
      res[date] = {};
    }

    if (!res[date][agent_id]) {
      if (final_status === "completed") {
        res[date][agent_id] = {
          first_transaction_finished_at: finished_at,
          selected_service,
          first_transaction_agent_joined_at: started_at,
        };

        return;
      }

      return;
    }

    if (!res[date][agent_id]["second_transaction_start_at"]) {
      res[date][agent_id] = {
        ...res[date][agent_id],
        second_transaction_start_at: started_at,
        second_transaction_agent_joined_at: started_at,
        duration_in_seconds:
          (new Date(started_at) - new Date(res[date][agent_id]["first_transaction_finished_at"])) / 1000,
      };
    }
  });

  return res;
}

function averageAfterCallWorkByService(parsedTransactions) {
  const durationListByService = {};
  const avgListByService = {};

  Object.keys(parsedTransactions).forEach(date => {
    Object.keys(parsedTransactions[date]).forEach(agent_id => {
      const selected_service = parsedTransactions[date][agent_id]["selected_service"];
      const durationOrNaN = parsedTransactions[date][agent_id]["duration_in_seconds"];
      const duration = isNaN(durationOrNaN) ? 0 : durationOrNaN;

      if (!durationListByService[selected_service]) {
        durationListByService[selected_service] = [];
      }

      durationListByService[selected_service].push(duration);
    });
  });

  Object.keys(durationListByService).forEach(service => {
    const durations = durationListByService[service];
    const sum = durations.reduce((acc, curr) => acc + curr, 0);
    const avg = sum / durations.length;

    avgListByService[service] = Math.round(avg);
  });

  return avgListByService;
}

const csvPath = "transactions.csv";

readCSV(csvPath).then(transactions => {
  const parsedTransactions = getFirstCallAfterCallWorkByDateByAgent(transactions);
  const avg = averageAfterCallWorkByService(parsedTransactions);

  console.table(avg);
});
