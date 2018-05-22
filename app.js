const fs = require("fs");
const mongoose = require('mongoose');
const TransactionModel = require('./TransactionModel')

const MONGODB_CONTAINER = process.env.MONGODB_CONTAINER || "localhost";

const CUSTOMER_ADDRESS = {
    'mvd6qFeVkqH6MNAS2Y2cLifbdaX5XUkbZJ': 'Wesley Crusher',
    'mmFFG4jqAtw9MoCC88hw5FNfreQWuEHADp': 'Leonard McCoy',
    'mzzg8fvHXydKs8j9D2a8t7KpSXpGgAnk4n': 'Jonathan Archer',
    '2N1SP7r92ZZJvYKG2oNtzPwYnzw62up7mTo': 'Jadzia Dax',
    'mutrAf4usv3HKNdpLwVD4ow2oLArL6Rez8': 'Montgomery Scott',
    'miTHhiX3iFhVnAEecLjybxvV5g8mKYTtnM': 'James T. Kirk',
    'mvcyJMiAcSXKAEsQxbW9TYZ369rsMG6rVV': 'Spock'
}

/**
 * Synchonously reads json file and parses to object
 * @param {String} path
 * @returns {Object}
 */
const parseJSON = function(path) {
    let object;
    if (fs.existsSync(path)) {
        const buffer = fs.readFileSync(path);
        try {
            object = JSON.parse(buffer);
        } catch (e) {
            console.error("JSON.parse Error", e);
        }
    }

    return object;
}

/**
 * Throw error
 * @param {String} err
 */
const throwError = function(err) {
    throw new Error(err);
}

/**
 * Open connection to db
 * @param {Connection} db
 * @returns {Promise}
 */
const openConnection = function(db) {
    return new Promise(resolve => {
        db.on('error', throwError);
        db.once('open', function() {
            resolve();
        });
    });
}

/**
 * Batch upserts with objectArray for model on query returned by generateQuery
 * @param {Model} model
 * @param {Function} generateQuery
 * @param {Array} objectArray
 * @returns {Promise}
 */
const batchUpsert = function(model, generateQuery, objectArray) {
    return new Promise(resolve => {
        model.collection.drop();
        const batch = model.collection.initializeOrderedBulkOp();
        objectArray.forEach(obj => {
            const query = generateQuery(obj);
            batch.find(query).upsert().updateOne(obj);
        });
        batch.execute((err, res) => {
            if (err) throwError();
            resolve(res);
        })
    });
}
/**
 * Returns aggregate query over valid transactions for each customer
 * @returns {Promise}
 */
const aggregateValidDeposits = () => {
    return TransactionModel.aggregate(
    [{
        $match: {
            confirmations: {
                $gte: 6
            },
            $or: [{ category: 'receive' }, { category: 'generate' }]
        }
    },
    {
        $group: {
            _id: "$address",
            count: {
                $sum: 1
            },
            sum: {
                $sum: "$amount"
            }
        }
    }]).exec();
}

/**
 * Output deposits data to stdout
 * @param {Object} deposits
 */
const showDeposits = function(deposits) {
    const referencedTransations = deposits.referencedTransations;
    const noReferenceTransations = deposits.noReferenceTransations;

    for (let address in CUSTOMER_ADDRESS) {
        const name = CUSTOMER_ADDRESS[address];
        const knownCustomerTransaction = referencedTransations.find(transaction => {
            return transaction._id === address;
        });
        if (knownCustomerTransaction) {
            console.log(`Deposited for ${name}: count=${knownCustomerTransaction.count} sum=${parseFloat(knownCustomerTransaction.sum).toFixed(8)}`);
        }
    }

    console.log(`Deposited without reference: count=${noReferenceTransations.count} sum=${parseFloat(noReferenceTransations.sum).toFixed(8)}`);
    console.log(`Smallest valid deposit: ${parseFloat(deposits.min).toFixed(8)}`);
    console.log(`Largest valid deposit: ${parseFloat(deposits.max).toFixed(8)}`);
}

/**
 * Transform db query results to output format
 * @param {Array} totalTransactionPerCustomer
 * @returns {Object}
 */
const processDeposits = function(totalTransactionPerCustomer) {
    const noReferenceTransations = totalTransactionPerCustomer.reduce((acc, curr) => {
        if (!CUSTOMER_ADDRESS[curr]) {
            acc.sum += curr.sum;
            acc.count += curr.count;
        }
        return acc;
    }, {
        count: 0,
        sum: 0
    });

    const referencedTransations = totalTransactionPerCustomer.filter(transaction => {
        return CUSTOMER_ADDRESS[transaction._id];
    })

    const sums = totalTransactionPerCustomer.map((obj => { return obj.sum; }));
    const max = Math.max.apply(Math, sums);
    let min = Number.MAX_SAFE_INTEGER;
    for (let i = 0; i < sums.length; i++) {
      if (sums[i] < min && sums[i] >= 0) {
        min = sums[i];
      }
    }

    const depositsData = {
        max: max,
        min: min,
        noReferenceTransations: noReferenceTransations,
        referencedTransations: referencedTransations
    }

    return depositsData;
}

/**
 * Save data to mongodb then query and process results
 * @param {String} dbURL
 * @param {Array} transactions
 * @returns {Promise}
 */
async function inputDB(dbURL, transactions) {
    try {
        mongoose.connect(mongodbURL);
        const db = mongoose.connection;
        await openConnection(db);
        const generateQuery = transaction => {
            return {
                txid: transaction.txid,
                vout: transaction.vout
            }
        }
        await batchUpsert(TransactionModel, generateQuery, transactions);
        const totalDepositsPerCustomer = await aggregateValidDeposits();
        db.close();

        return processDeposits(totalDepositsPerCustomer);
    } catch(error) {
        console.error(error);
    }
}

const mongodbURL = `mongodb://${MONGODB_CONTAINER}/transactions`
const transaction_1 = parseJSON("./transactions-1.json");
const transaction_2 = parseJSON("./transactions-2.json");
const transactions = transaction_1.transactions.concat(transaction_2.transactions);

inputDB(mongodbURL, transactions).then(depositsData => {
    showDeposits(depositsData);
});
